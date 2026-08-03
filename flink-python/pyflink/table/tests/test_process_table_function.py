################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from pyflink.common import Duration, Row, RowKind
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.changelog_mode import ChangelogMode
from pyflink.table.expressions import col, descriptor, lit
from pyflink.table.udf import (
    ProcessTableFunction,
    ProcessTableFunctionArgument,
    ProcessTableFunctionArgumentTrait as Trait,
    ProcessTableFunctionState,
    udptf,
)
from pyflink.testing.test_case_utils import PyFlinkTestCase


class Tokenize(ProcessTableFunction):
    def eval(self, ctx, event, separator):
        for token in event.text.split(separator):
            if token:
                yield Row(token, separator)


class CountWithTimeout(ProcessTableFunction):
    def eval(self, ctx, memory, event, timeout_ms):
        memory["count"] = (memory.count or 0) + 1
        event_time = ctx.time_context(int).time()
        if event_time is not None:
            ctx.time_context(int).register_on_time("timeout", event_time + timeout_ms)
        yield Row(memory.count, "event")

    def on_timer(self, ctx, memory):
        yield Row(memory.count, ctx.current_timer())
        ctx.clear_all()


class CountByKey(ProcessTableFunction):
    def eval(self, ctx, memory, event):
        memory["count"] = (memory.count or 0) + 1
        yield Row(memory.count)


class OptionalOnTimeTimer(ProcessTableFunction):
    def eval(self, ctx, event):
        ctx.time_context(int).register_on_time("timeout", 0)
        yield Row("event")

    def on_timer(self, ctx):
        yield Row(ctx.current_timer())


class PassThroughLength(ProcessTableFunction):
    def eval(self, ctx, event):
        yield Row(len(event.text))


class OrderedScores(ProcessTableFunction):
    def eval(self, ctx, event):
        yield Row(event.score)


class InspectTableSemantics(ProcessTableFunction):
    def eval(self, ctx, event):
        semantics = ctx.table_semantics_for("event")
        yield Row(
            len(semantics.data_type().field_names()),
            semantics.partition_by_columns()[0],
            ctx.get_changelog_mode().contains_only(RowKind.INSERT))


class EmitDeclaredKind(ProcessTableFunction):
    def eval(self, ctx, event):
        yield Row.of_kind(RowKind(event.kind), event.value)


class ForwardInputKind(ProcessTableFunction):
    def eval(self, ctx, event):
        yield Row.of_kind(event.get_row_kind(), str(event.get_row_kind()))


class EmitUnexpectedDelete(ProcessTableFunction):
    def eval(self, ctx, event):
        yield Row.of_kind(RowKind.DELETE, event.value)


class TrackStateViews(ProcessTableFunction):
    def eval(self, ctx, memory, history, counts, event):
        memory["count"] = (memory.count or 0) + 1
        if event.action == "clear":
            ctx.clear_all_state()
        elif event.action == "remove":
            history.remove(event.value)
            counts.remove(event.value)
        else:
            history.add(event.value)
            counts.put(event.value, (counts.get(event.value) or 0) + 1)
        yield Row(event.sequence, memory.count, len(list(history.get())),
                  counts.get(event.value) or 0)


class ObserveExpiringState(ProcessTableFunction):
    def eval(self, ctx, memory, history, counts, event):
        previous_count = memory.count or 0
        previous_history = len(list(history.get()))
        previous_map_value = counts.get("seen") or 0

        memory["count"] = previous_count + 1
        history.add(event.sequence_id)
        counts.put("seen", previous_map_value + 1)
        yield Row(event.sequence_id, previous_count, previous_history, previous_map_value)


class ProcessTableFunctionTests(PyFlinkTestCase):

    @staticmethod
    def _tokenize_function():
        return udptf(
            Tokenize(),
            arguments=[
                ProcessTableFunctionArgument.table(
                    "event", traits={Trait.ROW_SEMANTIC_TABLE}),
                ProcessTableFunctionArgument.scalar("separator", DataTypes.STRING()),
            ],
            result_type=DataTypes.ROW([
                DataTypes.FIELD("text", DataTypes.STRING()),
                DataTypes.FIELD("separator", DataTypes.STRING()),
            ]),
        )

    @staticmethod
    def _count_with_timeout_function():
        return udptf(
            CountWithTimeout(),
            arguments=[
                ProcessTableFunctionArgument.table(
                    "event", traits={Trait.SET_SEMANTIC_TABLE, Trait.REQUIRE_ON_TIME}),
                ProcessTableFunctionArgument.scalar("timeout_ms", DataTypes.BIGINT()),
            ],
            states=[
                ProcessTableFunctionState.value(
                    "memory",
                    DataTypes.ROW([DataTypes.FIELD("count", DataTypes.BIGINT())]),
                    ttl=Duration.of_days(1),
                )
            ],
            result_type=DataTypes.ROW([
                DataTypes.FIELD("count", DataTypes.BIGINT()),
                DataTypes.FIELD("trigger", DataTypes.STRING()),
            ]),
        )

    @staticmethod
    def _count_by_key_function(optional_partition=False):
        traits = {Trait.SET_SEMANTIC_TABLE}
        if optional_partition:
            traits.add(Trait.OPTIONAL_PARTITION_BY)
        return udptf(
            CountByKey(),
            arguments=[ProcessTableFunctionArgument.table("event", traits=traits)],
            states=[ProcessTableFunctionState.value(
                "memory", DataTypes.ROW([DataTypes.FIELD("count", DataTypes.BIGINT())]))],
            result_type=DataTypes.ROW([DataTypes.FIELD("count", DataTypes.BIGINT())]),
        )

    @staticmethod
    def _optional_on_time_timer_function():
        return udptf(
            OptionalOnTimeTimer(),
            arguments=[ProcessTableFunctionArgument.table(
                "event", traits={Trait.SET_SEMANTIC_TABLE})],
            result_type=DataTypes.ROW([DataTypes.FIELD("trigger", DataTypes.STRING())]),
        )

    def test_create_stateless_function(self):
        function = self._tokenize_function()

        java_function = function._java_user_defined_function()
        self.assertEqual("PythonProcessTableFunction", java_function.getClass().getSimpleName())
        static_arguments = java_function.getTypeInference(None).getStaticArguments().get()
        self.assertEqual(["event", "separator"], [a.getName() for a in static_arguments])

    def test_declares_fixed_changelog_mode(self):
        function = udptf(
            EmitDeclaredKind(),
            arguments=[ProcessTableFunctionArgument.table(
                "event", traits={Trait.SET_SEMANTIC_TABLE})],
            result_type=DataTypes.ROW([DataTypes.FIELD("value", DataTypes.STRING())]),
            changelog_mode=ChangelogMode.upsert(False),
        )

        java_mode = function._java_user_defined_function().getChangelogMode(None)
        self.assertTrue(java_mode.contains(RowKind.INSERT.to_j_row_kind()))
        self.assertTrue(java_mode.contains(RowKind.UPDATE_AFTER.to_j_row_kind()))
        self.assertTrue(java_mode.contains(RowKind.DELETE.to_j_row_kind()))
        self.assertFalse(java_mode.keyOnlyDeletes())

    def test_declares_state_views(self):
        states = [
            ProcessTableFunctionState.list_view(
                "history", DataTypes.STRING(), Duration.of_hours(1)),
            ProcessTableFunctionState.map_view(
                "counts", DataTypes.STRING(), DataTypes.BIGINT(), Duration.of_hours(2)),
        ]
        function = udptf(
            TrackStateViews(),
            arguments=[ProcessTableFunctionArgument.table(
                "event", traits={Trait.SET_SEMANTIC_TABLE})],
            states=[ProcessTableFunctionState.value(
                "memory", DataTypes.ROW([DataTypes.FIELD("count", DataTypes.BIGINT())]))] + states,
            result_type=DataTypes.ROW([
                DataTypes.FIELD("sequence", DataTypes.INT()),
                DataTypes.FIELD("invocations", DataTypes.BIGINT()),
                DataTypes.FIELD("history_size", DataTypes.INT()),
                DataTypes.FIELD("value_count", DataTypes.BIGINT()),
            ]),
        )

        state_strategies = function._java_user_defined_function() \
            .getTypeInference(None).getStateTypeStrategies()
        self.assertEqual(["memory", "history", "counts"], list(state_strategies.keySet()))

    def test_state_views_execution_and_key_isolation(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        table_env.get_config().set("python.fn-execution.bundle.size", "1")
        function = udptf(
            TrackStateViews(),
            arguments=[ProcessTableFunctionArgument.table(
                "event", traits={Trait.SET_SEMANTIC_TABLE})],
            states=[
                ProcessTableFunctionState.value(
                    "memory",
                    DataTypes.ROW([DataTypes.FIELD("count", DataTypes.BIGINT())])),
                ProcessTableFunctionState.list_view("history", DataTypes.STRING()),
                ProcessTableFunctionState.map_view(
                    "counts", DataTypes.STRING(), DataTypes.BIGINT()),
            ],
            result_type=DataTypes.ROW([
                DataTypes.FIELD("sequence", DataTypes.INT()),
                DataTypes.FIELD("invocations", DataTypes.BIGINT()),
                DataTypes.FIELD("history_size", DataTypes.INT()),
                DataTypes.FIELD("value_count", DataTypes.BIGINT()),
            ]),
        )
        table_env.create_temporary_system_function("track_views", function)
        events = table_env.from_elements(
            [
                (1, 1, "add", "a"),
                (1, 2, "add", "a"),
                (2, 3, "add", "a"),
                (1, 4, "clear", "a"),
                (1, 5, "add", "b"),
                (1, 6, "remove", "b"),
            ],
            DataTypes.ROW([
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("sequence", DataTypes.INT()),
                DataTypes.FIELD("action", DataTypes.STRING()),
                DataTypes.FIELD("value", DataTypes.STRING()),
            ]))

        result = events.partition_by(col("id")).process("track_views")
        with result.execute().collect() as rows:
            actual = sorted((row[1], row[2], row[3], row[4]) for row in rows)

        self.assertEqual([
            (1, 1, 1, 1),
            (2, 2, 2, 2),
            (3, 1, 1, 1),
            (4, 3, 2, 2),
            (5, 1, 1, 1),
            (6, 2, 0, 0),
        ], actual)

    def test_value_state_and_state_views_expire(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        table_env.get_config().set("python.fn-execution.bundle.size", "1")
        table_env.get_config().set("python.state.cache-size", "0")
        table_env.get_config().set("python.map-state.read-cache-size", "0")
        table_env.get_config().set("python.map-state.write-cache-size", "0")
        table_env.get_config().set("parallelism.default", "1")
        ttl = Duration.of_millis(100)
        function = udptf(
            ObserveExpiringState(),
            arguments=[ProcessTableFunctionArgument.table(
                "event", traits={Trait.SET_SEMANTIC_TABLE})],
            states=[
                ProcessTableFunctionState.value(
                    "memory",
                    DataTypes.ROW([DataTypes.FIELD("count", DataTypes.BIGINT())]),
                    ttl=ttl),
                ProcessTableFunctionState.list_view(
                    "history", DataTypes.BIGINT(), ttl=ttl),
                ProcessTableFunctionState.map_view(
                    "counts", DataTypes.STRING(), DataTypes.BIGINT(), ttl=ttl),
            ],
            result_type=DataTypes.ROW([
                DataTypes.FIELD("sequence", DataTypes.BIGINT()),
                DataTypes.FIELD("previous_count", DataTypes.BIGINT()),
                DataTypes.FIELD("previous_history", DataTypes.INT()),
                DataTypes.FIELD("previous_map_value", DataTypes.BIGINT()),
            ]),
        )
        table_env.create_temporary_system_function("observe_expiring_state", function)
        table_env.execute_sql("""
            CREATE TEMPORARY TABLE ttl_events (
                user_id AS CAST(1 AS BIGINT),
                sequence_id BIGINT
            ) WITH (
                'connector' = 'datagen',
                'number-of-rows' = '12',
                'rows-per-second' = '1',
                'scan.parallelism' = '1',
                'fields.sequence_id.kind' = 'sequence',
                'fields.sequence_id.start' = '1',
                'fields.sequence_id.end' = '12'
            )
        """)

        result = table_env.from_path("ttl_events").partition_by(
            col("user_id")).process("observe_expiring_state")
        with result.execute().collect() as rows:
            actual = {row[1]: (row[2], row[3], row[4]) for row in rows}

        self.assertEqual(set(range(1, 13)), set(actual))
        self.assertEqual((0, 0, 0), actual[1])
        self.assertEqual((0, 0, 0), actual[12])

    def test_table_process_and_from_call_plan(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        table_env.create_temporary_system_function("tokenize", self._tokenize_function())
        events = table_env.from_elements(
            [("hello world",)],
            DataTypes.ROW([DataTypes.FIELD("text", DataTypes.STRING())]))

        implicit_result = events.process(
            "tokenize", lit(" ").as_argument("separator"))
        explicit_result = table_env.from_call(
            "tokenize",
            events.as_argument("event"),
            lit(" ").as_argument("separator"))

        self.assertEqual(["text", "separator"],
                         implicit_result.get_schema().get_field_names())
        self.assertEqual(["text", "separator"],
                         explicit_result.get_schema().get_field_names())
        self.assertIn("ProcessTableFunction", implicit_result.explain())
        self.assertIn("ProcessTableFunction", explicit_result.explain())

    def test_registered_function_can_be_called_from_sql(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        table_env.get_config().set("python.fn-execution.bundle.size", "1")
        table_env.create_temporary_system_function("tokenize", self._tokenize_function())
        table_env.create_temporary_view(
            "events",
            table_env.from_elements(
                [("hello flink",)],
                DataTypes.ROW([DataTypes.FIELD("text", DataTypes.STRING())])))

        result = table_env.sql_query(
            "SELECT * FROM tokenize(event => TABLE events, `separator` => ' ')")
        with result.execute().collect() as rows:
            actual = sorted((row[0], row[1]) for row in rows)

        self.assertEqual([("flink", " "), ("hello", " ")], actual)

    def test_call_java_process_table_functions(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        function_prefix = (
            "org.apache.flink.table.runtime.operators.python.process."
            "TestJavaProcessTableFunctions$")
        table_env.create_java_temporary_system_function(
            "java_row_ptf", function_prefix + "RowSemanticFunction")
        table_env.create_java_temporary_system_function(
            "java_multi_ptf", function_prefix + "MultiInputFunction")
        orders = table_env.from_elements(
            [("Alice", 1)],
            DataTypes.ROW([
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("score", DataTypes.INT()),
            ]))
        profiles = table_env.from_elements(
            [("Alice", 2)],
            DataTypes.ROW([
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("score", DataTypes.INT()),
            ]))

        single = orders.process("java_row_ptf", lit(42).as_argument("increment"))
        multiple = table_env.from_call(
            "java_multi_ptf",
            orders.partition_by(col("name")).as_argument("in1"),
            profiles.partition_by(col("name")).as_argument("in2"),
        )

        self.assertEqual(["out"], single.get_schema().get_field_names())
        self.assertEqual(["name", "name0", "out"],
                         multiple.get_schema().get_field_names())
        self.assertIn("ProcessTableFunction", single.explain())
        self.assertIn("ProcessTableFunction", multiple.explain())
        with single.execute().collect() as rows:
            self.assertEqual([Row("Alice:42")], list(rows))

    def test_stateless_execution(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        table_env.get_config().set("python.fn-execution.bundle.size", "1")
        table_env.create_temporary_system_function("tokenize", self._tokenize_function())
        events = table_env.from_elements(
            [("hello world",), ("flink",)],
            DataTypes.ROW([DataTypes.FIELD("text", DataTypes.STRING())]))

        result = events.process("tokenize", lit(" ").as_argument("separator"))

        with result.execute().collect() as rows:
            actual = sorted((row[0], row[1]) for row in rows)
        self.assertEqual(
            [("flink", " "), ("hello", " "), ("world", " ")], actual)

    def test_create_stateful_timer_function(self):
        function = self._count_with_timeout_function()

        java_function = function._java_user_defined_function()
        self.assertTrue(java_function.hasOnTimer())
        state_strategies = java_function.getTypeInference(None).getStateTypeStrategies()
        self.assertEqual(["memory"], list(state_strategies.keySet()))

    def test_stateful_timer_plan(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        function = self._count_with_timeout_function()
        table_env.create_temporary_system_function("count_with_timeout", function)
        table_env.execute_sql("""
            CREATE TEMPORARY TABLE events (
                user_id STRING,
                text STRING,
                ts TIMESTAMP_LTZ(3),
                WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
            ) WITH (
                'connector' = 'datagen',
                'number-of-rows' = '1'
            )
        """)

        result = table_env.from_path("events").partition_by(col("user_id")).process(
            "count_with_timeout",
            lit(60_000).as_argument("timeout_ms"),
            descriptor("ts").as_argument("on_time"))

        self.assertEqual(["user_id", "count", "trigger", "rowtime"],
                         result.get_schema().get_field_names())
        plan = result.explain()
        self.assertIn("ProcessTableFunction", plan)
        self.assertIn("Exchange", plan)

    def test_order_by_plan(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        function = udptf(
            OrderedScores(),
            arguments=[ProcessTableFunctionArgument.table(
                "event", traits={Trait.SET_SEMANTIC_TABLE})],
            result_type=DataTypes.ROW([DataTypes.FIELD("score", DataTypes.INT())]),
        )
        table_env.create_temporary_system_function("ordered_scores", function)
        table_env.execute_sql("""
            CREATE TEMPORARY TABLE ordered_events (
                user_id STRING,
                score INT,
                ts TIMESTAMP_LTZ(3),
                WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
            ) WITH (
                'connector' = 'datagen',
                'number-of-rows' = '1'
            )
        """)

        result = table_env.from_path("ordered_events").partition_by(
            col("user_id")).order_by(col("ts").asc, col("score").desc).process(
                "ordered_scores")

        plan = result.explain()
        self.assertIn("ProcessTableFunction", plan)
        self.assertIn("ORDER BY", plan)
        self.assertIn("DESC", plan)

    def test_table_semantics_execution(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        table_env.get_config().set("python.fn-execution.bundle.size", "1")
        function = udptf(
            InspectTableSemantics(),
            arguments=[ProcessTableFunctionArgument.table(
                "event", traits={Trait.SET_SEMANTIC_TABLE})],
            result_type=DataTypes.ROW([
                DataTypes.FIELD("field_count", DataTypes.INT()),
                DataTypes.FIELD("partition_column", DataTypes.INT()),
                DataTypes.FIELD("insert_only", DataTypes.BOOLEAN()),
            ]),
        )
        table_env.create_temporary_system_function("inspect_semantics", function)
        events = table_env.from_elements(
            [(7, "flink")],
            DataTypes.ROW([
                DataTypes.FIELD("user_id", DataTypes.BIGINT()),
                DataTypes.FIELD("value", DataTypes.STRING()),
            ]))

        result = events.partition_by(col("user_id")).process("inspect_semantics")

        with result.execute().collect() as rows:
            self.assertEqual([Row(7, 2, 0, True)], list(rows))

    def test_upsert_output_execution(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        table_env.get_config().set("python.fn-execution.bundle.size", "1")
        function = udptf(
            EmitDeclaredKind(),
            arguments=[ProcessTableFunctionArgument.table(
                "event", traits={Trait.SET_SEMANTIC_TABLE})],
            result_type=DataTypes.ROW([DataTypes.FIELD("value", DataTypes.STRING())]),
            changelog_mode=ChangelogMode.upsert(False),
        )
        table_env.create_temporary_system_function("emit_upsert", function)
        events = table_env.from_elements(
            [(1, RowKind.INSERT.value, "first"),
             (1, RowKind.UPDATE_AFTER.value, "second"),
             (1, RowKind.DELETE.value, "second")],
            DataTypes.ROW([
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("kind", DataTypes.INT()),
                DataTypes.FIELD("value", DataTypes.STRING()),
            ]))

        result = events.partition_by(col("id")).process("emit_upsert")

        table_env.execute_sql("""
            CREATE TEMPORARY TABLE upsert_sink (
                id INT,
                payload STRING,
                PRIMARY KEY (id) NOT ENFORCED
            ) WITH (
                'connector' = 'blackhole'
            )
        """)
        result.execute_insert("upsert_sink").wait()

    def test_retract_output_supports_row_semantics(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        table_env.get_config().set("python.fn-execution.bundle.size", "1")
        function = udptf(
            EmitDeclaredKind(),
            arguments=[ProcessTableFunctionArgument.table(
                "event", traits={Trait.ROW_SEMANTIC_TABLE})],
            result_type=DataTypes.ROW([DataTypes.FIELD("value", DataTypes.STRING())]),
            changelog_mode=ChangelogMode.all(),
        )
        table_env.create_temporary_system_function("emit_retract", function)
        events = table_env.from_elements(
            [(RowKind.UPDATE_BEFORE.value, "old"),
             (RowKind.UPDATE_AFTER.value, "new")],
            DataTypes.ROW([
                DataTypes.FIELD("kind", DataTypes.INT()),
                DataTypes.FIELD("value", DataTypes.STRING()),
            ]))

        with events.process("emit_retract").execute().collect() as rows:
            actual = [(row.get_row_kind(), row[0]) for row in rows]
        self.assertEqual([
            (RowKind.UPDATE_BEFORE, "old"),
            (RowKind.UPDATE_AFTER, "new"),
        ], actual)

    def test_updating_input_preserves_row_kinds(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        table_env.get_config().set("python.fn-execution.bundle.size", "1")
        function = udptf(
            ForwardInputKind(),
            arguments=[ProcessTableFunctionArgument.table(
                "event",
                traits={
                    Trait.SET_SEMANTIC_TABLE,
                    Trait.SUPPORT_UPDATES,
                    Trait.REQUIRE_UPDATE_BEFORE,
                })],
            result_type=DataTypes.ROW([DataTypes.FIELD("kind", DataTypes.STRING())]),
            changelog_mode=ChangelogMode.all(),
        )
        table_env.create_temporary_system_function("forward_changes", function)
        events = table_env.from_elements(
            [("A", 1), ("A", 2)],
            DataTypes.ROW([
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("score", DataTypes.INT()),
            ]))
        updates = events.group_by(col("name")).select(
            col("name"), col("score").sum.alias("score"))

        result = updates.partition_by(col("name")).process("forward_changes")

        with result.execute().collect() as rows:
            actual_kinds = [row.get_row_kind() for row in rows]
        self.assertEqual(
            [RowKind.INSERT, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER], actual_kinds)

    def test_rejects_output_kind_outside_declared_mode(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        table_env.get_config().set("python.fn-execution.bundle.size", "1")
        function = udptf(
            EmitUnexpectedDelete(),
            arguments=[ProcessTableFunctionArgument.table(
                "event", traits={Trait.ROW_SEMANTIC_TABLE})],
            result_type=DataTypes.ROW([DataTypes.FIELD("value", DataTypes.STRING())]),
        )
        table_env.create_temporary_system_function("invalid_change", function)
        events = table_env.from_elements(
            [("value",)],
            DataTypes.ROW([DataTypes.FIELD("value", DataTypes.STRING())]))

        with self.assertRaisesRegex(Exception, "Invalid row kind received: DELETE"):
            with events.process("invalid_change").execute().collect() as rows:
                list(rows)

    def test_upsert_output_requires_set_semantics(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        function = udptf(
            EmitDeclaredKind(),
            arguments=[ProcessTableFunctionArgument.table(
                "event", traits={Trait.ROW_SEMANTIC_TABLE})],
            result_type=DataTypes.ROW([DataTypes.FIELD("value", DataTypes.STRING())]),
            changelog_mode=ChangelogMode.upsert(),
        )
        table_env.create_temporary_system_function("invalid_upsert", function)
        events = table_env.from_elements(
            [(RowKind.INSERT.value, "value")],
            DataTypes.ROW([
                DataTypes.FIELD("kind", DataTypes.INT()),
                DataTypes.FIELD("value", DataTypes.STRING()),
            ]))

        with self.assertRaisesRegex(Exception, "row semantics.*upsert output"):
            events.process("invalid_upsert").explain()

    def test_updating_input_rejects_pass_through_columns(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        function = udptf(
            ForwardInputKind(),
            arguments=[ProcessTableFunctionArgument.table(
                "event",
                traits={
                    Trait.ROW_SEMANTIC_TABLE,
                    Trait.PASS_COLUMNS_THROUGH,
                    Trait.SUPPORT_UPDATES,
                })],
            result_type=DataTypes.ROW([DataTypes.FIELD("kind", DataTypes.STRING())]),
            changelog_mode=ChangelogMode.all(),
        )
        table_env.create_temporary_system_function("invalid_updates", function)
        events = table_env.from_elements(
            [("value",)],
            DataTypes.ROW([DataTypes.FIELD("value", DataTypes.STRING())]))

        with self.assertRaisesRegex(Exception, "updating inputs must not pass columns through"):
            events.process("invalid_updates").explain()

    def test_stateful_named_timer_execution(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        table_env.get_config().set("python.fn-execution.bundle.size", "1")
        table_env.create_temporary_system_function(
            "count_with_timeout", self._count_with_timeout_function())
        table_env.execute_sql("""
            CREATE TEMPORARY TABLE timer_events (
                user_id BIGINT,
                ts TIMESTAMP_LTZ(3),
                WATERMARK FOR ts AS ts
            ) WITH (
                'connector' = 'datagen',
                'number-of-rows' = '1',
                'fields.user_id.kind' = 'sequence',
                'fields.user_id.start' = '1',
                'fields.user_id.end' = '1'
            )
        """)

        result = table_env.from_path("timer_events").partition_by(col("user_id")).process(
            "count_with_timeout",
            lit(0).as_argument("timeout_ms"),
            descriptor("ts").as_argument("on_time")).select(col("count"), col("trigger"))

        with result.execute().collect() as rows:
            actual = sorted((row[0], row[1]) for row in rows)
        self.assertEqual([(1, "event"), (1, "timeout")], actual)

    def test_state_is_isolated_by_partition_key(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        table_env.get_config().set("python.fn-execution.bundle.size", "1")
        table_env.create_temporary_system_function(
            "count_by_key", self._count_by_key_function())
        events = table_env.from_elements(
            [(1,), (2,), (1,)],
            DataTypes.ROW([DataTypes.FIELD("user_id", DataTypes.BIGINT())]))

        result = events.partition_by(col("user_id")).process("count_by_key")

        with result.execute().collect() as rows:
            actual = sorted((row[0], row[1]) for row in rows)
        self.assertEqual([(1, 1), (1, 2), (2, 1)], actual)

    def test_optional_partition_uses_global_state(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        table_env.get_config().set("python.fn-execution.bundle.size", "1")
        table_env.create_temporary_system_function(
            "global_count", self._count_by_key_function(optional_partition=True))
        events = table_env.from_elements(
            [(1,), (2,), (3,)],
            DataTypes.ROW([DataTypes.FIELD("value", DataTypes.BIGINT())]))

        result = events.process("global_count")

        with result.execute().collect() as rows:
            actual = sorted(row[0] for row in rows)
        self.assertEqual([1, 2, 3], actual)

    def test_pass_through_columns_execution(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        table_env.get_config().set("python.fn-execution.bundle.size", "1")
        function = udptf(
            PassThroughLength(),
            arguments=[ProcessTableFunctionArgument.table(
                "event", traits={Trait.ROW_SEMANTIC_TABLE, Trait.PASS_COLUMNS_THROUGH})],
            result_type=DataTypes.ROW([DataTypes.FIELD("length", DataTypes.INT())]),
        )
        table_env.create_temporary_system_function("pass_length", function)
        events = table_env.from_elements(
            [("flink", "stream")],
            DataTypes.ROW([
                DataTypes.FIELD("text", DataTypes.STRING()),
                DataTypes.FIELD("category", DataTypes.STRING()),
            ]))

        result = events.process("pass_length")

        self.assertEqual(["text", "category", "length"],
                         result.get_schema().get_field_names())
        with result.execute().collect() as rows:
            self.assertEqual([Row("flink", "stream", 5)], list(rows))

    def test_timer_execution_without_on_time_argument(self):
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        table_env.get_config().set("python.fn-execution.bundle.size", "1")
        table_env.create_temporary_system_function(
            "optional_on_time", self._optional_on_time_timer_function())
        table_env.execute_sql("""
            CREATE TEMPORARY TABLE optional_timer_events (
                user_id BIGINT,
                sequence_id BIGINT,
                ts AS TO_TIMESTAMP_LTZ(sequence_id * 1000, 3),
                WATERMARK FOR ts AS ts
            ) WITH (
                'connector' = 'datagen',
                'number-of-rows' = '1',
                'fields.user_id.kind' = 'sequence',
                'fields.user_id.start' = '1',
                'fields.user_id.end' = '1',
                'fields.sequence_id.kind' = 'sequence',
                'fields.sequence_id.start' = '1',
                'fields.sequence_id.end' = '1'
            )
        """)

        result = table_env.from_path("optional_timer_events").partition_by(
            col("user_id")).process("optional_on_time").select(col("trigger"))

        self.assertEqual(["trigger"], result.get_schema().get_field_names())
        with result.execute().collect() as rows:
            actual = sorted(row[0] for row in rows)
        self.assertEqual(["event", "timeout"], actual)

    def test_rejects_invalid_eval_signature(self):
        class Invalid(ProcessTableFunction):
            def eval(self, ctx, separator, event):
                return ()

        with self.assertRaisesRegex(ValueError, "Expected \\(ctx, event, separator\\)"):
            udptf(
                Invalid(),
                arguments=[
                    ProcessTableFunctionArgument.table("event"),
                    ProcessTableFunctionArgument.scalar("separator", DataTypes.STRING()),
                ],
                result_type=DataTypes.ROW([DataTypes.FIELD("v", DataTypes.STRING())]),
            )

    def test_state_requires_set_semantics(self):
        class Stateful(ProcessTableFunction):
            def eval(self, ctx, memory, event):
                return ()

        with self.assertRaisesRegex(ValueError, "State requires"):
            udptf(
                Stateful(),
                arguments=[ProcessTableFunctionArgument.table("event")],
                states=[ProcessTableFunctionState.value(
                    "memory", DataTypes.ROW([DataTypes.FIELD("v", DataTypes.INT())]))],
                result_type=DataTypes.ROW([DataTypes.FIELD("v", DataTypes.INT())]),
            )

    def test_timer_allows_optional_on_time(self):
        class Timer(ProcessTableFunction):
            def eval(self, ctx, event):
                return ()

            def on_timer(self, ctx):
                return ()

        function = udptf(
            Timer(),
            arguments=[ProcessTableFunctionArgument.table(
                "event", traits={Trait.SET_SEMANTIC_TABLE})],
            result_type=DataTypes.ROW([DataTypes.FIELD("v", DataTypes.INT())]),
        )

        self.assertTrue(function._java_user_defined_function().hasOnTimer())

    def test_timer_rejects_pass_through_columns(self):
        class Timer(ProcessTableFunction):
            def eval(self, ctx, event):
                return ()

            def on_timer(self, ctx):
                return ()

        with self.assertRaisesRegex(ValueError, "pass-through columns"):
            udptf(
                Timer(),
                arguments=[ProcessTableFunctionArgument.table(
                    "event",
                    traits={
                        Trait.SET_SEMANTIC_TABLE,
                        Trait.REQUIRE_ON_TIME,
                        Trait.PASS_COLUMNS_THROUGH,
                    })],
                result_type=DataTypes.ROW([DataTypes.FIELD("v", DataTypes.INT())]),
            )


if __name__ == '__main__':
    import unittest
    unittest.main()
