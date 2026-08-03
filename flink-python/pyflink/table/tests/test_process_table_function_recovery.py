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

import os

from pyflink.common import Configuration, Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.java_gateway import get_gateway
from pyflink.table import DataTypes, StreamTableEnvironment
from pyflink.table.expressions import col, descriptor, lit
from pyflink.table.udf import (
    ProcessTableFunction,
    ProcessTableFunctionArgument,
    ProcessTableFunctionArgumentTrait as Trait,
    ProcessTableFunctionState,
    udptf,
)
from pyflink.testing.test_case_utils import PyFlinkTestCase


class CountAndRegisterTimer(ProcessTableFunction):
    def eval(self, ctx, memory, event, timeout_ms):
        memory["count"] = (memory.count or 0) + 1
        event_time = ctx.time_context(int).time()
        ctx.time_context(int).register_on_time("timeout", event_time + timeout_ms)
        yield Row(event.sequence_id, memory.count, "event")

    def on_timer(self, ctx, memory):
        yield Row(None, memory.count, ctx.current_timer())
        ctx.clear_all_timers()


class ProcessTableFunctionRecoveryTests(PyFlinkTestCase):

    def test_value_state_and_named_timer_recover_after_task_manager_failure(self):
        gateway = get_gateway()
        checkpoint_dir = "file://" + os.path.join(self.tempdir, "checkpoints")
        configuration = Configuration()
        configuration.set_string("state.backend.type", "hashmap")
        configuration.set_string("execution.checkpointing.dir", checkpoint_dir)
        configuration.set_string("restart-strategy.type", "fixed-delay")
        configuration.set_string("restart-strategy.fixed-delay.attempts", "3")
        configuration.set_string("restart-strategy.fixed-delay.delay", "100 ms")

        resource_configuration = (
            gateway.jvm.org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
            .Builder()
            .setConfiguration(configuration._j_configuration)
            .setNumberTaskManagers(1)
            .setNumberSlotsPerTaskManager(2)
            .build())
        resource = (
            gateway.jvm.org.apache.flink.test.util.MiniClusterWithClientResource(
                resource_configuration))
        resource.before()
        try:
            env = StreamExecutionEnvironment(
                gateway.jvm.org.apache.flink.streaming.util.TestStreamEnvironment(
                    resource.getMiniCluster(), 1))
            env.configure(configuration)
            env.enable_checkpointing(3_600_000)
            env.set_parallelism(1)
            table_env = StreamTableEnvironment.create(env)
            table_env.get_config().set("python.fn-execution.bundle.size", "1")
            function = udptf(
                CountAndRegisterTimer(),
                arguments=[
                    ProcessTableFunctionArgument.table(
                        "event", traits={Trait.SET_SEMANTIC_TABLE, Trait.REQUIRE_ON_TIME}),
                    ProcessTableFunctionArgument.scalar("timeout_ms", DataTypes.BIGINT()),
                ],
                states=[ProcessTableFunctionState.value(
                    "memory",
                    DataTypes.ROW([DataTypes.FIELD("count", DataTypes.BIGINT())]))],
                result_type=DataTypes.ROW([
                    DataTypes.FIELD("sequence_id", DataTypes.BIGINT()),
                    DataTypes.FIELD("count", DataTypes.BIGINT()),
                    DataTypes.FIELD("trigger", DataTypes.STRING()),
                ]),
            )
            table_env.create_temporary_system_function("recovering_count", function)
            table_env.execute_sql("""
                CREATE TEMPORARY TABLE recovery_events (
                    user_id AS CAST(1 AS BIGINT),
                    sequence_id BIGINT,
                    ts AS TO_TIMESTAMP_LTZ(sequence_id * 1000, 3),
                    WATERMARK FOR ts AS ts
                ) WITH (
                    'connector' = 'datagen',
                    'number-of-rows' = '1000',
                    'rows-per-second' = '50',
                    'scan.parallelism' = '1',
                    'fields.sequence_id.kind' = 'sequence',
                    'fields.sequence_id.start' = '1',
                    'fields.sequence_id.end' = '1000'
                )
            """)

            result = table_env.from_path("recovery_events").partition_by(
                col("user_id")).process(
                    "recovering_count",
                    lit(3_600_000).as_argument("timeout_ms"),
                    descriptor("ts").as_argument("on_time")).select(
                        col("sequence_id"), col("count"), col("trigger"))
            table_result = result.execute()
            job_client = table_result.get_job_client()
            job_id = job_client.get_job_id()._j_job_id

            with table_result.collect() as rows:
                first = next(rows)
                self.assertEqual("event", first[2])
                resource.getMiniCluster().triggerCheckpoint(job_id).get()
                resource.getMiniCluster().terminateTaskManager(0).get()
                resource.getMiniCluster().startTaskManager()
                timer_rows = []
                sequence_1000 = None
                for row in rows:
                    if row[2] == "timeout":
                        timer_rows.append(row)
                    elif row[0] == 1000:
                        sequence_1000 = row

            self.assertEqual(Row(1000, 1000, "event"), sequence_1000)
            self.assertEqual([Row(None, 1000, "timeout")], timer_rows)
        finally:
            resource.after()


if __name__ == '__main__':
    import unittest
    unittest.main()
