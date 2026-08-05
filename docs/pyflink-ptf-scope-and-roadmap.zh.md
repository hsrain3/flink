<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# PyFlink Process Table Function 首版范围与后续规划

## 目标

PyFlink PTF Phase 1 以补齐单表输入的 Streaming PTF 完整链路为目标，而不是一次性实现与
Java PTF 的全部功能对齐。

用户可以使用 `@udptf` 装饰普通 Python 函数，通过有序 mapping 声明参数、状态和结果类型，
注册函数名称后使用 `Table.process()` 或 `TableEnvironment.from_call()` 调用。复杂函数仍可使用
`ProcessTableFunction` 类风格，但本文的用户示例统一采用推荐的 Pythonic decorator 风格。

## 社区实现现状

本节基于 2026-07-22 的 Apache Flink 社区状态。代码基线为 Apache Flink `master`
提交 [`a0605af9`](https://github.com/apache/flink/commit/a0605af9be86bec09dbf7d75969644eb8d268792)，
发布状态参考
[`Flink 2.3` 稳定版 PTF 文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/functions/ptfs/)。
社区已发布能力、社区 `master` 能力和本文工作区中的未合入实现需要严格区分。

### Java PTF

Java PTF 由
[`FLIP-440`](https://cwiki.apache.org/confluence/spaces/FLINK/pages/298781093/FLIP-440+User-defined+SQL+operators+ProcessTableFunction+PTF)
引入，已经进入 Apache Flink 社区实现和稳定版文档。当前 Java PTF 具备以下主要能力：

- 可以通过 Java Table API 或注册后的 SQL 调用，Table API 同时支持注册名称和 inline
  function class。
- 支持零表、单表和多表参数，以及标量参数、optional arguments 和系统参数
  `on_time`、`uid`。
- 支持 Row semantics、Set semantics、`PARTITION BY`、`ORDER BY`、pass-through columns
  和运行时 `TableSemantics`。
- 支持 `Row`/POJO Value State、`ListView`、`MapView`、独立 TTL、状态清理、checkpoint
  和恢复。
- 支持事件时间、watermark、named/anonymous Timer、Timer 清理和 `onTimer()` 回调。
- 支持 append、upsert 和 retract changelog 输入输出，以及 update-before 和 full-delete
  要求。
- 支持通过稳定 UID 进行有状态查询演进，并提供 Java `ProcessTableFunctionTestHarness`。

Java PTF 当前仍只支持 Streaming，不支持 Batch、processing-time Timer 和 broadcast
state。社区仍在继续演进 PTF；例如
[`FLINK-39254`/`FLIP-565`](https://issues.apache.org/jira/browse/FLINK-39254)
跟踪迟到数据、懒加载 Value State 和 broadcast table semantics 等改进，其中总任务目前
仍为 Open，不能把其全部目标视为已经发布的能力。

### PyFlink 社区基线

截至上述基线，Apache Flink 官方教程仍将 PTF 标记为
[`Java only`](https://nightlies.apache.org/flink/flink-docs-master/docs/getting-started/table_api/#process-table-functions-java-only)。
社区 `master` 中尚未提供以下 PyFlink 能力：

- 没有 Python `ProcessTableFunction` 基类、`udptf()` 或 Python PTF 参数/state 声明 API。
- 没有面向 Python-defined PTF 的 planner/runtime 分支、Python operator、Beam runner、
  worker operation 或 Timer bridge。
- 没有通用的 Python `Table.process()`、`PartitionedTable` 和
  `TableEnvironment.from_call()` PTF 调用链。

PyFlink 已有一些可复用但不等价于 Python PTF 的能力：

- Python UDF、UDTF、UDAF 和 DataStream ProcessFunction 已有成熟 API；DataStream
  ProcessFunction 已支持 keyed state 和事件时间/处理时间 Timer。
- Process Python worker、Beam Fn API、远程 keyed state、bundle、checkpoint、metrics
  和 coder 基础设施可以被 Python PTF 复用。
- `Table.to_changelog()` 和 `Table.from_changelog()` 已通过专用 Table API 调用社区内置
  Java PTF；这不代表用户已经可以用 Python 定义 PTF。
- PyFlink 作业可以注册 Java PTF class 并通过 SQL/JVM planner 使用它，实际函数仍运行在
  JVM，不经过 Python worker。社区基线尚没有本文计划中的通用 Python Table API PTF
  调用便利方法。

### 当前工作区实现

本文所描述的 Python PTF Phase 1 已经在当前功能分支实现，包括 Python API、Java
占位函数、共享 PTF planner 的 Python 分支、单输入 operator、PTF Beam runner、Python
worker、Value State、State Views、事件时间 Timer bridge、`ORDER BY`、TableSemantics、
changelog 和最小恢复验证。该实现复用社区 Java PTF 语义以及现有 Flink/Python state、
checkpoint、watermark 和 Beam 基础设施。

本文示例采用目标 Pythonic public API。普通函数 decorator、mapping shorthand 和无名称
state/argument descriptor 仍需要在 Python API PR 中落地；它们只负责将声明归一化为现有
有序参数和状态模型，不改变已经实现的 planner、proto 或 runtime 语义。

这些改动尚未通过 Apache Flink 社区的 FLIP、代码审查和发布流程。因此本文中的
“已实现”仅表示当前功能分支状态，不表示 Apache Flink 稳定版或社区 `master` 已支持
Python-defined PTF。新增 PyFlink 公共 API 在合入前仍需要获得 FLIP 批准，并补充 JIRA、
release note、兼容性说明和社区级 CI 验证。

## Phase 1 支持范围

| 领域 | Phase 1 支持范围 | 对应 Case |
|------|--------------|-----------|
| 定义方式 | 使用 `@udptf` 装饰普通 Python 函数，通过 mapping 声明参数和状态 | Case 1 |
| 调用方式 | 注册名称后使用 `Table.process()`、`TableEnvironment.from_call()` 或 SQL | Case 2、17 |
| 参数 | 恰好一个表参数，以及零个或多个按声明顺序注入的标量参数 | Case 3 |
| 表语义 | 支持 Row/Set semantics、分区以及单表核心 `TableSemantics` metadata | Case 4、14 |
| 表参数 Trait | 支持 pass-through columns 和 required on-time，但 Timer 不能与 pass-through columns 同时使用 | Case 5 |
| 排序 | 支持 Set semantics 的 `ORDER BY`、watermark 释放、次级排序和迟到数据处理 | Case 13 |
| 状态 | 支持一个或多个 keyed `ROW` Value State、`ListView` 和 `MapView`，以及显式清理、独立 TTL 和增量访问 | Case 6、16 |
| 时间 | 使用 epoch 毫秒、`Instant` 或 UTC-naive `datetime` 访问事件时间、表 watermark 和当前 PTF watermark | Case 7 |
| Timer | 支持 named 和 anonymous 事件时间 Timer，以及替换、单个删除、全部清理和回调 | Case 8 |
| 输出 | 支持 0-N 条 `ROW` 结果，并组合分区键、pass-through 列和可选 rowtime | Case 9 |
| Changelog | 支持 append/upsert/retract 输入输出及更新 Trait，输入输出保留 `RowKind` | Case 15 |
| Runtime | Streaming 和 Process Python worker | Case 10 |
| 可靠性 | 状态、排序缓冲和 Timer 使用 Flink managed state；具备固定并行度 HashMap backend 的最小 checkpoint/failover/TTL 恢复验证 | Case 11、18 |
| Java 互操作 | PyFlink 可以调用已注册的 Java PTF，包括多表输入 Java PTF | Case 12 |

## Phase 1 支持范围 Case

下面的 case 与上表对应。除 changelog Case 外，示例假设 `events` 是 append-only 流表，包含
`user_id STRING`、`text STRING` 和已定义 watermark 的 `ts TIMESTAMP_LTZ(3)`。

### Case 1：在 Python 中定义 PTF

```python
from pyflink.common import Row
from pyflink.table import DataTypes
from pyflink.table.udf import (
    ProcessTableFunctionArgumentTrait as Trait,
    table_arg,
    udptf,
)


@udptf(
    arguments={
        "event": table_arg(traits={Trait.ROW_SEMANTIC_TABLE}),
        "separator": DataTypes.STRING(),
    },
    result_type="ROW<token STRING>",
)
def tokenize(ctx, event, separator):
    for token in event.text.split(separator):
        if token:
            yield Row(token=token)


t_env.create_temporary_system_function("tokenize", tokenize)
```

**预期：** `udptf()` 保留 `arguments` mapping 的插入顺序，并校验回调必须是
`(ctx, event, separator)`。名称、数量或顺序不匹配时在定义阶段报错。

### Case 2：隐式和显式调用

```python
from pyflink.table.expressions import lit

implicit_result = events.process(
    "tokenize",
    lit(" ").as_argument("separator"),
)

explicit_result = t_env.from_call(
    "tokenize",
    events.as_argument("event"),
    lit(" ").as_argument("separator"),
)
```

**预期：** 两种调用生成等价的 PTF 计划。`Table.process()` 自动将当前表作为
第一个表参数；`from_call()` 需显式传入并命名表参数。输入 `"hello world"`
时产生 `"hello"` 和 `"world"` 两条结果。

### Case 3：单表参数和有序标量参数

```python
@udptf(
    arguments={
        "event": table_arg(),
        "prefix": DataTypes.STRING(),
        "suffix": DataTypes.STRING(),
    },
    result_type="ROW<value STRING>",
)
def wrap(ctx, event, prefix, suffix):
    yield Row(value=prefix + event.text + suffix)
```

**预期：** 每次回调按 `event`、`prefix`、`suffix` 的声明顺序注入参数。首版
必须恰好声明一个表参数，可以不声明标量参数，也可以声明多个标量参数。

### Case 4：Row semantics 和 Set semantics

```python
# 无状态：每行独立调用。
row_argument = table_arg(traits={Trait.ROW_SEMANTIC_TABLE})

# 有状态：相同 user_id 共享一份 keyed state。
set_argument = table_arg(traits={Trait.SET_SEMANTIC_TABLE})
result = events.partition_by(col("user_id")).process("count_by_key")
```

**预期：** 输入 `(A, x)`、`(B, y)`、`(A, z)` 时，`count_by_key` 输出 `(A, 1)`、
`(B, 1)`、`(A, 2)`。声明 `OPTIONAL_PARTITION_BY` 后也可不调用
`partition_by()`，此时所有输入使用同一份全局逻辑分区状态。

### Case 5：表参数 Trait

```python
pass_through = table_arg(
    traits={Trait.ROW_SEMANTIC_TABLE, Trait.PASS_COLUMNS_THROUGH},
)

required_time = table_arg(
    traits={Trait.SET_SEMANTIC_TABLE, Trait.REQUIRE_ON_TIME},
)
result = events.partition_by(col("user_id")).process(
    "count_with_timeout",
    descriptor("ts").as_argument("on_time"),
)
```

**预期：** `PASS_COLUMNS_THROUGH` 使原表列保留在输出中。`REQUIRE_ON_TIME` 要求
调用时传入 `on_time` descriptor，并将 rowtime 传播到输出。首版不允许带 Timer
的 PTF 同时使用 pass-through columns。

### Case 6：多个 ROW Value State、TTL 和清理

```python
from pyflink.common import Duration
from pyflink.table.udf import value_state


@udptf(
    arguments={
        "event": table_arg(traits={Trait.SET_SEMANTIC_TABLE}),
    },
    states={
        "count_state": value_state(
            "ROW<value BIGINT>", ttl=Duration.of_days(1)
        ),
        "last_state": value_state("ROW<text STRING>"),
    },
    result_type="ROW<count BIGINT, previous STRING>",
)
def count_and_remember(ctx, count_state, last_state, event):
    previous = last_state.text
    count_state["value"] = (count_state.value or 0) + 1
    last_state["text"] = event.text
    yield Row(count=count_state.value, previous=previous)

    if event.text == "reset":
        ctx.clear_state("count_state")
```

**预期：** 状态按声明顺序、作为可变 `Row` 注入，并在 generator 消费完成后自动
写回当前分区。初始值是所有字段为 `None` 的 `Row`；全空 `Row` 自动删除。
`clear_state()` 优先于对象修改；回调异常时不写回；每个状态可以配置独立 TTL。

### Case 7：三种时间表示和 watermark

```python
from datetime import datetime
from pyflink.common import Instant


@udptf(
    arguments={
        "event": table_arg(
            traits={Trait.ROW_SEMANTIC_TABLE, Trait.REQUIRE_ON_TIME}
        ),
    },
    result_type="ROW<event_time BIGINT>",
)
def inspect_time(ctx, event):
    epoch_millis = ctx.time_context(int).time()
    instant = ctx.time_context(Instant).time()
    utc_datetime = ctx.time_context(datetime).time()

    table_watermark = ctx.time_context(int).table_watermark()
    ptf_watermark = ctx.time_context(int).current_watermark()
    yield Row(event_time=epoch_millis)
```

**预期：** 同一个时间值可以表示为 epoch 毫秒、`Instant` 或 UTC-naive
`datetime`。没有可用时间或 watermark 时返回 `None`；timezone-aware `datetime`
会被拒绝。只有传入 `on_time` 时，`time()` 才能返回当前行时间，并向输出传播
rowtime。`table_watermark()` 返回当前输入表的 watermark，`current_watermark()`
返回当前 PTF 的 watermark；两者在启动或恢复期间都可能为 `None`。

### Case 8：named 和 anonymous 事件时间 Timer

```python
@udptf(
    arguments={
        "event": table_arg(
            traits={Trait.SET_SEMANTIC_TABLE, Trait.REQUIRE_ON_TIME}
        ),
    },
    result_type="ROW<timer_name STRING>",
)
def timeout(ctx, event):
    timers = ctx.time_context(int)
    event_time = timers.time()
    if event_time is not None:
        timers.register_on_time("timeout", event_time + 10_000)
        # 同名 Timer 被替换为更晚的时间。
        timers.register_on_time("timeout", event_time + 20_000)
        timers.register_on_time(event_time + 30_000)  # anonymous Timer

    if event.text == "cancel":
        timers.clear_timer("timeout")

    return


@timeout.on_timer
def timeout_on_timer(ctx):
    yield Row(timer_name=ctx.current_timer())
    ctx.clear_all()
```

**预期：** 同一 key 下同名 Timer 只保留最后一次注册；anonymous Timer 按时间戳独立注册。
watermark 越过时间戳后调用 `on_timer()`，named Timer 可通过 `current_timer()`
获取 `"timeout"`。支持按名称或时间删除、`clear_all_timers()` 和同时清理状态与
Timer 的 `clear_all()`。

### Case 9：单次调用输出 0-N 条结果

```python
@udptf(
    arguments={"event": table_arg()},
    result_type="ROW<token STRING>",
)
def positive_tokens(ctx, event):
    if event.text is None:
        return
    for token in event.text.split(" "):
        if token:
            yield Row(token=token)
```

**预期：** `text=None` 输出 0 条，`text="flink"` 输出 1 条，
`text="hello flink"` 输出 2 条。结果必须是 append-only `ROW`，运行时根据声明组合
分区键、pass-through 列和可选 rowtime。

### Case 10：Streaming Process worker 执行

```python
from pyflink.table import EnvironmentSettings, TableEnvironment

t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
t_env.get_config().set("python.execution-mode", "process")
```

**预期：** Python PTF 通过独立 Process Python worker 执行。Batch mode、Embedded/Thread mode
或非 `ROW` 结果会在 API 或 planner 阶段被拒绝；更新输入输出由声明的 Trait 和
`ChangelogMode` 控制。

### Case 11：checkpoint、watermark 和恢复

假设 key `A` 的状态计数为 3，并已注册事件时间 Timer `timeout@20s`：

1. checkpoint barrier 到达时，operator 先刷新 Python bundle、远程状态请求和 Timer 命令。
2. checkpoint 将 keyed Value State、State Views、排序缓冲、named Timer 映射和 Flink
   internal Timer 一起持久化。
3. 任务故障并从该 checkpoint 恢复后，key `A` 的下一条输入从计数 3 继续，
   watermark 越过 20 秒时原 Timer 仍可触发。

end-of-input 和 watermark 前也会进行同类刷新，防止控制命令滞留在 Python bundle 中。
PTF 状态和 Timer 参与 Flink checkpoint，但作业端到端 exactly-once 仍取决于 source 和
sink 是否支持对应保证。

### Case 12：从 PyFlink 调用 Java PTF

```python
t_env.create_java_temporary_system_function(
    "java_timeout_ptf",
    "com.example.JavaTimeoutProcessTableFunction",
)
single_result = events.partition_by(col("user_id")).process(
    "java_timeout_ptf",
    descriptor("ts").as_argument("on_time"),
)

multi_result = t_env.from_call(
    "java_multi_input_ptf",
    orders.partition_by(col("user_id")).as_argument("orders"),
    profiles.partition_by(col("user_id")).as_argument("profiles"),
    lit(100).as_argument("threshold"),
)
```

**预期：** Java 单表 PTF 可用 `process()` 调用，Java 多表 PTF 可用 `from_call()` 显式
传入所有表参数。“恰好一个表参数”只限制 Python 定义的 PTF，不限制 PyFlink
调用 Java PTF。

### Case 13：`ORDER BY` 和 watermark 驱动释放

```python
ordered = events.partition_by(col("user_id")).order_by(
    col("ts").asc,
    col("priority").desc,
)
result = ordered.process(
    "ordered_ptf",
    descriptor("ts").as_argument("on_time"),
)
```

**预期：** `eval()` 先按 `ts` 升序接收记录，相同 `ts` 再按 `priority` 降序接收。
第一个排序列必须是带 watermark 的升序时间属性，并与 `on_time` 一致；watermark 推进后
排序缓冲才释放，已经晚于 watermark 的记录按 Java PTF 规则丢弃。排序 MapState 和内部
Timer 参与 checkpoint。

### Case 14：读取当前表参数语义

```python
@udptf(
    arguments={"event": table_arg(traits={Trait.SET_SEMANTIC_TABLE})},
    result_type=(
        "ROW<partition_columns ARRAY<INT>, "
        "order_columns ARRAY<INT>, time_column INT>"
    ),
)
def inspect_input(ctx, event):
    semantics = ctx.table_semantics_for("event")
    yield Row(
        partition_columns=list(semantics.partition_by_columns()),
        order_columns=list(semantics.order_by_columns()),
        time_column=semantics.time_column(),
    )
```

**预期：** context 返回调用点的实际 `DataType`、分区列、排序列及方向、时间列、输入
changelog mode 和 upsert-key candidates。未知参数名抛出 `ValueError`，列集合以不可变
tuple 返回；`ctx.get_changelog_mode()` 返回当前 Python PTF 声明的输出 mode。

### Case 15：更新输入和 changelog 输出

```python
@udptf(
    arguments={
        "event": table_arg(
            traits={Trait.SET_SEMANTIC_TABLE, Trait.SUPPORT_UPDATES}
        ),
    },
    result_type="ROW<value STRING>",
    changelog_mode=ChangelogMode.all(),
)
def forward_changes(ctx, event):
    yield Row.of_kind(event.get_row_kind(), value=event.value)
```

**预期：** 输入 `Row` 保留 `+I/-U/+U/-D`，输出 `RowKind` 不被重置为 INSERT，且运行时
拒绝声明 mode 之外的输出。`REQUIRE_UPDATE_BEFORE` 要求 planner 提供 retract 编码，
`REQUIRE_FULL_DELETE` 要求 DELETE 携带完整行。upsert 输出要求 Set semantics 且 upsert
key 等于 `PARTITION BY` key；retract 输出不要求 upsert key，也可以用于 Row semantics。
更新输入不能与 pass-through columns 组合，更新输出不能与 `on_time` 组合。

### Case 16：`ListView`、`MapView` 和 Value State 混用

```python
from pyflink.table.udf import list_view_state, map_view_state, value_state


@udptf(
    arguments={
        "event": table_arg(traits={Trait.SET_SEMANTIC_TABLE}),
    },
    states={
        "memory": value_state("ROW<total BIGINT>"),
        "history": list_view_state(
            DataTypes.STRING(), ttl=Duration.of_days(1)
        ),
        "counts": map_view_state(
            DataTypes.STRING(), DataTypes.BIGINT(), ttl=Duration.of_days(7)
        ),
    },
    result_type="ROW<total BIGINT, history_size INT, value_count BIGINT>",
)
def track_history(ctx, memory, history, counts, event):
    memory["total"] = (memory.total or 0) + 1
    history.add(event.value)
    counts.put(event.value, (counts.get(event.value) or 0) + 1)
    yield Row(memory.total, len(list(history.get())), counts.get(event.value))
```

**预期：** View 操作直接访问 keyed managed state，不在回调前后整体复制集合。
`ListView` 支持 `get/add/add_all/remove/clear`，`MapView` 支持单 key 读写、删除、contains、
迭代和 clear。三个状态独立配置 TTL，并同时受 `clear_state()`、`clear_all_state()` 和
`clear_all()` 控制。

### Case 17：通过 SQL 调用已注册 Python PTF

```python
t_env.create_temporary_system_function("tokenize", tokenize)
result = t_env.sql_query("""
    SELECT *
    FROM tokenize(event => TABLE events, `separator` => ' ')
""")
```

**预期：** SQL 与 Table API 解析到同一个已注册 Python PTF 和同一套 planner/runtime
链路。Phase 1 仍不支持在调用位置直接传入 inline Python 函数实例或函数类。

### Case 18：Phase 1 最小恢复门禁

固定并行度为 1，并使用默认 HashMap backend：

1. 同一 key 连续累加 `ROW` Value State，并反复以同名 Timer 替换旧 deadline。
2. 完成 checkpoint 后终止唯一 TaskManager，再启动替代 TaskManager。
3. 作业恢复并处理完 1000 条限速输入后，序号和状态计数必须同为 1000；反复替换的
   named Timer 在 end-of-input watermark 到达后只触发一次。
4. 使用短 TTL 分别写入 Value State、`ListView` 和 `MapView`，过期后再次读取均不可见。

**预期：** 该 Case 只证明 Phase 1 的最小 checkpoint、Timer 和 TTL 恢复闭环，不替代
savepoint、rescale、多 backend、压力、性能或长期稳定性矩阵。

## API 约定

`arguments` mapping 的 key 是 PTF 参数名，value 使用 `table_arg()` 声明表参数，或直接使用
`DataType`/类型字符串声明标量参数。`states` mapping 的 key 是状态名，value 使用
`value_state()`、`list_view_state()` 或 `map_view_state()` 声明类型和 TTL。两个 mapping
都保留插入顺序；`table_arg()` 默认使用 Row semantics。`result_type` 同时接受 `DataType`
和类型字符串。

状态和 Timer 使用固定的回调签名，状态参数位于业务参数之前。Timer callback 通过已装饰
函数的 `on_timer` decorator 绑定：

```python
@udptf(
    arguments={
        "table_arg": table_arg(traits={Trait.SET_SEMANTIC_TABLE}),
        "scalar_arg": DataTypes.STRING(),
    },
    states={
        "state1": value_state("ROW<count BIGINT>"),
        "state2": list_view_state(DataTypes.STRING()),
    },
    result_type="ROW<result STRING>",
)
def process_records(ctx, state1, state2, table_arg, scalar_arg):
    ...


@process_records.on_timer
def process_records_on_timer(ctx, state1, state2):
    ...
```

`udptf()` 校验回调参数名和 mapping key 完全匹配。类风格 PTF 仍使用
`eval(self, ctx, ...)` 和 `on_timer(self, ctx, ...)`，但不是本文示例的推荐写法。
主回调或 Timer callback 返回 `None` 表示输出 0 条记录；返回 generator/iterator 时按
迭代顺序输出 0-N 条记录。

状态和 Timer 要求 Set semantics，Row-semantics PTF 仅支持无状态处理。除非表参数声明了
`REQUIRE_ON_TIME`，否则 `on_time` 是可选参数。传入 `on_time` 后，PTF 可以访问当前行时间，
并向输出传播 rowtime；无论是否传入 `on_time`，事件时间 Timer 都由当前 PTF watermark 驱动。

## 当前限制

- Python PTF 必须声明恰好一个表参数，不支持零表输入或多表输入。该限制不影响 PyFlink
  调用 Java PTF。
- 仅支持 Streaming 和 Process Python worker，不支持 Batch 和 Embedded Python mode。
- `result_type` 必须是顶层 `ROW` 类型，不支持顶层标量、`ARRAY`、`MAP`、`RAW` 或动态
  结果类型。
- 输出 changelog mode 必须在 `@udptf` 中固定声明，不支持根据 planning context 动态
  选择输出 mode。
- `table_semantics_for()` 覆盖单表核心 metadata，不包含尚未暴露的高级 Java context API。
- 状态和 Timer 要求 Set semantics，Timer 不能与 pass-through columns 同时使用。
- Timer 仅支持事件时间，并且注册 Timer 的函数必须通过 `@function.on_timer` 绑定回调；
  类风格函数则必须实现 `on_timer()`。
- Python PTF 必须先注册名称，不支持 inline Python 函数实例或函数类。
- 业务 scalar/table arguments 当前全部必填，不支持 optional arguments。
- Phase 1 仅包含固定并行度、HashMap backend 的最小恢复门禁，不包含 savepoint、rescale、
  多 backend、压力、性能和长期稳定性矩阵。

## 阶段规划与当前状态

以下阶段用于说明当前功能边界和后续方向，不构成兼容性或版本承诺。

### 第一阶段：完整的单表 Python PTF

第一阶段目标是让 Python PTF 覆盖完整的单表 Streaming 主路径：

- Python PTF 定义、注册，以及通过 Table API 或 SQL 调用已注册函数。
- 恰好一个表输入和零个或多个标量参数。
- Row/Set semantics、`PARTITION BY`、pass-through columns 和 `on_time`。
- 事件时间 Timer、显式清理和 checkpoint 恢复。
- append、upsert 和 retract 输入输出，以及 `SUPPORT_UPDATES`、
  `REQUIRE_UPDATE_BEFORE` 和 `REQUIRE_FULL_DELETE`。
- `ORDER BY`、watermark 驱动的排序缓冲和排序状态恢复。
- 核心 `ctx.table_semantics_for()`，覆盖输入类型、分区、排序、时间列和 changelog
  metadata。
- 一个或多个 keyed `ROW` Value State、`ListView` 和 `MapView`，支持显式清理、独立
  TTL 和增量访问。
- 固定并行度、HashMap backend 下的最小 checkpoint、Timer 和 TTL 恢复门禁。

阶段一内部按以下里程碑推进，但这些里程碑属于同一个单表能力阶段：

#### 1A Core：append-only MVP，底层已完成

- 提供 `@udptf`、`table_arg()`、状态 descriptor、Trait API，以及兼容复杂实现的
  `ProcessTableFunction` 类风格。
- 支持注册名称后的 `Table.process()`、`PartitionedTable.process()` 和
  `TableEnvironment.from_call()`。
- 支持单表 append-only 输入输出、Row/Set semantics、分区、pass-through 和 on-time。
- 支持 `ROW` Value State、TTL、named/anonymous 事件时间 Timer，以及 Process Python
  worker 执行链路。

#### 1B Parity：单表语义对齐

- 支持核心 table semantics 和 `ORDER BY`。
- 支持更新表输入，以及 update-before、retraction 和 delete 消息。
- 支持 Python PTF 以固定 `ChangelogMode` 声明并产出 append、upsert 或 retract
  changelog；基于 planning context 动态选择 mode 不作为 1B 的前置条件。
- 验证已注册 Python PTF 的 SQL 调用能力。

#### 1C State Views：增量集合状态

- 支持 `list_view_state()` 和 `map_view_state()`。
- 回调中注入 state-backed `ListView` 和 `MapView`，操作直接访问 Flink keyed managed
  state，不整体反序列化和写回集合。
- 支持按状态独立 TTL、按 key 隔离、增删迭代，以及与 Value State 混用。
- `clear_state()`、`clear_all_state()` 和 `clear_all()` 同时覆盖 Value State 和 Views。

#### Phase 1 Exit Gate：最小恢复验证

- 真实 Process Python worker 作业完成 checkpoint 后触发一次 TaskManager failover。
- 验证 keyed Value State 连续、named Timer 恢复并只触发一次。
- 验证短 TTL 的 Value State、`ListView` 和 `MapView` 过期后不可见。
- 使用固定并行度和 HashMap backend，不扩展为生产矩阵。

#### 1B Parity：Table semantics 和 ORDER BY 的详细范围

这里的“核心”包括单表元数据、分区内有序处理和更新流元数据。1B 包含：

| 子能力 | 目标行为 |
|--------|----------|
| Python table-semantics context | 提供 `ctx.table_semantics_for(argument_name)`，返回指定表参数在本次调用中的实际语义 |
| 表类型 | 可读取实际 `DataType`，包括多态表参数在调用时确定的 `ROW` schema |
| 分区元数据 | 可读取 `PARTITION BY` 列的零基位置；可选分区未指定 key 时返回空集合 |
| 排序元数据 | 可读取 `ORDER BY` 列位置、ASC/DESC 方向和 null ordering |
| 时间列元数据 | 可读取 `on_time` 对应的列位置；未指定时返回 `-1` |
| 更新流元数据 | 可读取输入 `changelog_mode()` 和 upsert-key candidates，供函数理解实际收到的变更编码 |
| Table API 调用 | 启用现有 `PartitionedTable.order_by()` 调用链执行 Python-defined PTF，并保留 `as_argument()` 和 `process()` |
| 运行时排序 | 每个表参数、每个分区维护排序缓冲，由该输入表的 watermark 推进并释放有序数据 |
| 恢复语义 | 排序缓冲、watermark 进度和相关 Timer 必须参与 checkpoint，恢复后不破坏分区内顺序 |

目标 Table API 调用形式为：

```python
ordered_events = events.partition_by(col("user_id")).order_by(
    col("ts").asc,
    col("priority").desc,
)

result = ordered_events.process(
    "ordered_ptf",
    descriptor("ts").as_argument("on_time"),
)
```

`ORDER BY` 的规则与 Java PTF 保持一致：

- 只能用于 Set semantics 表参数，先由 `PARTITION BY` 确定数据共置范围。
- 第一个排序列必须是已声明 watermark 的时间属性，必须升序且不能在上游被改写。
- 后续列是同一事件时间下的次级排序键，可以指定升序或降序。
- 如果同时传入 `on_time`，它必须与第一个 `ORDER BY` 时间列相同。`on_time`
  负责 `time()` 和输出 rowtime，`ORDER BY` 负责实际缓冲和重排，两者不可互相替代。
- watermark 到达后，运行时按排序键将已完成的数据交给 `eval()`。为保持严格顺序，
  晚于 watermark 到达的数据按 Java PTF 语义丢弃。
- 排序会增加状态容量和 watermark 之前的处理延迟，需增加大分区、空闲输入、背压和
  checkpoint restore 测试。

Python context 的目标使用方式为：

```python
def inspect_ordering(ctx, event):
    semantics = ctx.table_semantics_for("event")
    input_type = semantics.data_type()
    partition_columns = semantics.partition_by_columns()
    order_columns = semantics.order_by_columns()
    order_directions = semantics.order_by_directions()
    time_column = semantics.time_column()
```

`ORDER BY`、更新输入和更新输出属于 1B 中的独立能力，组合使用时继续遵守 Java
PTF 的校验规则。例如，更新表不能使用 pass-through columns，接收或产出更新的 PTF
不能使用 `on_time`。

#### 1B Parity：更新输入和输出的详细范围

1B 允许 Python PTF 消费和产出 changelog，不再把 operator 固定为 append-only。
目标行为与 Java PTF 一致：

| 子能力 | 目标行为 |
|--------|----------|
| 更新输入声明 | 表参数支持 `SUPPORT_UPDATES`，未声明时仍拒绝更新表 |
| Retract 输入 | 支持 `REQUIRE_UPDATE_BEFORE`，确保更新以 `-U/+U` 形式进入函数 |
| 完整删除 | 支持 `REQUIRE_FULL_DELETE`，避免 upsert 输入的 key-only delete 将非 key 字段置为 `NULL` |
| Python 输入对象 | 传入 `eval()` 的 `Row` 保留 `RowKind`，函数可区分 `+I`、`-U`、`+U` 和 `-D` |
| 输出声明 | 提供与 Java `ChangelogFunction` 等价的 Python 声明，使 planner 能推导 append、upsert 或 retract 输出模式 |
| Python 输出对象 | `eval()` 和 `on_timer()` 产出的 `Row` 可以携带 `RowKind`，runner 和 collector 不得将其重置为 `INSERT` |
| Planner 协商 | 根据输入 changelog、函数能力和下游要求协商输出 changelog mode，并校验 upsert key、update-before 和完整 delete 要求 |
| 恢复语义 | checkpoint 前刷新 Python bundle 和状态请求；故障后从同一个 checkpoint 恢复 PTF 状态并由 source 重放，端到端 exactly-once 仍取决于 source 和 sink |

例如，声明 `REQUIRE_UPDATE_BEFORE` 后，一次更新应以如下两条消息进入 `eval()`：

```text
-U[user_id=Alice, score=10]
+U[user_id=Alice, score=20]
```

如果函数输出 retract changelog，它可以先产出旧聚合值的 `UPDATE_BEFORE`，再产出新值的
`UPDATE_AFTER`。函数声明的 changelog mode 必须与实际产出的 `RowKind` 一致，否则下游
物化结果可能不正确。

下面的代码说明当前固定 `changelog_mode` API 的使用语义。

##### Case A：消费 retract 输入，产出 append-only 审计流

上游聚合表更新 `Alice` 的分数时，PTF 通过 `SUPPORT_UPDATES` 声明可以消费更新；额外声明
`REQUIRE_UPDATE_BEFORE` 后，更新必须以旧值 `-U` 和新值 `+U` 两次调用 `eval()`：

```python
from pyflink.common import Row
from pyflink.table import ChangelogMode
from pyflink.table.udf import (
    ProcessTableFunctionArgumentTrait as Trait,
    table_arg,
    udptf,
)


@udptf(
    arguments={
        "event": table_arg(
            traits={
                Trait.SET_SEMANTIC_TABLE,
                Trait.SUPPORT_UPDATES,
                Trait.REQUIRE_UPDATE_BEFORE,
            },
        )
    },
    result_type="ROW<input_kind STRING, score BIGINT>",
    changelog_mode=ChangelogMode.insert_only(),
)
def audit_updates(ctx, event):
    # 输出 Row 没有显式设置 RowKind，因此每条审计记录都是 INSERT。
    yield Row(
        input_kind=event.get_row_kind().name,
        score=event.score,
    )
```

假设 `event` 已按 `user_id` 分区，输入与输出的对应关系为：

```text
输入到 eval()                              PTF 输出
+I[user_id=Alice, score=10]        ->      +I[Alice, INSERT, 10]
-U[user_id=Alice, score=10]        ->      +I[Alice, UPDATE_BEFORE, 10]
+U[user_id=Alice, score=20]        ->      +I[Alice, UPDATE_AFTER, 20]
```

这里输出中的第一个 `Alice` 是 planner 自动添加的分区列。这个 Case 把输入的变更类型编码
成普通字段，因此输出仍是 append-only；它适合 CDC 审计、变更过滤和事件化处理。若上游
是 upsert 流，planner 负责在进入 Python worker 前补齐 `UPDATE_BEFORE` 所需的旧值。

##### Case B：消费 append-only 输入，产出 retract 聚合结果

下面的 PTF 按用户维护累计值。第一条结果输出 `+I`，累计值变化时输出 `-U/+U`，收到
`RESET` 时输出 `-D`：

```python
from pyflink.common import Row, RowKind
from pyflink.table import ChangelogMode
from pyflink.table.udf import value_state


@udptf(
    arguments={
        "event": table_arg(traits={Trait.SET_SEMANTIC_TABLE}),
    },
    states={
        "memory": value_state("ROW<sum BIGINT>"),
    },
    result_type="ROW<sum BIGINT>",
    changelog_mode=ChangelogMode.all(),
)
def retract_sum(ctx, memory, event):
    old_sum = memory.sum

    if event.action == "RESET":
        if old_sum is not None:
            yield Row.of_kind(RowKind.DELETE, sum=old_sum)
        ctx.clear_state("memory")
        return

    new_sum = (old_sum or 0) + event.score
    if old_sum is None:
        yield Row.of_kind(RowKind.INSERT, sum=new_sum)
    else:
        yield Row.of_kind(RowKind.UPDATE_BEFORE, sum=old_sum)
        yield Row.of_kind(RowKind.UPDATE_AFTER, sum=new_sum)
    memory["sum"] = new_sum
```

调用和结果示意如下：

```python
from pyflink.table.expressions import col

t_env.create_temporary_system_function("retract_sum", retract_sum)

result = events.partition_by(col("user_id")).process("retract_sum")
```

```text
输入                                      PTF 输出
+I[Alice, ADD, 10]                ->      +I[Alice, 10]
+I[Alice, ADD, 5]                 ->      -U[Alice, 10]
                                            +U[Alice, 15]
+I[Alice, RESET, NULL]            ->      -D[Alice, 15]
```

`ChangelogMode.all()` 告诉 planner 函数可能产出完整 retract changelog。Python operator
会校验函数实际产出的 `RowKind` 是否属于声明 mode。upsert 输出没有 `UPDATE_BEFORE`，
因此要求 Set semantics，且 upsert key 必须等于 `PARTITION BY` key；同一分区键只能维护
一个当前逻辑结果。retract 输出通过 `UPDATE_BEFORE` 描述撤回，不要求 upsert key，也可
用于 Row semantics。更新输出不能使用 `on_time`；如果函数还声明消费更新输入，
`SUPPORT_UPDATES` 不能与 pass-through columns 组合。

#### 完成阶段一后与 Java PTF 的剩余差异

完成 1A、1B、1C 和 Exit Gate 后，Python PTF 在单表场景中应已覆盖 Java PTF 的核心 table
semantics、`ORDER BY`、changelog、状态恢复和 Timer 语义。仍然存在以下差异：

| 能力 | Java PTF | 完成阶段一后的 Python PTF | 后续安排 |
|------|----------|---------------------------|----------|
| 表输入数量 | 支持零表、单表和多表，默认最多 20 个表参数 | 仍要求恰好一个表参数 | 多表放在阶段二，零表放在阶段三 |
| 零表/纯标量调用 | 可以通过 `fromCall()` 调用只含标量参数的 PTF | 不支持，因为当前 runtime 必须由表记录触发 `eval()` | 阶段三补充触发、并行度和 end-of-input 语义 |
| 多表协调 | 支持多路输入、兼容分区键、每输入 watermark/排序缓冲和 Timer 协调 | 不支持 | 阶段二实现 |
| 状态模型 | 支持 POJO/`Row` Value State、`ListView` 和 `MapView` | 支持 `ROW` Value State、`ListView` 和 `MapView`，不支持任意 Python structured state object | Python structured state object 放在阶段三评估 |
| 顶层结果类型 | 支持标量、`ROW` 和其他结构化结果 | 仍要求顶层 `ROW`，但字段可以使用现有 coder 支持的嵌套类型 | 阶段三支持标量、`ARRAY`、`MAP`、`RAW` 和动态结果类型 |
| Changelog 推导 | `getChangelogMode(context)` 可以根据输入和下游要求动态选择 mode | 1B 支持固定 append/upsert/retract 声明 | 动态 planning context 放在阶段三评估 |
| Table semantics context | Java context 暴露完整表语义 | 1B 暴露单表核心 metadata | 剩余高级 context API 放在阶段三补齐 |
| Inline 调用 | Java Table API 可以直接传入 PTF class，不要求先注册名称 | Python PTF 仍必须先注册名称 | 阶段三评估 inline Python PTF |
| 可选参数 | Java 静态签名支持 optional scalar/table arguments | `@udptf` 中声明的业务参数全部必填 | 阶段三补齐 |
| 高级类型推导 | 支持反射、`@DataTypeHint`、POJO/STRUCTURED/RAW 和覆盖 `getTypeInference()` | 使用显式 Python `DataType`，不承诺自定义 structured object、`RAW` 或动态输出 schema | 阶段三设计 Python API 和 coder |
| Python 执行模式 | 不适用；Java PTF 直接在 JVM operator 中运行 | 仍只支持 Process Python worker | 阶段三评估 Embedded Python mode |
| 恢复验证矩阵 | Java PTF 已覆盖更广泛的 checkpoint/savepoint 和运行时组合 | Phase 1 只覆盖固定并行度、HashMap backend 的 checkpoint/failover/TTL 门禁 | Post-Phase-1 Hardening 补 savepoint、rescale、backend、压力和性能矩阵 |
| 专用测试工具 | Java 提供 `ProcessTableFunctionTestHarness` | 主要依赖 Python 单元测试、planner/runtime 测试和端到端作业 | 可后续增加 Python PTF test harness |

以下项目在完成阶段一后不应再视为差异：单表 Row/Set semantics、表参数 Trait、
pass-through 约束、核心 `table_semantics_for()`、`ORDER BY`、append/upsert/retract 输入输出、
0-N 条 `ROW` 输出、`ROW` Value State、`ListView`、`MapView`、事件时间 Timer、watermark、TTL、checkpoint restore、
注册名称的 Table API/SQL 调用，以及 Java PTF 互操作。

### Post-Phase-1 Hardening：生产验证

以下工作不作为 Phase 1 功能完成的退出条件：

- savepoint、跨版本恢复和 schema evolution。
- rescale 或修改并行度后的 keyed state、排序缓冲和 Timer 恢复。
- HashMap、RocksDB 和 ForSt backend 矩阵。
- 大 bundle、热点 key、背压、压力和长期运行。
- 状态、排序和 Timer 密集场景的吞吐、延迟基线与性能门禁。
- 函数重命名、稳定 UID 和函数升级兼容性的完整矩阵。

### 第二阶段：扩展输入模型

#### 2A Multi-input Core

- 支持多个 append-only Python 表输入，包括表参数投影、输入标识和单次 `eval()` 的
  `None` 占位语义。
- 对齐 Java 多表约束：所有表参数使用 Set semantics，分区键类型兼容，并遵守默认
  20 个表参数的上限。
- 首个可运行版本支持 Value State，禁止 Timer、`ORDER BY` 和更新流。
- 输出按表参数声明顺序附加每个表参数的分区列；即使多个输入的 key 值和类型相同，
  schema 中也保留多组列。

#### 2B Time Coordination

- 支持每输入 watermark、idle/active 状态和输入结束。
- 从所有 active 输入计算全局 watermark，并据此协调事件时间 Timer 和 checkpoint。
- 明确跨输入到达顺序：运行时不承诺任意输入之间的先后关系；需要确定性的函数必须
  使用 watermark、Timer 或业务条件等待所需输入。

#### 2C Per-input ORDER BY

- 为每个表参数维护独立排序缓冲和迟到数据判定。
- 排序状态、每输入 watermark 进度和内部 Timer 参与 checkpoint/savepoint restore。

#### 2D Multi-changelog

- 协调多个更新输入的 changelog mode，按需 normalize 或 materialize update-before/full
  delete，并推导 Python PTF 输出与下游需求。
- 明确多输入输出前缀的列数量、顺序、重名规则、upsert key metadata 和 Python collector
  编码，覆盖更新流与 `ORDER BY`、Timer、checkpoint 的组合。

### 第三阶段：低优先级完整对齐

- 支持零表输入和只包含标量参数的 Python PTF，定义明确的触发次数、并行度和
  end-of-input 行为。
- 评估 inline Python PTF。
- 评估 Embedded Python mode。
- 支持顶层标量、`ARRAY`、`MAP`、`RAW` 和基于输入 schema 动态生成的结果类型。
- 支持 optional scalar/table arguments。
- 评估 Python structured state object。
- 评估动态 `get_changelog_mode(context)` 和 planning-time specialization。
- 补齐其余 Java PTF context API 和调用便利性。
- 仅在 Java PTF 先支持 Batch 后评估 Python Batch PTF；否则这属于超出 Java 对齐范围的
  独立扩展。

#### 阶段三：更丰富结果类型的详细范围

阶段一要求顶层 `result_type` 必须是 `ROW`。这不限制 `ROW` 字段使用 `ARRAY`、
`MAP` 或其他已有 Python coder 支持的嵌套类型；阶段三的“更丰富结果类型”主要指
对齐 Java PTF 的顶层非 `ROW` 结果和隐式单列包装。

| 结果形式 | Python 产出值 | 计划行为 |
|----------|----------------|----------|
| 显式多列 `ROW` | `Row(...)` 或 tuple | 保持阶段一行为，每个字段成为 PTF 结果列 |
| 顶层标量 | `int`、`str`、`Decimal`、日期时间等 | 接受标量 `result_type`，由 planner 隐式包装为单列 `ROW` |
| 顶层 `ARRAY` | list | 整个 list 作为单列值，而不是展开为多条 PTF 输出 |
| 顶层 `MAP` | dict | 整个 dict 作为单列值 |
| `RAW`/动态结果 | Python object 或基于输入生成的值 | 需要单独定义类型推导、coder 和兼容性规则 |
| 带稳定列名的单列 | 单字段 `Row` | 用显式 `ROW<name TYPE>` 定义列名，避免依赖系统生成的隐式列名 |

顶层标量的目标定义方式为：

```python
@udptf(
    arguments={
        "event": table_arg(traits={Trait.ROW_SEMANTIC_TABLE}),
    },
    result_type=DataTypes.BIGINT(),
)
def double_score(ctx, event):
    yield event.score * 2
```

输出归一化需要固定以下语义：

- `yield value` 表示产出一条结果；对顶层 `ARRAY` 和 `MAP`，list 或 dict 是这条结果的
  单列值。
- `yield None` 在结果类型可空时表示一条值为 `NULL` 的结果；没有执行任何
  `yield`、不带值的 `return` 或空 iterator 表示输出 0 条。
- `yield from values` 仍表示 N 条输出，不会因为顶层类型是集合而改变 generator 语义。
- 隐式包装后，分区键或 pass-through 列仍位于函数结果之前，rowtime 仍位于结果之后。
- 主回调和 Timer callback 使用同一套结果序列化和 0-N 条输出规则。

阶段三需要覆盖标量、`ARRAY`、`MAP`、显式 `ROW`、`NULL`、0-N 条输出、Timer 回调、
append/upsert/retract 输出，以及与分区键、pass-through 列和 rowtime 的合法组合。

Batch PTF、processing-time Timer 和 broadcast state 当前不是阶段一遗留的 Java 对齐项，
因为 Java PTF 本身也不支持这些能力。若 Python 后续单独支持，应视为 PTF 总体能力扩展。

### 社区提交与 PR 拆分

三个阶段可以属于同一个 FLIP/JIRA umbrella，但不应合并成一个巨大 PR。建议至少按
以下边界拆分：

1. Python public API、Java placeholder 和基础 Table API 调用。
2. 核心 append-only runtime、Beam runner、Value State 和 Timer。
3. `ORDER BY` 与 Java 排序缓冲接入。
4. 核心 `TableSemantics` context 与 proto。
5. Changelog input/output，包括更新 Trait、固定 mode、`RowKind` 和 planner 协商。
6. `ListView`、`MapView` 与 state-backed DataView 接入。
7. SQL smoke、最小 checkpoint/failover/TTL 恢复门禁和 roadmap。

savepoint、rescale、多 backend、压力和性能测试属于 Post-Phase-1 Hardening，应继续按测试
主题拆分，不与 Exit Gate 合并。

阶段用于表达用户能力和交付顺序，PR 用于控制 API、Planner、Runtime 和 worker 变更的
审查风险。结论是：阶段可以合并，PR 不应合并。

## 阶段一验收标准

### 1A Core

- 无状态 Row-semantics Python PTF 可以定义、注册并执行。
- Set-semantics Python PTF 的 Value State 按分区键隔离。
- State TTL、显式清理和异常不写回语义正确。
- Named 和 anonymous 事件时间 Timer 可以注册、替换、删除、触发和恢复。
- PyFlink 可以调用 Java 单输入和多输入 PTF。
- JDK 17 构建、Python worker 测试、Java runner/timer 测试和 Table API
  端到端测试通过。

### 1B Parity

- 已注册 Python PTF 可以通过 Table API 和 SQL 调用。
- `ORDER BY` 保证分区内顺序，晚到数据、watermark 和恢复语义与 Java PTF 一致。
- 排序缓冲、watermark 进度和内部 Timer 参与 checkpoint，基础 snapshot restore 通过。
- `table_semantics_for()` 返回单表核心类型、分区、排序、时间和 changelog metadata。
- 更新输入保留 `+I/-U/+U/-D`，并正确执行 update-before 和 full-delete 要求。
- Python PTF 可以声明并产出固定 append、upsert 或 retract changelog。
- operator 拒绝声明 mode 之外的输出，并覆盖 Java planner 的主要非法组合。

### 1C State Views

- `ListView` 支持 `get/add/add_all/remove/clear`。
- `MapView` 支持 `get/put/remove/contains`、迭代和 clear。
- View 直接访问 keyed managed state，按 key 隔离并支持独立 TTL。
- Value State、`ListView` 和 `MapView` 可以混用，并受 context 清理 API 统一控制。

### Phase 1 Exit Gate

- 使用真实 Process Python worker 完成 checkpoint 后触发一次 TaskManager failover。
- 恢复后 keyed Value State 计数连续，named Timer 恢复且只触发一次。
- 短 TTL 的 Value State、`ListView` 和 `MapView` 过期后不可见。
- 使用固定并行度和 HashMap backend；Post-Phase-1 Hardening 项不阻塞 Phase 1 退出。
