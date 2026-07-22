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

PyFlink PTF 首版以打通单表输入的 Streaming PTF 完整链路为目标，而不是一次性实现与
Java PTF 的全部功能对齐。

用户可以继承 `ProcessTableFunction`，通过 `udptf()` 声明有序参数、状态和结果类型，
注册函数名称后使用 `Table.process()` 或 `TableEnvironment.from_call()` 调用。

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

本文所描述的 Python PTF 首版已经在当前工作区完成原型实现，包括 Python API、Java
占位函数、共享 PTF planner 的 Python 分支、单输入 operator、PTF Beam runner、Python
worker、Value State、事件时间 Timer bridge 和相应测试。该实现复用社区 Java PTF
语义以及现有 Flink/Python state、checkpoint、watermark 和 Beam 基础设施。

但是，这些文件目前是本地未提交修改，尚未通过 Apache Flink 社区的 FLIP、代码审查和
发布流程。因此本文中的“已实现”仅表示当前工作区状态，不表示 Apache Flink 稳定版或
社区 `master` 已支持 Python-defined PTF。新增 PyFlink 公共 API 在合入前仍需要获得
FLIP 批准，并补充 JIRA、release note、兼容性说明和社区级 CI 验证。

## 首版支持范围

| 领域 | 首版支持范围 | 对应 Case |
|------|--------------|-----------|
| 定义方式 | 继承 `ProcessTableFunction` 并通过 `udptf()` 创建 Python PTF | Case 1 |
| 调用方式 | 注册名称后使用 `Table.process()` 或 `TableEnvironment.from_call()` | Case 2 |
| 参数 | 恰好一个表参数，以及零个或多个按声明顺序注入的标量参数 | Case 3 |
| 表语义 | 无状态处理支持 Row semantics；状态和 Timer 支持 Set semantics 以及必选或可选分区 | Case 4 |
| 表参数 Trait | 支持 pass-through columns 和 required on-time，但 Timer 不能与 pass-through columns 同时使用 | Case 5 |
| 状态 | 一个或多个 keyed `ROW` Value State，支持可变 `Row` 注入、显式清理和独立 TTL | Case 6 |
| 时间 | 使用 epoch 毫秒、`Instant` 或 UTC-naive `datetime` 访问事件时间、表 watermark 和当前 PTF watermark | Case 7 |
| Timer | 支持 named 和 anonymous 事件时间 Timer，以及替换、单个删除、全部清理和回调 | Case 8 |
| 输出 | 支持 0-N 条 `ROW` 结果，并组合分区键、pass-through 列和可选 rowtime | Case 9 |
| Runtime | Streaming、Process Python worker、append-only 输入和输出 | Case 10 |
| 可靠性 | 状态和 Timer 使用 Flink managed state；watermark、checkpoint 和 end-of-input 前刷新 bundle、状态请求和 Timer 命令 | Case 11 |
| Java 互操作 | PyFlink 可以调用已注册的 Java PTF，包括多表输入 Java PTF | Case 12 |

## 首版支持范围 Case

下面的 case 与上表一一对应。示例假设 `events` 是 append-only 流表，包含
`user_id STRING`、`text STRING` 和已定义 watermark 的 `ts TIMESTAMP_LTZ(3)`。

### Case 1：在 Python 中定义 PTF

```python
from pyflink.common import Row
from pyflink.table import DataTypes
from pyflink.table.udf import (
    ProcessTableFunction,
    ProcessTableFunctionArgument,
    ProcessTableFunctionArgumentTrait as Trait,
    udptf,
)


class Tokenize(ProcessTableFunction):
    def eval(self, ctx, event, separator):
        for token in event.text.split(separator):
            if token:
                yield Row(token=token)


tokenize = udptf(
    Tokenize(),
    arguments=[
        ProcessTableFunctionArgument.table(
            "event", traits={Trait.ROW_SEMANTIC_TABLE}
        ),
        ProcessTableFunctionArgument.scalar("separator", DataTypes.STRING()),
    ],
    result_type=DataTypes.ROW([
        DataTypes.FIELD("token", DataTypes.STRING()),
    ]),
)
t_env.create_temporary_system_function("tokenize", tokenize)
```

**预期：** `udptf()` 保留参数的声明顺序，并校验 `eval()` 必须是
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
class Wrap(ProcessTableFunction):
    def eval(self, ctx, event, prefix, suffix):
        yield Row(value=prefix + event.text + suffix)


wrap = udptf(
    Wrap(),
    arguments=[
        ProcessTableFunctionArgument.table("event"),
        ProcessTableFunctionArgument.scalar("prefix", DataTypes.STRING()),
        ProcessTableFunctionArgument.scalar("suffix", DataTypes.STRING()),
    ],
    result_type=DataTypes.ROW([
        DataTypes.FIELD("value", DataTypes.STRING()),
    ]),
)
```

**预期：** 每次回调按 `event`、`prefix`、`suffix` 的声明顺序注入参数。首版
必须恰好声明一个表参数，可以不声明标量参数，也可以声明多个标量参数。

### Case 4：Row semantics 和 Set semantics

```python
# 无状态：每行独立调用。
row_argument = ProcessTableFunctionArgument.table(
    "event", traits={Trait.ROW_SEMANTIC_TABLE}
)

# 有状态：相同 user_id 共享一份 keyed state。
set_argument = ProcessTableFunctionArgument.table(
    "event", traits={Trait.SET_SEMANTIC_TABLE}
)
result = events.partition_by(col("user_id")).process("count_by_key")
```

**预期：** 输入 `(A, x)`、`(B, y)`、`(A, z)` 时，`count_by_key` 输出 `(A, 1)`、
`(B, 1)`、`(A, 2)`。声明 `OPTIONAL_PARTITION_BY` 后也可不调用
`partition_by()`，此时所有输入使用同一份全局逻辑分区状态。

### Case 5：表参数 Trait

```python
pass_through = ProcessTableFunctionArgument.table(
    "event",
    traits={Trait.ROW_SEMANTIC_TABLE, Trait.PASS_COLUMNS_THROUGH},
)

required_time = ProcessTableFunctionArgument.table(
    "event",
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
from pyflink.table.udf import ProcessTableFunctionState


class CountAndRemember(ProcessTableFunction):
    def eval(self, ctx, count_state, last_state, event):
        previous = last_state.text
        count_state["value"] = (count_state.value or 0) + 1
        last_state["text"] = event.text
        yield Row(count=count_state.value, previous=previous)

        if event.text == "reset":
            ctx.clear_state("count_state")


states = [
    ProcessTableFunctionState.value(
        "count_state",
        DataTypes.ROW([DataTypes.FIELD("value", DataTypes.BIGINT())]),
        ttl=Duration.of_days(1),
    ),
    ProcessTableFunctionState.value(
        "last_state",
        DataTypes.ROW([DataTypes.FIELD("text", DataTypes.STRING())]),
    ),
]

count_and_remember = udptf(
    CountAndRemember(),
    arguments=[
        ProcessTableFunctionArgument.table(
            "event", traits={Trait.SET_SEMANTIC_TABLE}
        ),
    ],
    states=states,
    result_type=DataTypes.ROW([
        DataTypes.FIELD("count", DataTypes.BIGINT()),
        DataTypes.FIELD("previous", DataTypes.STRING()),
    ]),
)
```

**预期：** 状态按声明顺序、作为可变 `Row` 注入，并在 generator 消费完成后自动
写回当前分区。初始值是所有字段为 `None` 的 `Row`；全空 `Row` 自动删除。
`clear_state()` 优先于对象修改；回调异常时不写回；每个状态可以配置独立 TTL。

### Case 7：三种时间表示和 watermark

```python
from datetime import datetime
from pyflink.common import Instant


def eval(self, ctx, event):
    epoch_millis = ctx.time_context(int).time()
    instant = ctx.time_context(Instant).time()
    utc_datetime = ctx.time_context(datetime).time()

    table_watermark = ctx.time_context(int).table_watermark()
    ptf_watermark = ctx.time_context(int).current_watermark()
```

**预期：** 同一个时间值可以表示为 epoch 毫秒、`Instant` 或 UTC-naive
`datetime`。没有可用时间或 watermark 时返回 `None`；timezone-aware `datetime`
会被拒绝。只有传入 `on_time` 时，`time()` 才能返回当前行时间，并向输出传播
rowtime。`table_watermark()` 返回当前输入表的 watermark，`current_watermark()`
返回当前 PTF 的 watermark；两者在启动或恢复期间都可能为 `None`。

### Case 8：named 和 anonymous 事件时间 Timer

```python
class Timeout(ProcessTableFunction):
    def eval(self, ctx, memory, event):
        timers = ctx.time_context(int)
        event_time = timers.time()
        if event_time is not None:
            timers.register_on_time("timeout", event_time + 10_000)
            # 同名 Timer 被替换为更晚的时间。
            timers.register_on_time("timeout", event_time + 20_000)
            timers.register_on_time(event_time + 30_000)  # anonymous Timer

        if event.text == "cancel":
            timers.clear_timer("timeout")

    def on_timer(self, ctx, memory):
        yield Row(timer_name=ctx.current_timer())
        ctx.clear_all()
```

**预期：** 同一 key 下同名 Timer 只保留最后一次注册；anonymous Timer 按时间戳独立注册。
watermark 越过时间戳后调用 `on_timer()`，named Timer 可通过 `current_timer()`
获取 `"timeout"`。支持按名称或时间删除、`clear_all_timers()` 和同时清理状态与
Timer 的 `clear_all()`。

### Case 9：单次调用输出 0-N 条结果

```python
class PositiveTokens(ProcessTableFunction):
    def eval(self, ctx, event):
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

**预期：** Python PTF 通过独立 Process Python worker 执行。Batch mode、Embedded/Thread mode、
非 append-only 输入或非 `ROW` 结果会在 API 或 planner 阶段被拒绝。

### Case 11：checkpoint、watermark 和恢复

假设 key `A` 的状态计数为 3，并已注册事件时间 Timer `timeout@20s`：

1. checkpoint barrier 到达时，operator 先刷新 Python bundle、远程状态请求和 Timer 命令。
2. checkpoint 将 keyed Value State、named Timer 映射和 Flink internal Timer 一起持久化。
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

## API 约定

状态和 Timer 使用固定的回调签名，状态参数位于函数参数之前：

```python
def eval(self, ctx, state1, state2, table_arg, scalar_arg):
    ...

def on_timer(self, ctx, state1, state2):
    ...
```

状态和 Timer 要求 Set semantics，Row-semantics PTF 仅支持无状态处理。除非表参数声明了
`REQUIRE_ON_TIME`，否则 `on_time` 是可选参数。传入 `on_time` 后，PTF 可以访问当前行时间，
并向输出传播 rowtime；无论是否传入 `on_time`，事件时间 Timer 都由当前 PTF watermark 驱动。

## 当前限制

- Python PTF 必须声明恰好一个表参数，不支持零表输入或多表输入。该限制不影响 PyFlink
  调用 Java PTF。
- 仅支持 Streaming 和 Process Python worker，不支持 Batch 和 Embedded Python mode。
- 输入和输出必须为 append-only，`result_type` 必须是 `ROW` 类型。
- 不支持 `ORDER BY`、更新表参数、update-before、retraction 和 full-delete 语义。
- 状态仅支持 `ROW` Value State，不支持 List State 和 Map State。
- 状态和 Timer 要求 Set semantics，Timer 不能与 pass-through columns 同时使用。
- Timer 仅支持事件时间，并且注册 Timer 的函数必须实现 `on_timer()`。
- Python PTF 必须先注册名称，不支持 inline Python 函数实例或函数类。
- 首版以 Python Table API 为正式支持的调用入口，不包含更广泛的 SQL 调用覆盖。

## 后续规划

以下内容表示后续实现方向，不构成兼容性或版本承诺。

### 第一阶段：完整的单表 Python PTF

第一阶段合并原“首版实现”“生产能力加固”和“单表 Java PTF 能力对齐”，目标是让
Python PTF 覆盖完整的单表 Streaming 主路径：

- Python PTF 定义、注册，以及通过 Table API 或 SQL 调用已注册函数。
- 恰好一个表输入和零个或多个标量参数。
- Row/Set semantics、`PARTITION BY`、pass-through columns 和 `on_time`。
- `ROW` Value State、独立 TTL、事件时间 Timer、显式清理和 checkpoint 恢复。
- append、upsert 和 retract 输入输出，以及 `SUPPORT_UPDATES`、
  `REQUIRE_UPDATE_BEFORE` 和 `REQUIRE_FULL_DELETE`。
- `ORDER BY`、watermark 驱动的排序缓冲和排序状态恢复。
- 核心 `ctx.table_semantics_for()`，覆盖输入类型、分区、排序、时间列和 changelog
  metadata。
- 大 bundle、watermark、背压、checkpoint、故障恢复和性能测试。

阶段一内部按以下里程碑推进，但这些里程碑属于同一个单表能力阶段：

#### 1A Core：append-only MVP，已完成

- 提供 `ProcessTableFunction`、`udptf()`、参数、状态和 Trait API。
- 支持注册名称后的 `Table.process()`、`PartitionedTable.process()` 和
  `TableEnvironment.from_call()`。
- 支持单表 append-only 输入输出、Row/Set semantics、分区、pass-through 和 on-time。
- 支持 `ROW` Value State、TTL、named/anonymous 事件时间 Timer，以及 Process Python
  worker 执行链路。

#### 1B Hardening：生产能力加固

- 增加 Python Value State 和 Timer 故障恢复的端到端测试。
- 增加 TTL 过期和 state backend 兼容性测试。
- 覆盖大 bundle、背压、watermark 和 checkpoint 并发场景。
- 建立状态访问和 Timer 密集场景的性能基线。

#### 1C Parity：单表高级能力

- 支持核心 table semantics 和 `ORDER BY`。
- 支持更新表输入，以及 update-before、retraction 和 delete 消息。
- 支持 Python PTF 以固定 `ChangelogMode` 声明并产出 append、upsert 或 retract
  changelog；基于 planning context 动态选择 mode 不作为 1C 的前置条件。
- 验证已注册 Python PTF 的 SQL 调用能力。

#### 1C：Table semantics 和 ORDER BY 的详细范围

这里的“核心”包括单表元数据、分区内有序处理和更新流元数据。1C 计划包含：

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
def eval(self, ctx, event):
    semantics = ctx.table_semantics_for("event")
    input_type = semantics.data_type()
    partition_columns = semantics.partition_by_columns()
    order_columns = semantics.order_by_columns()
    order_directions = semantics.order_by_directions()
    time_column = semantics.time_column()
```

`ORDER BY`、更新输入和更新输出属于 1C 中的独立能力，组合使用时继续遵守 Java
PTF 的校验规则。例如，更新表不能使用 pass-through columns，接收或产出更新的 PTF
不能使用 `on_time`。

#### 1C：更新输入和输出的详细范围

1C 允许 Python PTF 消费和产出 changelog，不再把 operator 固定为 append-only。
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

下面的代码用于说明 1C 的目标语义。示例中的 `changelog_mode` 是拟议 API，当前 1A
`udptf()` 尚不接受该参数。

##### Case A：消费 retract 输入，产出 append-only 审计流

上游聚合表更新 `Alice` 的分数时，PTF 通过 `SUPPORT_UPDATES` 声明可以消费更新；额外声明
`REQUIRE_UPDATE_BEFORE` 后，更新必须以旧值 `-U` 和新值 `+U` 两次调用 `eval()`：

```python
from pyflink.common import Row
from pyflink.table import ChangelogMode, DataTypes
from pyflink.table.udf import (
    ProcessTableFunction,
    ProcessTableFunctionArgument,
    ProcessTableFunctionArgumentTrait as Trait,
    udptf,
)


class AuditUpdates(ProcessTableFunction):
    def eval(self, ctx, event):
        # 输出 Row 没有显式设置 RowKind，因此每条审计记录都是 INSERT。
        yield Row(
            input_kind=event.get_row_kind().name,
            score=event.score,
        )


audit_updates = udptf(
    AuditUpdates(),
    arguments=[
        ProcessTableFunctionArgument.table(
            "event",
            traits={
                Trait.SET_SEMANTIC_TABLE,
                Trait.SUPPORT_UPDATES,
                Trait.REQUIRE_UPDATE_BEFORE,
            },
        )
    ],
    result_type=DataTypes.ROW([
        DataTypes.FIELD("input_kind", DataTypes.STRING()),
        DataTypes.FIELD("score", DataTypes.BIGINT()),
    ]),
    changelog_mode=ChangelogMode.insert_only(),
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
from pyflink.table import ChangelogMode, DataTypes
from pyflink.table.udf import ProcessTableFunctionState


class RetractSum(ProcessTableFunction):
    def eval(self, ctx, memory, event):
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


retract_sum = udptf(
    RetractSum(),
    arguments=[
        ProcessTableFunctionArgument.table(
            "event", traits={Trait.SET_SEMANTIC_TABLE}
        )
    ],
    states=[
        ProcessTableFunctionState.value(
            "memory",
            DataTypes.ROW([
                DataTypes.FIELD("sum", DataTypes.BIGINT())
            ]),
        )
    ],
    result_type=DataTypes.ROW([
        DataTypes.FIELD("sum", DataTypes.BIGINT())
    ]),
    changelog_mode=ChangelogMode.all(),
)
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

`ChangelogMode.all()` 告诉 planner 函数可能产出完整 retract changelog。函数实际产出的
`RowKind` 必须属于声明的 mode；更新输出要求 Set semantics，upsert/retract 结果的 key
必须与 `PARTITION BY` key 一致，并且不能使用 `on_time`。如果函数还声明消费更新输入，
`SUPPORT_UPDATES` 不能与 pass-through columns 组合。

#### 完成阶段一后与 Java PTF 的剩余差异

完成 1A、1B 和 1C 后，Python PTF 在单表场景中应已覆盖 Java PTF 的核心 table
semantics、`ORDER BY`、changelog、状态恢复和 Timer 语义。仍然存在以下差异：

| 能力 | Java PTF | 完成阶段一后的 Python PTF | 后续安排 |
|------|----------|---------------------------|----------|
| 表输入数量 | 支持零表、单表和多表，默认最多 20 个表参数 | 仍要求恰好一个表参数 | 多表放在阶段二，零表放在阶段三 |
| 零表/纯标量调用 | 可以通过 `fromCall()` 调用只含标量参数的 PTF | 不支持，因为当前 runtime 必须由表记录触发 `eval()` | 阶段三补充触发、并行度和 end-of-input 语义 |
| 多表协调 | 支持多路输入、兼容分区键、每输入 watermark/排序缓冲和 Timer 协调 | 不支持 | 阶段二实现 |
| 状态模型 | 支持 POJO/`Row` Value State、`ListView` 和 `MapView` | 只支持 `ROW` Value State | 阶段二实现 List/Map State；Python structured state object 放在阶段三评估 |
| 顶层结果类型 | 支持标量、`ROW` 和其他结构化结果 | 仍要求顶层 `ROW`，但字段可以使用现有 coder 支持的嵌套类型 | 阶段三支持标量、`ARRAY`、`MAP`、`RAW` 和动态结果类型 |
| Changelog 推导 | `getChangelogMode(context)` 可以根据输入和下游要求动态选择 mode | 1C 首先支持固定 append/upsert/retract 声明 | 动态 planning context 放在阶段三评估 |
| Table semantics context | Java context 暴露完整表语义 | 1C 暴露单表核心 metadata | 剩余高级 context API 放在阶段三补齐 |
| Inline 调用 | Java Table API 可以直接传入 PTF class，不要求先注册名称 | Python PTF 仍必须先注册名称 | 阶段三评估 inline Python PTF |
| 可选参数 | Java 静态签名支持 optional scalar/table arguments | `udptf()` 中声明的业务参数全部必填 | 阶段三补齐 |
| 高级类型推导 | 支持反射、`@DataTypeHint`、POJO/STRUCTURED/RAW 和覆盖 `getTypeInference()` | 使用显式 Python `DataType`，不承诺自定义 structured object、`RAW` 或动态输出 schema | 阶段三设计 Python API 和 coder |
| Python 执行模式 | 不适用；Java PTF 直接在 JVM operator 中运行 | 仍只支持 Process Python worker | 阶段三评估 Embedded Python mode |
| 查询演进验证 | Java PTF 支持通过稳定 UID 恢复兼容 schema 的状态 | 阶段一覆盖 checkpoint restore，但跨函数版本和 savepoint schema 演进仍需单独矩阵 | 持续生产化验证 |
| 专用测试工具 | Java 提供 `ProcessTableFunctionTestHarness` | 主要依赖 Python 单元测试、planner/runtime 测试和端到端作业 | 可后续增加 Python PTF test harness |

以下项目在完成阶段一后不应再视为差异：单表 Row/Set semantics、表参数 Trait、
pass-through 约束、核心 `table_semantics_for()`、`ORDER BY`、append/upsert/retract 输入输出、
0-N 条 `ROW` 输出、`ROW` Value State、事件时间 Timer、watermark、TTL、checkpoint restore、
注册名称的 Table API/SQL 调用，以及 Java PTF 互操作。

### 第二阶段：扩展输入和状态模型

- 支持 Python 多表输入，包括多表参数投影、输入标识和单次 `eval()` 的空行占位语义。
- 对齐 Java 多表约束：所有表参数使用 Set semantics，分区键类型兼容，并遵守默认
  20 个表参数的上限。
- 支持多表 watermark、每输入排序缓冲、Timer 和 checkpoint 协调。
- 支持多个更新流之间的 changelog 协商，并验证多表、更新输入、更新输出和
  `ORDER BY` 组合时的 planner 与恢复语义。
- 明确跨输入到达顺序：运行时不承诺任意输入之间的先后关系；需要确定性的函数必须
  使用 watermark、Timer 或业务条件等待所需输入。
- 支持 List State 和 Map State。
- List State 和 Map State 使用 Flink keyed managed state，而不是仅保存在 Python worker
  内存中；其修改必须参与 checkpoint，并在故障恢复后回到同一个 checkpoint 快照。
- TTL 与 Java PTF 对齐：List State 的元素和 Map State 的 entry 独立过期，过期数据对
  Python 回调不可见；TTL 仍基于处理时间，不触发 `on_timer()`。
- 增加 List/Map State 的按 key 隔离、增删迭代、TTL 过期、checkpoint restore 和
  state backend 兼容性测试。

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
class DoubleScore(ProcessTableFunction):
    def eval(self, ctx, event):
        yield event.score * 2


double_score = udptf(
    DoubleScore(),
    arguments=[
        ProcessTableFunctionArgument.table(
            "event", traits={Trait.ROW_SEMANTIC_TABLE}
        ),
    ],
    result_type=DataTypes.BIGINT(),
)
```

输出归一化需要固定以下语义：

- `yield value` 表示产出一条结果；对顶层 `ARRAY` 和 `MAP`，list 或 dict 是这条结果的
  单列值。
- `yield None` 在结果类型可空时表示一条值为 `NULL` 的结果；没有执行任何
  `yield`、不带值的 `return` 或空 iterator 表示输出 0 条。
- `yield from values` 仍表示 N 条输出，不会因为顶层类型是集合而改变 generator 语义。
- 隐式包装后，分区键或 pass-through 列仍位于函数结果之前，rowtime 仍位于结果之后。
- `eval()` 和 `on_timer()` 使用同一套结果序列化和 0-N 条输出规则。

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
4. Changelog input，包括更新 Trait 和输入 `RowKind`。
5. Changelog output，包括固定 mode 声明、输出 `RowKind` 和 planner 协商。
6. 核心 `TableSemantics` context 与 proto。
7. SQL、故障恢复、压力、性能和组合 E2E。

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

### 1B Hardening

- Value State 和 Timer 通过故障注入、checkpoint 和 savepoint restore 测试。
- TTL、state backend、大 bundle、背压和 watermark/checkpoint 并发场景通过测试。
- 建立可重复的状态访问和 Timer 性能基线。

### 1C Parity

- 已注册 Python PTF 可以通过 Table API 和 SQL 调用。
- `ORDER BY` 保证分区内顺序，晚到数据、watermark 和恢复语义与 Java PTF 一致。
- 排序缓冲通过 checkpoint/savepoint restore、背压和大分区测试，并建立性能基线。
- `table_semantics_for()` 返回单表核心类型、分区、排序、时间和 changelog metadata。
- 更新输入保留 `+I/-U/+U/-D`，并正确执行 update-before 和 full-delete 要求。
- Python PTF 可以声明并产出固定 append、upsert 或 retract changelog。
- 更新输入输出与状态、checkpoint、下游 retract/upsert sink 的组合测试通过。
