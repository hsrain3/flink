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
import abc
import enum
import functools
import inspect
from typing import Union, List, Type, Callable, TypeVar, Generic, Iterable, Optional, Sequence, Set

from pyflink.java_gateway import get_gateway
from pyflink.metrics import MetricGroup
from pyflink.table import Expression
from pyflink.table.changelog_mode import ChangelogMode
from pyflink.table.types import DataType, _to_java_data_type
from pyflink.util import java_utils
from pyflink.util.api_stability_decorators import PublicEvolving, Internal

__all__ = ['FunctionContext', 'AggregateFunction', 'ScalarFunction', 'TableFunction',
           'TableAggregateFunction', 'AsyncScalarFunction', 'ProcessTableFunction',
           'ProcessTableFunctionArgument', 'ProcessTableFunctionState',
           'ProcessTableFunctionArgumentTrait', 'ProcessTableFunctionTableSemantics',
           'ProcessTableFunctionSortDirection', 'udf', 'udtf', 'udptf', 'udaf', 'udtaf']


@PublicEvolving()
class FunctionContext(object):
    """
    Used to obtain global runtime information about the context in which the
    user-defined function is executed. The information includes the metric group,
    global job parameters, and runtime task information such as task name, parallelism, etc.
    """

    def __init__(self, base_metric_group, job_parameters,
                 task_name=None, task_name_with_subtasks=None,
                 number_of_parallel_subtasks=None, max_number_of_parallel_subtasks=None,
                 index_of_this_subtask=None, attempt_number=None):
        self._base_metric_group = base_metric_group
        self._job_parameters = job_parameters
        self._task_name = task_name
        self._task_name_with_subtasks = task_name_with_subtasks
        self._number_of_parallel_subtasks = number_of_parallel_subtasks
        self._max_number_of_parallel_subtasks = max_number_of_parallel_subtasks
        self._index_of_this_subtask = index_of_this_subtask
        self._attempt_number = attempt_number

    def get_metric_group(self) -> MetricGroup:
        """
        Returns the metric group for this parallel subtask.

        .. versionadded:: 1.11.0
        """
        if self._base_metric_group is None:
            raise RuntimeError("Metric has not been enabled. You can enable "
                               "metric with the 'python.metric.enabled' configuration.")
        return self._base_metric_group

    def get_job_parameter(self, key: str, default_value: str) -> str:
        """
        Gets the global job parameter value associated with the given key as a string.

        :param key: The key pointing to the associated value.
        :param default_value: The default value which is returned in case global job parameter is
                              null or there is no value associated with the given key.

        .. versionadded:: 1.17.0
        """
        return self._job_parameters[key] if key in self._job_parameters else default_value

    def get_task_name(self) -> str:
        """
        Returns the name of the task in which the UDF runs, as assigned during plan construction.

        .. versionadded:: 2.3.0
        """
        return self._task_name

    def get_task_name_with_subtasks(self) -> str:
        """
        Returns the name of the task, appended with the subtask indicator, such as "MyTask (3/6)",
        where 3 would be (:func:`get_index_of_this_subtask` + 1), and 6 would be
        :func:`get_number_of_parallel_subtasks`.

        .. versionadded:: 2.3.0
        """
        return self._task_name_with_subtasks

    def get_number_of_parallel_subtasks(self) -> int:
        """
        Gets the parallelism with which the parallel task runs.

        .. versionadded:: 2.3.0
        """
        return self._number_of_parallel_subtasks

    def get_max_number_of_parallel_subtasks(self) -> int:
        """
        Gets the number of max-parallelism with which the parallel task runs.

        .. versionadded:: 2.3.0
        """
        return self._max_number_of_parallel_subtasks

    def get_index_of_this_subtask(self) -> int:
        """
        Gets the number of this parallel subtask. The numbering starts from 0 and goes up to
        parallelism-1 (parallelism as returned by :func:`get_number_of_parallel_subtasks`).

        .. versionadded:: 2.3.0
        """
        return self._index_of_this_subtask

    def get_attempt_number(self) -> int:
        """
        Gets the attempt number of this parallel subtask. First attempt is numbered 0.

        .. versionadded:: 2.3.0
        """
        return self._attempt_number


@PublicEvolving()
class UserDefinedFunction(abc.ABC):
    """
    Base interface for user-defined function.

    .. versionadded:: 1.10.0
    """

    def open(self, function_context: FunctionContext):
        """
        Initialization method for the function. It is called before the actual working methods
        and thus suitable for one time setup work.

        :param function_context: the context of the function
        :type function_context: FunctionContext
        """
        pass

    def close(self):
        """
        Tear-down method for the user code. It is called after the last call to the main
        working methods.
        """
        pass

    def is_deterministic(self) -> bool:
        """
        Returns information about the determinism of the function's results.
        It returns true if and only if a call to this function is guaranteed to
        always return the same result given the same parameters. true is assumed by default.
        If the function is not pure functional like random(), date(), now(),
        this method must return false.

        :return: the determinism of the function's results.
        """
        return True


@PublicEvolving()
class ScalarFunction(UserDefinedFunction):
    """
    Base interface for user-defined scalar function. A user-defined scalar functions maps zero, one,
    or multiple scalar values to a new scalar value.

    .. versionadded:: 1.10.0
    """

    @abc.abstractmethod
    def eval(self, *args):
        """
        Method which defines the logic of the scalar function.
        """
        pass


@PublicEvolving()
class AsyncScalarFunction(UserDefinedFunction):
    """
    Base interface for user-defined async scalar function. A user-defined async scalar function
    maps zero, one, or multiple scalar values to a new scalar value asynchronously.

    This function is similar to ScalarFunction but is executed asynchronously. It's useful when
    interacting with external systems (e.g., databases, REST APIs) where I/O operations would
    otherwise block.

    The eval method should be an async coroutine function that returns the result asynchronously.

    Example:
        ::

            >>> class AsyncLookupFunction(AsyncScalarFunction):
            ...     async def eval(self, key):
            ...         # Simulate async I/O operation
            ...         await asyncio.sleep(0.1)
            ...         return f"value_for_{key}"

    .. versionadded:: 2.3.0
    """

    @abc.abstractmethod
    async def eval(self, *args):
        """
        Async method which defines the logic of the async scalar function.
        This method should be an async coroutine.
        """
        pass


@PublicEvolving()
class TableFunction(UserDefinedFunction):
    """
    Base interface for user-defined table function. A user-defined table function creates zero, one,
    or multiple rows to a new row value.

    .. versionadded:: 1.11.0
    """

    @abc.abstractmethod
    def eval(self, *args):
        """
        Method which defines the logic of the table function.
        """
        pass


@PublicEvolving()
class ProcessTableFunction(UserDefinedFunction):
    """
    Base interface for a Python user-defined process table function.

    A process table function consumes a table argument and can emit zero, one, or multiple rows.
    Stateful functions receive their declared state entries as mutable
    :class:`~pyflink.common.Row` objects before the call arguments.

    .. versionadded:: 2.4.0
    """

    @abc.abstractmethod
    def eval(self, ctx, *args):
        """Processes an input row and yields zero, one, or multiple result rows."""
        pass

    def on_timer(self, ctx, *states):
        """Processes a firing event-time timer and yields result rows."""
        return ()


@PublicEvolving()
class ProcessTableFunctionArgumentTrait(enum.Enum):
    """Traits that describe the semantics of a process table function table argument.

    .. versionadded:: 2.4.0
    """

    ROW_SEMANTIC_TABLE = 'ROW_SEMANTIC_TABLE'
    SET_SEMANTIC_TABLE = 'SET_SEMANTIC_TABLE'
    PASS_COLUMNS_THROUGH = 'PASS_COLUMNS_THROUGH'
    SUPPORT_UPDATES = 'SUPPORT_UPDATES'
    REQUIRE_ON_TIME = 'REQUIRE_ON_TIME'
    OPTIONAL_PARTITION_BY = 'OPTIONAL_PARTITION_BY'
    REQUIRE_UPDATE_BEFORE = 'REQUIRE_UPDATE_BEFORE'
    REQUIRE_FULL_DELETE = 'REQUIRE_FULL_DELETE'


@PublicEvolving()
class ProcessTableFunctionSortDirection(enum.Enum):
    """Sort direction for an ``ORDER BY`` column of a PTF table argument."""

    ASC_NULLS_FIRST = 'ASC_NULLS_FIRST'
    ASC_NULLS_LAST = 'ASC_NULLS_LAST'
    DESC_NULLS_FIRST = 'DESC_NULLS_FIRST'
    DESC_NULLS_LAST = 'DESC_NULLS_LAST'

    def is_descending(self) -> bool:
        """Returns whether the direction is descending."""
        return self in (self.DESC_NULLS_FIRST, self.DESC_NULLS_LAST)

    def is_nulls_first(self) -> bool:
        """Returns whether null values are sorted first."""
        return self in (self.ASC_NULLS_FIRST, self.DESC_NULLS_FIRST)

    def is_nulls_last(self) -> bool:
        """Returns whether null values are sorted last."""
        return not self.is_nulls_first()


@PublicEvolving()
class ProcessTableFunctionTableSemantics(object):
    """Runtime metadata for a table argument of a process table function."""

    def __init__(self, data_type, partition_by_columns, order_by_columns,
                 order_by_directions, time_column, changelog_mode, upsert_key_columns):
        self._data_type = data_type
        self._partition_by_columns = tuple(partition_by_columns)
        self._order_by_columns = tuple(order_by_columns)
        self._order_by_directions = tuple(order_by_directions)
        self._time_column = time_column
        self._changelog_mode = changelog_mode
        self._upsert_key_columns = tuple(tuple(key) for key in upsert_key_columns)

    def data_type(self):
        """Returns the actual data type of the table argument."""
        return self._data_type

    def partition_by_columns(self):
        """Returns the 0-based ``PARTITION BY`` column indexes."""
        return self._partition_by_columns

    def order_by_columns(self):
        """Returns the 0-based ``ORDER BY`` column indexes."""
        return self._order_by_columns

    def order_by_directions(self):
        """Returns the sort direction for each ``ORDER BY`` column."""
        return self._order_by_directions

    def time_column(self):
        """Returns the 0-based ``on_time`` column index, or ``-1`` if absent."""
        return self._time_column

    def changelog_mode(self):
        """Returns the changelog mode consumed for the table argument."""
        return self._changelog_mode

    def upsert_key_columns(self):
        """Returns the candidate upsert keys as immutable tuples of column indexes."""
        return self._upsert_key_columns


@PublicEvolving()
class ProcessTableFunctionArgument(object):
    """Declaration of a scalar or table argument of a process table function.

    .. versionadded:: 2.4.0
    """

    def __init__(self, name: str, data_type: Optional[DataType], is_table: bool,
                 traits: Set[ProcessTableFunctionArgumentTrait]):
        if not isinstance(name, str) or not name:
            raise ValueError("The argument name must be a non-empty string.")
        self.name = name
        self.data_type = data_type
        self.is_table = is_table
        self.traits = frozenset(traits)

    @staticmethod
    def scalar(name: str, data_type: DataType) -> 'ProcessTableFunctionArgument':
        """Declares a scalar argument with an explicit data type."""
        if not isinstance(data_type, DataType):
            raise TypeError("A scalar argument data type must be a DataType.")
        return ProcessTableFunctionArgument(name, data_type, False, set())

    @staticmethod
    def table(name: str,
              traits: Optional[Set[ProcessTableFunctionArgumentTrait]] = None,
              data_type: Optional[DataType] = None) -> 'ProcessTableFunctionArgument':
        """Declares a polymorphic or explicitly typed table argument."""
        if data_type is not None and not isinstance(data_type, DataType):
            raise TypeError("A table argument data type must be a DataType.")
        normalized_traits = set(traits or {ProcessTableFunctionArgumentTrait.ROW_SEMANTIC_TABLE})
        if not all(isinstance(t, ProcessTableFunctionArgumentTrait) for t in normalized_traits):
            raise TypeError(
                "Table argument traits must be ProcessTableFunctionArgumentTrait values.")
        row_semantics = ProcessTableFunctionArgumentTrait.ROW_SEMANTIC_TABLE
        set_semantics = ProcessTableFunctionArgumentTrait.SET_SEMANTIC_TABLE
        if row_semantics in normalized_traits and set_semantics in normalized_traits:
            raise ValueError("A table argument cannot have both row and set semantics.")
        if row_semantics not in normalized_traits and set_semantics not in normalized_traits:
            normalized_traits.add(row_semantics)
        return ProcessTableFunctionArgument(name, data_type, True, normalized_traits)


@PublicEvolving()
class ProcessTableFunctionState(object):
    """Declaration of a keyed value state entry of a process table function.

    .. versionadded:: 2.4.0
    """

    def __init__(self, name: str, data_type: DataType, ttl=None):
        if not isinstance(name, str) or not name:
            raise ValueError("The state name must be a non-empty string.")
        from pyflink.table.types import RowType
        if not isinstance(data_type, RowType):
            raise TypeError("Process table function state must use a ROW data type.")
        if ttl is not None:
            from pyflink.common import Duration
            if not isinstance(ttl, Duration):
                raise TypeError("State TTL must be a pyflink.common.Duration.")
        self.name = name
        self.data_type = data_type
        self.ttl = ttl

    @staticmethod
    def value(name: str, data_type: DataType, ttl=None) -> 'ProcessTableFunctionState':
        """Declares a ROW value state entry."""
        return ProcessTableFunctionState(name, data_type, ttl)


T = TypeVar('T')
ACC = TypeVar('ACC')


@PublicEvolving()
class ImperativeAggregateFunction(UserDefinedFunction, Generic[T, ACC]):
    """
    Base interface for user-defined aggregate function and table aggregate function.

    This class is used for unified handling of imperative aggregating functions. Concrete
    implementations should extend from :class:`~pyflink.table.AggregateFunction` or
    :class:`~pyflink.table.TableAggregateFunction`.

    .. versionadded:: 1.13.0
    """

    @abc.abstractmethod
    def create_accumulator(self) -> ACC:
        """
        Creates and initializes the accumulator for this AggregateFunction.

        :return: the accumulator with the initial value
        """
        pass

    @abc.abstractmethod
    def accumulate(self, accumulator: ACC, *args):
        """
        Processes the input values and updates the provided accumulator instance.

        :param accumulator: the accumulator which contains the current aggregated results
        :param args: the input value (usually obtained from new arrived data)
        """
        pass

    def retract(self, accumulator: ACC, *args):
        """
        Retracts the input values from the accumulator instance.The current design assumes the
        inputs are the values that have been previously accumulated.

        :param accumulator: the accumulator which contains the current aggregated results
        :param args: the input value (usually obtained from new arrived data).
        """
        raise RuntimeError("Method retract is not implemented")

    def merge(self, accumulator: ACC, accumulators):
        """
        Merges a group of accumulator instances into one accumulator instance. This method must be
        implemented for unbounded session window grouping aggregates and bounded grouping
        aggregates.

        :param accumulator: the accumulator which will keep the merged aggregate results. It should
                            be noted that the accumulator may contain the previous aggregated
                            results. Therefore user should not replace or clean this instance in the
                            custom merge method.
        :param accumulators: a group of accumulators that will be merged.
        """
        raise RuntimeError("Method merge is not implemented")

    def get_result_type(self) -> Union[DataType, str]:
        """
        Returns the DataType of the AggregateFunction's result.

        :return: The :class:`~pyflink.table.types.DataType` of the AggregateFunction's result.

        """
        raise RuntimeError("Method get_result_type is not implemented")

    def get_accumulator_type(self) -> Union[DataType, str]:
        """
        Returns the DataType of the AggregateFunction's accumulator.

        :return: The :class:`~pyflink.table.types.DataType` of the AggregateFunction's accumulator.

        """
        raise RuntimeError("Method get_accumulator_type is not implemented")


@PublicEvolving()
class AggregateFunction(ImperativeAggregateFunction):
    """
    Base interface for user-defined aggregate function. A user-defined aggregate function maps
    scalar values of multiple rows to a new scalar value.

    .. versionadded:: 1.12.0
    """

    @abc.abstractmethod
    def get_value(self, accumulator: ACC) -> T:  # type: ignore[type-var]
        """
        Called every time when an aggregation result should be materialized. The returned value
        could be either an early and incomplete result (periodically emitted as data arrives) or
        the final result of the aggregation.

        :param accumulator: the accumulator which contains the current intermediate results
        :return: the aggregation result
        """
        pass


@PublicEvolving()
class TableAggregateFunction(ImperativeAggregateFunction):
    """
    Base class for a user-defined table aggregate function. A user-defined table aggregate function
    maps scalar values of multiple rows to zero, one, or multiple rows (or structured types). If an
    output record consists of only one field, the structured record can be omitted, and a scalar
    value can be emitted that will be implicitly wrapped into a row by the runtime.

    .. versionadded:: 1.13.0
    """

    @abc.abstractmethod
    def emit_value(self, accumulator: ACC) -> Iterable[T]:
        """
        Called every time when an aggregation result should be materialized. The returned value
        could be either an early and incomplete result (periodically emitted as data arrives) or the
        final result of the aggregation.

        :param accumulator: the accumulator which contains the current aggregated results.
        :return: multiple aggregated result
        """
        pass


@Internal()
class DelegatingScalarFunction(ScalarFunction):
    """
    Helper scalar function implementation for lambda expression and python function. It's for
    internal use only.
    """

    def __init__(self, func):
        self.func = func

    def eval(self, *args):
        return self.func(*args)


@Internal()
class DelegatingAsyncScalarFunction(AsyncScalarFunction):
    """
    Helper async scalar function implementation for async lambda expression and python async
    function. It's for internal use only.
    """

    def __init__(self, func):
        self.func = func

    async def eval(self, *args):
        return await self.func(*args)


@Internal()
class DelegationTableFunction(TableFunction):
    """
    Helper table function implementation for lambda expression and python function. It's for
    internal use only.
    """

    def __init__(self, func):
        self.func = func

    def eval(self, *args):
        return self.func(*args)


@Internal()
class DelegatingPandasAggregateFunction(AggregateFunction):
    """
    Helper pandas aggregate function implementation for lambda expression and python function.
    It's for internal use only.
    """

    def __init__(self, func):
        self.func = func

    def get_value(self, accumulator):
        return accumulator[0]

    def create_accumulator(self):
        return []

    def accumulate(self, accumulator, *args):
        accumulator.append(self.func(*args))


class PandasAggregateFunctionWrapper(object):
    """
    Wrapper for Pandas Aggregate function.
    """
    def __init__(self, func: AggregateFunction):
        self.func = func

    def open(self, function_context: FunctionContext):
        self.func.open(function_context)

    def eval(self, *args):
        accumulator = self.func.create_accumulator()
        self.func.accumulate(accumulator, *args)
        return self.func.get_value(accumulator)

    def close(self):
        self.func.close()


@Internal()
class UserDefinedFunctionWrapper(object):
    """
    Base Wrapper for Python user-defined function. It handles things like converting lambda
    functions to user-defined functions, creating the Java user-defined function representation,
    etc. It's for internal use only.
    """

    def __init__(self, func, input_types, func_type, deterministic=None, name=None):
        if inspect.isclass(func) or (
                not isinstance(func, UserDefinedFunction) and not callable(func)):
            raise TypeError(
                "Invalid function: not a function or callable (__call__ is not defined): {0}"
                .format(type(func)))

        if input_types is not None:
            from pyflink.table.types import RowType
            if isinstance(input_types, RowType):
                input_types = input_types.field_types()
            elif isinstance(input_types, (DataType, str)):
                input_types = [input_types]
            else:
                input_types = list(input_types)

            for input_type in input_types:
                if not isinstance(input_type, (DataType, str)):
                    raise TypeError(
                        "Invalid input_type: input_type should be DataType or str but contains {}"
                        .format(input_type))

        self._func = func
        self._input_types = input_types
        self._name = name or (
            func.__name__ if hasattr(func, '__name__') else func.__class__.__name__)

        if deterministic is not None and isinstance(func, UserDefinedFunction) and deterministic \
                != func.is_deterministic():
            raise ValueError("Inconsistent deterministic: {} and {}".format(
                deterministic, func.is_deterministic()))

        # default deterministic is True
        self._deterministic = deterministic if deterministic is not None else (
            func.is_deterministic() if isinstance(func, UserDefinedFunction) else True)
        self._func_type = func_type
        self._judf_placeholder = None
        self._takes_row_as_input = False

    def __call__(self, *args) -> Expression:
        from pyflink.table import expressions as expr
        return expr.call(self, *args)

    def alias(self, *alias_names: str):
        self._alias_names = alias_names
        return self

    def _set_takes_row_as_input(self):
        self._takes_row_as_input = True
        return self

    def _java_user_defined_function(self):
        if self._judf_placeholder is None:
            gateway = get_gateway()

            def get_python_function_kind():
                JPythonFunctionKind = gateway.jvm.org.apache.flink.table.functions.python. \
                    PythonFunctionKind
                if self._func_type == "general":
                    return JPythonFunctionKind.GENERAL
                elif self._func_type == "pandas":
                    return JPythonFunctionKind.PANDAS
                else:
                    raise TypeError("Unsupported func_type: %s." % self._func_type)

            if self._input_types is not None:
                if isinstance(self._input_types[0], str):
                    j_input_types = java_utils.to_jarray(gateway.jvm.String, self._input_types)
                else:
                    j_input_types = java_utils.to_jarray(
                        gateway.jvm.DataType, [_to_java_data_type(i) for i in self._input_types])
            else:
                j_input_types = None
            j_function_kind = get_python_function_kind()
            func = self._func
            if not isinstance(self._func, UserDefinedFunction):
                func = self._create_delegate_function()

            import cloudpickle
            serialized_func = cloudpickle.dumps(func)
            self._judf_placeholder = \
                self._create_judf(serialized_func, j_input_types, j_function_kind)
        return self._judf_placeholder

    def _create_delegate_function(self) -> UserDefinedFunction:
        pass

    def _create_judf(self, serialized_func, j_input_types, j_function_kind):
        pass


class UserDefinedScalarFunctionWrapper(UserDefinedFunctionWrapper):
    """
    Wrapper for Python user-defined scalar function.
    """

    def __init__(self, func, input_types, result_type, func_type, deterministic, name):
        super(UserDefinedScalarFunctionWrapper, self).__init__(
            func, input_types, func_type, deterministic, name)

        if not isinstance(result_type, (DataType, str)):
            raise TypeError(
                "Invalid returnType: returnType should be DataType or str but is {}".format(
                    result_type))
        self._result_type = result_type
        self._judf_placeholder = None

    def _create_judf(self, serialized_func, j_input_types, j_function_kind):
        gateway = get_gateway()
        if isinstance(self._result_type, DataType):
            j_result_type = _to_java_data_type(self._result_type)
        else:
            j_result_type = self._result_type
        PythonScalarFunction = gateway.jvm \
            .org.apache.flink.table.functions.python.PythonScalarFunction
        j_scalar_function = PythonScalarFunction(
            self._name,
            bytearray(serialized_func),
            j_input_types,
            j_result_type,
            j_function_kind,
            self._deterministic,
            self._takes_row_as_input,
            _get_python_env())
        return j_scalar_function

    def _create_delegate_function(self) -> UserDefinedFunction:
        return DelegatingScalarFunction(self._func)


class UserDefinedAsyncScalarFunctionWrapper(UserDefinedFunctionWrapper):
    """
    Wrapper for Python user-defined async scalar function.
    """

    def __init__(self, func, input_types, result_type, func_type, deterministic, name):
        super(UserDefinedAsyncScalarFunctionWrapper, self).__init__(
            func, input_types, func_type, deterministic, name)

        if not isinstance(result_type, (DataType, str)):
            raise TypeError(
                "Invalid returnType: returnType should be DataType or str but is {}".format(
                    result_type))
        self._result_type = result_type
        self._judf_placeholder = None

    def _create_judf(self, serialized_func, j_input_types, j_function_kind):
        gateway = get_gateway()
        if isinstance(self._result_type, DataType):
            j_result_type = _to_java_data_type(self._result_type)
        else:
            j_result_type = self._result_type
        PythonAsyncScalarFunction = gateway.jvm \
            .org.apache.flink.table.functions.python.PythonAsyncScalarFunction
        j_async_scalar_function = PythonAsyncScalarFunction(
            self._name,
            bytearray(serialized_func),
            j_input_types,
            j_result_type,
            j_function_kind,
            self._deterministic,
            self._takes_row_as_input,
            _get_python_env())
        return j_async_scalar_function

    def _create_delegate_function(self) -> UserDefinedFunction:
        return DelegatingAsyncScalarFunction(self._func)


class UserDefinedTableFunctionWrapper(UserDefinedFunctionWrapper):
    """
    Wrapper for Python user-defined table function.
    """

    def __init__(self, func, input_types, result_types, deterministic=None, name=None):
        super(UserDefinedTableFunctionWrapper, self).__init__(
            func, input_types, "general", deterministic, name)

        from pyflink.table.types import RowType
        if isinstance(result_types, RowType):
            # DataTypes.ROW([DataTypes.FIELD("f0", DataTypes.INT()),
            #               DataTypes.FIELD("f1", DataTypes.BIGINT())])
            result_types = result_types.field_types()
        elif isinstance(result_types, str):
            # ROW<f0 INT, f1 BIGINT>
            result_types = result_types
        elif isinstance(result_types, DataType):
            # DataTypes.INT()
            result_types = [result_types]
        else:
            # [DataTypes.INT(), DataTypes.BIGINT()]
            result_types = list(result_types)

        for result_type in result_types:
            if not isinstance(result_type, (DataType, str)):
                raise TypeError(
                    "Invalid result_type: result_type should be DataType or str but contains {}"
                    .format(result_type))

        self._result_types = result_types

    def _create_judf(self, serialized_func, j_input_types, j_function_kind):
        gateway = get_gateway()

        if isinstance(self._result_types, str):
            j_result_type = self._result_types
        elif isinstance(self._result_types[0], DataType):
            j_result_types = java_utils.to_jarray(
                gateway.jvm.DataType, [_to_java_data_type(i) for i in self._result_types])
            j_result_type = gateway.jvm.DataTypes.ROW(j_result_types)
        else:
            j_result_type = 'Row<{0}>'.format(','.join(
                ['f{0} {1}'.format(i, result_type)
                 for i, result_type in enumerate(self._result_types)]))
        PythonTableFunction = gateway.jvm \
            .org.apache.flink.table.functions.python.PythonTableFunction
        j_table_function = PythonTableFunction(
            self._name,
            bytearray(serialized_func),
            j_input_types,
            j_result_type,
            j_function_kind,
            self._deterministic,
            self._takes_row_as_input,
            _get_python_env())
        return j_table_function

    def _create_delegate_function(self) -> UserDefinedFunction:
        return DelegationTableFunction(self._func)


class UserDefinedProcessTableFunctionWrapper(UserDefinedFunctionWrapper):
    """Wrapper for a Python user-defined process table function."""

    def __init__(self, func: ProcessTableFunction,
                 arguments: Sequence[ProcessTableFunctionArgument],
                 states: Sequence[ProcessTableFunctionState], result_type: DataType,
                 changelog_mode: ChangelogMode, deterministic=None, name=None):
        super(UserDefinedProcessTableFunctionWrapper, self).__init__(
            func, None, "general", deterministic, name)
        self._arguments = tuple(arguments)
        self._states = tuple(states)
        self._result_type = result_type
        self._changelog_mode = changelog_mode
        self._has_on_timer = func.__class__.on_timer is not ProcessTableFunction.on_timer

    def _create_judf(self, serialized_func, j_input_types, j_function_kind):
        gateway = get_gateway()
        argument_names = java_utils.to_jarray(
            gateway.jvm.String, [argument.name for argument in self._arguments])
        argument_types = java_utils.to_jarray(
            gateway.jvm.DataType,
            [_to_java_data_type(argument.data_type) if argument.data_type is not None else None
             for argument in self._arguments])
        table_arguments = java_utils.to_jarray(
            gateway.jvm.boolean, [argument.is_table for argument in self._arguments])
        argument_traits = java_utils.to_jarray(
            gateway.jvm.String,
            [','.join(sorted(trait.value for trait in argument.traits))
             for argument in self._arguments])
        state_names = java_utils.to_jarray(
            gateway.jvm.String, [state.name for state in self._states])
        state_types = java_utils.to_jarray(
            gateway.jvm.DataType, [_to_java_data_type(state.data_type) for state in self._states])
        state_ttls = java_utils.to_jarray(
            gateway.jvm.java.time.Duration,
            [state.ttl._j_duration if state.ttl is not None else None for state in self._states])
        changelog_kinds = bytearray(
            kind.value for kind in sorted(
                self._changelog_mode.get_contained_kinds(), key=lambda kind: kind.value))

        PythonProcessTableFunction = gateway.jvm \
            .org.apache.flink.table.functions.python.PythonProcessTableFunction
        return PythonProcessTableFunction(
            self._name,
            bytearray(serialized_func),
            argument_names,
            argument_types,
            table_arguments,
            argument_traits,
            state_names,
            state_types,
            state_ttls,
            _to_java_data_type(self._result_type),
            changelog_kinds,
            self._changelog_mode.key_only_deletes(),
            self._deterministic,
            self._has_on_timer,
            _get_python_env())

    def _create_delegate_function(self) -> UserDefinedFunction:
        raise TypeError("A process table function must extend ProcessTableFunction.")


class UserDefinedAggregateFunctionWrapper(UserDefinedFunctionWrapper):
    """
    Wrapper for Python user-defined aggregate function or user-defined table aggregate function.
    """
    def __init__(self, func, input_types, result_type, accumulator_type, func_type,
                 deterministic, name, is_table_aggregate=False):
        super(UserDefinedAggregateFunctionWrapper, self).__init__(
            func, input_types, func_type, deterministic, name)

        if accumulator_type is None and func_type == "general":
            accumulator_type = func.get_accumulator_type()
        if result_type is None:
            result_type = func.get_result_type()
        if not isinstance(result_type, (DataType, str)):
            raise TypeError(
                "Invalid returnType: returnType should be DataType or str but is {}"
                .format(result_type))
        from pyflink.table.types import MapType
        if func_type == 'pandas' and isinstance(result_type, MapType):
            raise TypeError(
                "Invalid returnType: Pandas UDAF doesn't support DataType type {} currently"
                .format(result_type))
        if accumulator_type is not None and not isinstance(accumulator_type, (DataType, str)):
            raise TypeError(
                "Invalid accumulator_type: accumulator_type should be DataType or str but is {}"
                .format(accumulator_type))
        if (func_type == "general" and
                not (isinstance(result_type, str) and (accumulator_type, str) or
                     isinstance(result_type, DataType) and isinstance(accumulator_type, DataType))):
            raise TypeError("result_type and accumulator_type should be DataType or str "
                            "at the same time.")
        self._result_type = result_type
        self._accumulator_type = accumulator_type
        self._is_table_aggregate = is_table_aggregate

    def _create_judf(self, serialized_func, j_input_types, j_function_kind):
        if self._func_type == "pandas":
            if isinstance(self._result_type, DataType):
                from pyflink.table.types import DataTypes
                self._accumulator_type = DataTypes.ARRAY(self._result_type)
            else:
                self._accumulator_type = 'ARRAY<{0}>'.format(self._result_type)

        if isinstance(self._result_type, DataType):
            j_result_type = _to_java_data_type(self._result_type)
        else:
            j_result_type = self._result_type
        if isinstance(self._accumulator_type, DataType):
            j_accumulator_type = _to_java_data_type(self._accumulator_type)
        else:
            j_accumulator_type = self._accumulator_type

        gateway = get_gateway()
        if self._is_table_aggregate:
            PythonAggregateFunction = gateway.jvm \
                .org.apache.flink.table.functions.python.PythonTableAggregateFunction
        else:
            PythonAggregateFunction = gateway.jvm \
                .org.apache.flink.table.functions.python.PythonAggregateFunction
        j_aggregate_function = PythonAggregateFunction(
            self._name,
            bytearray(serialized_func),
            j_input_types,
            j_result_type,
            j_accumulator_type,
            j_function_kind,
            self._deterministic,
            self._takes_row_as_input,
            _get_python_env())
        return j_aggregate_function

    def _create_delegate_function(self) -> UserDefinedFunction:
        assert self._func_type == 'pandas'
        return DelegatingPandasAggregateFunction(self._func)


# TODO: support to configure the python execution environment
def _get_python_env():
    gateway = get_gateway()
    exec_type = gateway.jvm.org.apache.flink.table.functions.python.PythonEnv.ExecType.PROCESS
    return gateway.jvm.org.apache.flink.table.functions.python.PythonEnv(exec_type)


def _create_udf(f, input_types, result_type, func_type, deterministic, name):
    if isinstance(f, AsyncScalarFunction) or inspect.iscoroutinefunction(f):
        if func_type == 'pandas':
            raise ValueError(
                "Async scalar functions do not support pandas func_type. "
                "Please use func_type='general' (default) for async functions.")
        return UserDefinedAsyncScalarFunctionWrapper(
            f, input_types, result_type, func_type, deterministic, name)
    else:
        return UserDefinedScalarFunctionWrapper(
            f, input_types, result_type, func_type, deterministic, name)


def _create_udtf(f, input_types, result_types, deterministic, name):
    return UserDefinedTableFunctionWrapper(f, input_types, result_types, deterministic, name)


def _create_udptf(f, arguments, states, result_type, changelog_mode, deterministic, name):
    return UserDefinedProcessTableFunctionWrapper(
        f, arguments, states, result_type, changelog_mode, deterministic, name)


def _create_udaf(f, input_types, result_type, accumulator_type, func_type, deterministic, name):
    return UserDefinedAggregateFunctionWrapper(
        f, input_types, result_type, accumulator_type, func_type, deterministic, name)


def _create_udtaf(f, input_types, result_type, accumulator_type, func_type, deterministic, name):
    return UserDefinedAggregateFunctionWrapper(
        f, input_types, result_type, accumulator_type, func_type, deterministic, name, True)


def udf(f: Union[Callable, ScalarFunction, AsyncScalarFunction, Type] = None,
        input_types: Union[List[DataType], DataType, str, List[str]] = None,
        result_type: Union[DataType, str] = None,
        deterministic: bool = None, name: str = None, func_type: str = "general"
        ) -> Union[
        UserDefinedScalarFunctionWrapper, UserDefinedAsyncScalarFunctionWrapper, Callable]:
    """
    Helper method for creating a user-defined scalar function.

    This decorator can automatically detect whether the function is async (defined with `async def`
    or is an instance of AsyncScalarFunction).

    Example:
        ::

            >>> add_one = udf(lambda i: i + 1, DataTypes.BIGINT(), DataTypes.BIGINT())

            >>> # The input_types is optional.
            >>> @udf(result_type=DataTypes.BIGINT())
            ... def add(i, j):
            ...     return i + j

            >>> # Specify result_type via string.
            >>> @udf(result_type='BIGINT')
            ... def add(i, j):
            ...     return i + j

            >>> # Async function will be automatically detected
            >>> @udf(result_type=DataTypes.STRING())
            ... async def async_lookup(key):
            ...     await asyncio.sleep(0.1)
            ...     return f"value_for_{key}"

            >>> class SubtractOne(ScalarFunction):
            ...     def eval(self, i):
            ...         return i - 1
            >>> subtract_one = udf(SubtractOne(), DataTypes.BIGINT(), DataTypes.BIGINT())

            >>> # AsyncScalarFunction will be automatically detected
            >>> class AsyncLookup(AsyncScalarFunction):
            ...     async def eval(self, key):
            ...         await asyncio.sleep(0.1)
            ...         return f"value_for_{key}"
            >>> async_lookup = udf(AsyncLookup(), result_type=DataTypes.STRING())

    :param f: lambda function, user-defined function, or async function.
    :param input_types: optional, the input data types.
    :param result_type: the result data type.
    :param deterministic: the determinism of the function's results. True if and only if a call to
                          this function is guaranteed to always return the same result given the
                          same parameters. (default True)
    :param name: the function name.
    :param func_type: the type of the python function, available value: general, pandas,
                     (default: general)
    :return: UserDefinedScalarFunctionWrapper, UserDefinedAsyncScalarFunctionWrapper, or function.

    .. versionadded:: 1.10.0
    """

    if func_type not in ('general', 'pandas'):
        raise ValueError("The func_type must be one of 'general, pandas', got %s."
                         % func_type)

    # decorator
    if f is None:
        return functools.partial(_create_udf, input_types=input_types, result_type=result_type,
                                 func_type=func_type, deterministic=deterministic,
                                 name=name)
    else:
        return _create_udf(f, input_types, result_type, func_type, deterministic, name)


def udtf(f: Union[Callable, TableFunction, Type] = None,
         input_types: Union[List[DataType], DataType, str, List[str]] = None,
         result_types: Union[List[DataType], DataType, str, List[str]] = None,
         deterministic: bool = None,
         name: str = None) -> Union[UserDefinedTableFunctionWrapper, Callable]:
    """
    Helper method for creating a user-defined table function.

    Example:
        ::

            >>> # The input_types is optional.
            >>> @udtf(result_types=[DataTypes.BIGINT(), DataTypes.BIGINT()])
            ... def range_emit(s, e):
            ...     for i in range(e):
            ...         yield s, i

            >>> # Specify result_types via string
            >>> @udtf(result_types=['BIGINT', 'BIGINT'])
            ... def range_emit(s, e):
            ...     for i in range(e):
            ...         yield s, i

            >>> # Specify result_types via row string
            >>> @udtf(result_types='Row<a BIGINT, b BIGINT>')
            ... def range_emit(s, e):
            ...     for i in range(e):
            ...         yield s, i

            >>> class MultiEmit(TableFunction):
            ...     def eval(self, i):
            ...         return range(i)
            >>> multi_emit = udtf(MultiEmit(), DataTypes.BIGINT(), DataTypes.BIGINT())

    :param f: user-defined table function.
    :param input_types: optional, the input data types.
    :param result_types: the result data types.
    :param name: the function name.
    :param deterministic: the determinism of the function's results. True if and only if a call to
                          this function is guaranteed to always return the same result given the
                          same parameters. (default True)
    :return: UserDefinedTableFunctionWrapper or function.

    .. versionadded:: 1.11.0
    """
    # decorator
    if f is None:
        return functools.partial(_create_udtf, input_types=input_types, result_types=result_types,
                                 deterministic=deterministic, name=name)
    else:
        return _create_udtf(f, input_types, result_types, deterministic, name)


def _validate_process_table_function_signature(func, method_name, expected_names):
    method = getattr(func, method_name)
    parameters = list(inspect.signature(method).parameters.values())
    invalid_kind = any(parameter.kind not in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD) for parameter in parameters)
    actual_names = [parameter.name for parameter in parameters]
    if invalid_kind or actual_names != expected_names:
        raise ValueError(
            "Invalid {}() signature. Expected ({}) but found ({}).".format(
                method_name, ', '.join(expected_names), ', '.join(actual_names)))


def udptf(f: Union[ProcessTableFunction, Type] = None,
          arguments: Sequence[ProcessTableFunctionArgument] = None,
          result_type: DataType = None,
          states: Sequence[ProcessTableFunctionState] = None,
          changelog_mode: ChangelogMode = None,
          deterministic: bool = None,
          name: str = None):
    """
    Creates a Python user-defined process table function.

    The ``eval`` callback receives ``ctx``, all declared states, and all declared arguments in
    exactly that order. An optional ``on_timer`` callback receives ``ctx`` and all states.
    ``changelog_mode`` declares the row kinds that the function can produce and defaults to
    :func:`~pyflink.table.ChangelogMode.insert_only`.

    .. versionadded:: 2.4.0
    """
    if f is None:
        return functools.partial(
            udptf,
            arguments=arguments,
            result_type=result_type,
            states=states,
            changelog_mode=changelog_mode,
            deterministic=deterministic,
            name=name)

    if inspect.isclass(f) and issubclass(f, ProcessTableFunction):
        f = f()
    if not isinstance(f, ProcessTableFunction):
        raise TypeError("A process table function must extend ProcessTableFunction.")
    if arguments is None:
        raise ValueError("Process table function arguments must be declared.")
    if not isinstance(arguments, (list, tuple)) or not all(
            isinstance(argument, ProcessTableFunctionArgument) for argument in arguments):
        raise TypeError("arguments must be an ordered list of ProcessTableFunctionArgument values.")
    if states is None:
        states = []
    if not isinstance(states, (list, tuple)) or not all(
            isinstance(state, ProcessTableFunctionState) for state in states):
        raise TypeError("states must be an ordered list of ProcessTableFunctionState values.")
    from pyflink.table.types import RowType
    if not isinstance(result_type, RowType):
        raise TypeError("result_type must be a ROW DataType.")
    if changelog_mode is None:
        changelog_mode = ChangelogMode.insert_only()
    if not isinstance(changelog_mode, ChangelogMode):
        raise TypeError("changelog_mode must be a ChangelogMode.")

    table_arguments = [argument for argument in arguments if argument.is_table]
    if len(table_arguments) != 1:
        raise ValueError("A Python process table function must declare exactly one table argument.")
    names = [state.name for state in states] + [argument.name for argument in arguments]
    if len(names) != len(set(names)):
        raise ValueError("Process table function state and argument names must be unique.")

    if states and ProcessTableFunctionArgumentTrait.SET_SEMANTIC_TABLE \
            not in table_arguments[0].traits:
        raise ValueError("State requires a table argument with set semantics.")

    state_names = [state.name for state in states]
    argument_names = [argument.name for argument in arguments]
    _validate_process_table_function_signature(
        f, 'eval', ['ctx'] + state_names + argument_names)
    if f.__class__.on_timer is not ProcessTableFunction.on_timer:
        if ProcessTableFunctionArgumentTrait.SET_SEMANTIC_TABLE not in table_arguments[0].traits:
            raise ValueError("Timers require a table argument with set semantics.")
        if ProcessTableFunctionArgumentTrait.PASS_COLUMNS_THROUGH in table_arguments[0].traits:
            raise ValueError("Timers do not support pass-through columns.")
        _validate_process_table_function_signature(f, 'on_timer', ['ctx'] + state_names)

    return _create_udptf(
        f, arguments, states, result_type, changelog_mode, deterministic, name)


def udaf(f: Union[Callable, AggregateFunction, Type] = None,
         input_types: Union[List[DataType], DataType, str, List[str]] = None,
         result_type: Union[DataType, str] = None, accumulator_type: Union[DataType, str] = None,
         deterministic: bool = None, name: str = None,
         func_type: str = "general") -> Union[UserDefinedAggregateFunctionWrapper, Callable]:
    """
    Helper method for creating a user-defined aggregate function.

    Example:
        ::

            >>> # The input_types is optional.
            >>> @udaf(result_type=DataTypes.FLOAT(), func_type="pandas")
            ... def mean_udaf(v):
            ...     return v.mean()

            >>> # Specify result_type via string
            >>> @udaf(result_type='FLOAT', func_type="pandas")
            ... def mean_udaf(v):
            ...     return v.mean()

    :param f: user-defined aggregate function.
    :param input_types: optional, the input data types.
    :param result_type: the result data type.
    :param accumulator_type: optional, the accumulator data type.
    :param deterministic: the determinism of the function's results. True if and only if a call to
                          this function is guaranteed to always return the same result given the
                          same parameters. (default True)
    :param name: the function name.
    :param func_type: the type of the python function, available value: general, pandas,
                     (default: general)
    :return: UserDefinedAggregateFunctionWrapper or function.

    .. versionadded:: 1.12.0
    """
    if func_type not in ('general', 'pandas'):
        raise ValueError("The func_type must be one of 'general, pandas', got %s."
                         % func_type)
    # decorator
    if f is None:
        return functools.partial(_create_udaf, input_types=input_types, result_type=result_type,
                                 accumulator_type=accumulator_type, func_type=func_type,
                                 deterministic=deterministic, name=name)
    else:
        return _create_udaf(f, input_types, result_type, accumulator_type, func_type,
                            deterministic, name)


def udtaf(f: Union[Callable, TableAggregateFunction, Type] = None,
          input_types: Union[List[DataType], DataType, str, List[str]] = None,
          result_type: Union[DataType, str] = None,
          accumulator_type: Union[DataType, str] = None,
          deterministic: bool = None, name: str = None,
          func_type: str = 'general') -> Union[UserDefinedAggregateFunctionWrapper, Callable]:
    """
    Helper method for creating a user-defined table aggregate function.

    Example:
    ::

        >>> # The input_types is optional.
        >>> class Top2(TableAggregateFunction):
        ...     def emit_value(self, accumulator):
        ...         yield Row(accumulator[0])
        ...         yield Row(accumulator[1])
        ...
        ...     def create_accumulator(self):
        ...         return [None, None]
        ...
        ...     def accumulate(self, accumulator, *args):
        ...         if args[0] is not None:
        ...             if accumulator[0] is None or args[0] > accumulator[0]:
        ...                 accumulator[1] = accumulator[0]
        ...                 accumulator[0] = args[0]
        ...             elif accumulator[1] is None or args[0] > accumulator[1]:
        ...                 accumulator[1] = args[0]
        ...
        ...     def retract(self, accumulator, *args):
        ...         accumulator[0] = accumulator[0] - 1
        ...
        ...     def merge(self, accumulator, accumulators):
        ...         for other_acc in accumulators:
        ...             self.accumulate(accumulator, other_acc[0])
        ...             self.accumulate(accumulator, other_acc[1])
        ...
        ...     def get_accumulator_type(self):
        ...         return 'ARRAY<BIGINT>'
        ...
        ...     def get_result_type(self):
        ...         return 'ROW<a BIGINT>'
        >>> top2 = udtaf(Top2())

    :param f: user-defined table aggregate function.
    :param input_types: optional, the input data types.
    :param result_type: the result data type.
    :param accumulator_type: optional, the accumulator data type.
    :param deterministic: the determinism of the function's results. True if and only if a call to
                          this function is guaranteed to always return the same result given the
                          same parameters. (default True)
    :param name: the function name.
    :param func_type: the type of the python function, available value: general
                     (default: general)
    :return: UserDefinedAggregateFunctionWrapper or function.

    .. versionadded:: 1.13.0
    """
    if func_type != 'general':
        raise ValueError("The func_type must be 'general', got %s."
                         % func_type)
    if f is None:
        return functools.partial(_create_udtaf, input_types=input_types, result_type=result_type,
                                 accumulator_type=accumulator_type, func_type=func_type,
                                 deterministic=deterministic, name=name)
    else:
        return _create_udtaf(f, input_types, result_type, accumulator_type, func_type,
                             deterministic, name)
