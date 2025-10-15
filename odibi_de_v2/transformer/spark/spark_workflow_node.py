from typing import List, Union, Callable, Dict, Any, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
import json
import importlib

from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, ensure_output_type, benchmark, log_call
)
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.spark_utils import get_active_spark
import time


StepType = Union[
    str,
    Dict[str, Any],
    Callable[[DataFrame], DataFrame],
    Tuple[Callable[..., DataFrame], Dict[str, Any]]
]



@enforce_types(strict=True)
@benchmark(module="TRANSFORMER", component="SparkWorkflowNode")
@log_call(module="TRANSFORMER", component="SparkWorkflowNode")
@log_exceptions(
    module="TRANSFORMER",
    component="SparkWorkflowNode",
    error_type=ErrorType.INIT_ERROR,
    raise_type=ValueError,
)
class SparkWorkflowNode(IDataTransformer):
    """
    A flexible workflow node that executes a sequence of transformations on a Spark DataFrame.

    ## Why use this?
    `SparkWorkflowNode` lets you combine:
    - **SQL queries** against Spark temp views.
    - **Config-driven operations** (`pivot`, `unpivot`, `derived`).
    - **Python functions** for custom transformations.
    - A mix of both config + Python, in one consistent interface.

    Each node optionally registers its output as a Spark temp view for downstream use.
    Individual steps can also register intermediate views if `"view_name"` is provided.

    ---
    ## 📊 Example Dataset
    >>> from pyspark.sql import SparkSession
    >>> spark = (
    ...     SparkSession.builder.master("local[*]").appName("WorkflowExample").getOrCreate()
    ... )
    >>> data = [
    ...     (100, 200, 50, "good"),
    ...     (90, 180, 55, "bad"),
    ... ]
    >>> df_raw = spark.createDataFrame(data, ["temperature", "pressure", "humidity", "status"])
    >>> df_raw.createOrReplaceTempView("raw_weather")
    >>> df_raw.show()
    +-----------+--------+--------+------+
    |temperature|pressure|humidity|status|
    +-----------+--------+--------+------+
    |        100|     200|      50|  good|
    |         90|     180|      55|   bad|
    +-----------+--------+--------+------+

    ---
    ## 📝 Example 1: SQL only
    >>> node = SparkWorkflowNode(
    ...     steps=["SELECT * FROM raw_weather"],
    ...     view_name="weather_sql"
    ... )
    >>> node.transform().show()

    ---
    ## 📝 Example 2: SQL + Derived Column
    >>> node = SparkWorkflowNode(
    ...     steps=[
    ...         "SELECT * FROM raw_weather",
    ...         {"operation": "derived", "params": {"temp_c": "(temperature-32)*5/9"}}
    ...     ],
    ...     view_name="weather_derived"
    ... )
    >>> node.transform().show()

    ---
    ## 📝 Example 3: Native Python Function
    >>> from pyspark.sql.functions import when, col
    >>> def add_flag(df):
    ...     return df.withColumn("flag", when(col("status") == "bad", 1).otherwise(0))
    ...
    >>> node = SparkWorkflowNode(
    ...     steps=["SELECT * FROM raw_weather", add_flag],
    ...     view_name="weather_flag"
    ... )
    >>> node.transform().show()

    ---
    ## 📝 Example 4: Python Function with Parameters
    >>> def normalize_column(df, colname: str, factor: float):
    ...     return df.withColumn(colname, df[colname] / factor)
    ...
    >>> node = SparkWorkflowNode(
    ...     steps=[
    ...         "SELECT * FROM raw_weather",
    ...         (normalize_column, {"colname": "temperature", "factor": 100})
    ...     ],
    ...     view_name="weather_norm"
    ... )
    >>> node.transform().show()

    ---
    ## 📝 Example 5: Config Pivot (long → wide)
    >>> steps_pivot = [
    ...     {"step_type": "sql", "step_value": "SELECT * FROM raw_weather", "view_name": "weather_base"},
    ...     {"step_type": "config", "step_value": "pivot",
    ...      "params": {
    ...          "conversion_query": "SELECT * FROM weather_base",
    ...          "group_by": ["status"],
    ...          "pivot_column": "humidity",
    ...          "value_column": "temperature",
    ...          "agg_func": "first"
    ...      },
    ...      "view_name": "weather_pivot"}
    ... ]
    >>> node = SparkWorkflowNode(steps=steps_pivot, view_name="weather_pivot")
    >>> node.transform().show()

    ---
    ## 📝 Example 6: Config Unpivot (wide → long)
    >>> # Imagine df_wide has columns: ["id", "temp", "pressure", "humidity"]
    >>> df_wide.createOrReplaceTempView("wide_weather")
    >>> steps_unpivot = [
    ...     {"step_type": "sql", "step_value": "SELECT * FROM wide_weather", "view_name": "wide_weather"},
    ...     {"step_type": "config", "step_value": "unpivot",
    ...      "params": {"id_columns": ["id"]},
    ...      "view_name": "weather_unpivot"}
    ... ]
    >>> node = SparkWorkflowNode(steps=steps_unpivot, view_name="weather_unpivot")
    >>> node.transform().show()

    ---
    ## 📝 Example 7: Mixing Config + Python with Intermediate Views
    >>> steps_mixed = [
    ...     {"step_type": "sql", "step_value": "SELECT * FROM raw_weather", "view_name": "weather_start"},
    ...     (normalize_column, {"colname": "temperature", "factor": 100}),
    ...     {"step_type": "sql", "step_value": "SELECT temperature FROM weather_start",
    ...      "view_name": "weather_temp"}
    ... ]
    >>> node = SparkWorkflowNode(steps=steps_mixed, view_name="weather_mixed")
    >>> node.transform().show()

    ---
    ## 🔑 Best Practices
    - Functions should **accept a DataFrame and return a DataFrame**.
    - Use `dotted paths` for reusable functions (e.g., `"odibi_de_v2.weather.normalize_column"`).
    - Use config for **simple operations**; Python for **complex logic**.
    - Use `"view_name"` inside a step **only when you need to reference it downstream**.
    - Avoid registering views for every step to prevent Spark namespace clutter.
    - Final output is always registered under the node’s `view_name`.
    - Remember: **pivot requires a SQL `conversion_query`**, while **unpivot consumes an existing DataFrame**.

    """
    from odibi_de_v2.transformer.spark.transformation_tracker import TransformationTracker


    def __init__(
        self,
        steps: List[StepType],
        view_name: str = "workflow_output",
        register_view: bool = True,
        tracker: Optional[TransformationTracker] = None,
    ):
        self.steps = steps
        self.view_name = view_name
        self.register_view = register_view
        self.tracker = tracker

    @ensure_output_type(DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkWorkflowNode",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError,
    )
    def transform(self, **kwargs) -> DataFrame:
        spark = get_active_spark()
        df: Optional[DataFrame] = None

        tracker = kwargs.get("tracker", self.tracker)
        parent_node = kwargs.get("parent_node")
        compute_counts = kwargs.get("compute_counts", False)

        if tracker:
            tracker.new_run()

        # Preserve list order unless explicit step_order is provided
        ordered_steps = sorted(
            [(s.get("step_order", i), s) if isinstance(s, dict) else (i, s)
             for i, s in enumerate(self.steps)],
            key=lambda x: x[0]
        )

        step_counter = 0
        for _, step in ordered_steps:
            step_view_name = None
            before_df = df
            step_type, intent = None, None
            step_counter +=1
            start_time = time.time()

            # --- Config/metadata style ---
            if isinstance(step, dict) and "step_type" in step:
                step_type = step.get("step_type")
                step_value = step.get("step_value")
                params = step.get("params", {})
                if isinstance(params, str):
                    params = json.loads(params)

                step_view_name = step.get("view_name")  # 👈 optional per-step

                if step_type == "sql":
                    df = spark.sql(step_value)
                elif step_type == "config":
                    df = self._apply_config_op(df, step_value, params)
                elif step_type == "python":
                    func = self._load_function(step_value)
                    df = func(df, **params)
                else:
                    raise ValueError(f"Unsupported step_type: {step_type}")

            # --- Native Python style ---
            elif isinstance(step, str):  # SQL
                df = spark.sql(step)
            elif isinstance(step, dict):  # Config dict without step_type
                df = self._apply_config_op(df, step["operation"], step.get("params", {}))
            elif callable(step):  # Direct function
                df = step(df)
            elif isinstance(step, tuple):  # (func or dotted_path, params)
                func, params = step
                if isinstance(func, str):
                    func = self._load_function(func)
                df = func(df, **params)
            else:
                raise ValueError(f"Unsupported step type: {type(step)}")

            # --- Register intermediate view if requested ---
            if step_view_name and df is not None:
                df.createOrReplaceTempView(step_view_name)
            # 🔹 Log transformation step
            duration = time.time() - start_time
            if tracker and df is not None:
                tracker.log(before_df, df,
                            node_name=self.view_name,
                            step_type=step_type,
                            intent=intent,
                            parent_node=parent_node,
                            step_order=step_counter,
                            duration_seconds=duration,
                            compute_counts=compute_counts)

        if self.register_view and df is not None:
            df.createOrReplaceTempView(self.view_name)

        return df

    def _apply_config_op(self, df: DataFrame, op: str, params: Dict[str, Any]) -> DataFrame:
        """Apply config-driven pivot, unpivot, or derived column operations."""
        if op == "pivot":
            from odibi_de_v2.transformer import SparkPivotTransformer
            return SparkPivotTransformer(**params, register_view=False).transform()

        elif op == "unpivot":
            from odibi_de_v2.transformer import SparkUnpivotTransformer
            return SparkUnpivotTransformer(**params).transform(df)

        elif op == "derived":
            for col_name, col_expr in params.items():
                df = df.withColumn(col_name, expr(col_expr))
            return df

        else:
            raise ValueError(f"Unknown operation: {op}")

    def _load_function(self, dotted_path: str) -> Callable:
        """
        Dynamically import a Python function from a dotted path string.

        Args:
            dotted_path (str): Full dotted path to a Python function.
                               Example: "odibi_de_v2.weather.normalize_column"

        Returns:
            Callable: Imported function object.

        Raises:
            ImportError: If the module or function cannot be imported.
        """
        module_path, func_name = dotted_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, func_name)
