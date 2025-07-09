from typing import List, Dict, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    benchmark, log_call
)
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.core.enums import ErrorType
from .spark_pivot_transformer import SparkPivotTransformer
from .spark_unpivot_transformer import SparkUnpivotTransformer
from .spark_humidity_ratio_extractor import SparkHumidityRatioExtractor



@enforce_types(strict=True)
@validate_non_empty(["conversion_query"])
@benchmark(module="TRANSFORMER", component="SparkWeatherWorkflowTransformer")
@log_call(module="TRANSFORMER", component="SparkWeatherWorkflowTransformer")
@log_exceptions(
    module="TRANSFORMER",
    component="SparkWeatherWorkflowTransformer",
    error_type=ErrorType.INIT_ERROR,
    raise_type=ValueError,
)
class SparkWeatherWorkflowTransformer(IDataTransformer):
    """
    A flexible Spark transformer that supports weather data transformations, including
    optional column derivations and humidity ratio calculations using psychrometric formulas.

    This transformer is ideal for weather-enriched datasets where calculations such as
    humidity ratio (lb water vapor / lb dry air) are needed based on dry bulb temperature (°F),
    relative humidity (%), and elevation (ft). It wraps one or more `SparkHumidityRatioExtractor`
    calls, optionally applies SQL-based derived columns, and can register the result as a temp view.

    Attributes:
        conversion_query (str): SQL query to pull the input dataset.
        derived_columns (Optional[Dict[str, str]]): Dict of SQL expressions to add new columns.
        humidity_ratio_configs (List[Dict]): humiditiy ratio configuration dictionaries.
        register_view (bool): Whether to register the final DataFrame as a temp view.
        view_name (str): Name of the temp view to register.

    Example:
    --------
    >>> from odibi_de_v2.weather_utils import SparkWeatherWorkflowTransformer
    >>> from pyspark.sql import SparkSession
    >>> import pandas as pd
    >>> spark = SparkSession.builder.getOrCreate()

    >>> data = [
    ...     {"DryBulbTemp_F": 80.0, "RelHum_Percent": 60.0, "timestamp": "2025-07-01 00:00:00"},
    ...     {"DryBulbTemp_F": 75.0, "RelHum_Percent": 45.0, "timestamp": "2025-07-01 01:00:00"},
    ...     {"DryBulbTemp_F": 90.0, "RelHum_Percent": 70.0, "timestamp": "2025-07-01 02:00:00"}
    ... ]
    >>> spark.createDataFrame(pd.DataFrame(data)).createOrReplaceTempView("weather_raw")

    >>> transformer = SparkWeatherWorkflowTransformer(
    ...     conversion_query="SELECT *, 875.2 as Elev_ft FROM weather_raw",
    ...     derived_columns={
    ...         "Elev_ft": "'875.2'"  # Can also be a column or literal expression
    ...     },
    ...     humidity_ratio_configs=[
    ...         {
    ...             "input_params": {
    ...                 "Tdb": "DryBulbTemp_F",
    ...                 "RH": "RelHum_Percent",
    ...                 "Elev_ft": "Elev_ft"
    ...             },
    ...             "prefix": "weather"
    ...         }
    ...     ],
    ...     register_view=True,
    ...     view_name="weather_enriched"
    ... )

    >>> df = transformer.transform()
    >>> df.show()

    +--------------+---------------+-------------------+--------+------------------------+
    |DryBulbTemp_F |RelHum_Percent |timestamp          |Elev_ft|weather_humidity_ratio |
    +--------------+---------------+-------------------+--------+------------------------+
    |80.0          |60.0           |2025-07-01 00:00:00|875.2   |0.013593                |
    |75.0          |45.0           |2025-07-01 01:00:00|875.2   |0.008455                |
    |90.0          |70.0           |2025-07-01 02:00:00|875.2   |0.022244                |
    +--------------+---------------+-------------------+--------+------------------------+
    """
    def __init__(
        self,
        conversion_query: str,
        humidity_ratio_configs: Optional[List[Dict]] = None,
        pre_pivot_config: Optional[Dict] = None,
        pre_unpivot_config: Optional[Dict] = None,
        derived_columns: Optional[Dict[str, str]] = None,
        post_pivot_config: Optional[Dict] = None,
        post_unpivot_config: Optional[Dict] = None,
        register_view: bool = True,
        view_name: str = "weather_workflow_view",
    ):
        self.conversion_query = conversion_query
        self.pre_pivot_config = pre_pivot_config
        self.pre_unpivot_config = pre_unpivot_config
        self.derived_columns = derived_columns or {}
        self.humidity_ratio_configs = humidity_ratio_configs or []
        self.post_pivot_config = post_pivot_config
        self.post_unpivot_config = post_unpivot_config
        self.register_view = register_view
        self.view_name = view_name

    @enforce_types(strict=True)
    @ensure_output_type(DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkWeatherWorkflowTransformer",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError,
    )
    def transform(self, **kwargs) -> DataFrame:
        from odibi_de_v2.spark_utils import get_active_spark
        spark = get_active_spark()
        df = spark.sql(self.conversion_query)

        # Optional Pre-processing
        if self.pre_pivot_config:
            df = SparkPivotTransformer(
                conversion_query=self.conversion_query,
                register_view=False,
                **self.pre_pivot_config
            ).transform()
        elif self.pre_unpivot_config:
            df = SparkUnpivotTransformer(**self.pre_unpivot_config).transform(df)

        # Derived Columns
        for col_name, col_expr in self.derived_columns.items():
            df = df.withColumn(col_name, expr(col_expr))

        # Weather Property Extractors
        for config in self.humidity_ratio_configs:
            df = SparkHumidityRatioExtractor(**config).transform(df)

        # Optional Post-processing
        if self.post_pivot_config:
            df.createOrReplaceTempView("__intermediate_weather__")
            df = SparkPivotTransformer(
                conversion_query="SELECT * FROM __intermediate_weather__",
                register_view=False,
                **self.post_pivot_config
            ).transform()
        elif self.post_unpivot_config:
            df = SparkUnpivotTransformer(**self.post_unpivot_config).transform(df)

        # Register final view
        if self.register_view:
            df.createOrReplaceTempView(self.view_name)

        return df
