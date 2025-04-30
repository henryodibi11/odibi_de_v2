from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, expr, sequence, explode, concat_ws, to_date, to_timestamp, unix_timestamp
)
from pyspark.sql.types import TimestampType

from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    log_call, benchmark
)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.core.enums import ErrorType


class SparkEventSplitter(IDataTransformer):
    """
    Initializes a transformer to split events spanning multiple intervals into separate records
    based on specified time boundaries.

    This class is designed to handle events that may span across multiple days or other time intervals,
    such as hours or minutes. It allows for the specification of an anchor time to align the interval
    boundaries, which is useful for cases where the day does not start at midnight.

    Args:
        start_time_col (str): The name of the column in the DataFrame that contains the start time of the event.
        end_time_col (str): The name of the column in the DataFrame that contains the end time of the event.
        interval_value (int, optional): The number of units each interval should represent. Defaults to 1.
        interval_unit (str, optional): The unit of time for the interval, such as "day", "hour", or "minute".
            Defaults to "day".
        anchor_time (str, optional): A string representing the time of day when the interval should start
        (e.g., "06:00:00"). Defaults to "00:00:00".
        duration_column_name (str, optional): The name of the column to store the duration of the event in minutes.
            Defaults to "duration_column_name".

    Raises:
        ValueError: If the `interval_unit` is not set to "day", as only day intervals are currently supported.

    Returns:
        DataFrame: A transformed DataFrame with events split across specified intervals and additional columns for
        adjusted start and end times, and event duration.

    Example:
        >>> splitter = SparkEventSplitter(
            "start_time", "end_time", interval_value=1, interval_unit="day", anchor_time="06:00:00")
        >>> transformed_df = splitter.transform(input_df)
        This will split events in `input_df` across days starting at 6 AM each day, adjusting the start and
        end times accordingly.
    """

    @enforce_types(strict=True)
    @validate_non_empty(["start_time_col", "end_time_col"])
    @benchmark(module="TRANSFORMER", component="SparkEventSplitter")
    @log_call(module="TRANSFORMER", component="SparkEventSplitter")
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkEventSplitter",
        error_type=ErrorType.INIT_ERROR,
        raise_type=ValueError,
    )
    def __init__(
        self,
        start_time_col: str,
        end_time_col: str,
        interval_value: int = 1,
        interval_unit: str = "day",
        anchor_time: str = "00:00:00",
        duration_column_name: str = "duration_column_name"
    ):
        """
        Initializes a new instance of the SparkEventSplitter class, configuring it to split events based on
        specified time intervals.

        Args:
            start_time_col (str): The name of the column in the data that contains the start time of each event.
            end_time_col (str): The name of the column in the data that contains the end time of each event.
            interval_value (int, optional): The numerical value of the time interval for splitting events.
                Defaults to 1.
            interval_unit (str, optional): The unit of time for the interval. Currently, only 'day' is supported.
                Defaults to "day".
            anchor_time (str, optional): A time string in the format "HH:MM:SS" that sets the daily anchor time for
                interval calculation. Defaults to "00:00:00".
            duration_column_name (str, optional): The name to assign to the new column that will store the duration
                of each split event. Defaults to "duration_column_name".

        Raises:
            ValueError: If the `interval_unit` is not set to "day".

        Example:
            >>> splitter = SparkEventSplitter(
                    start_time_col="start_time",
                    end_time_col="end_time",
                    interval_value=1,
                    interval_unit="day",
                    anchor_time="00:00:00",
                    duration_column_name="event_duration"
                )
        """
        if interval_unit.lower() != "day":
            raise ValueError("Only 'day' interval is currently supported.")

        self.start_time_col = start_time_col
        self.end_time_col = end_time_col
        self.interval_value = interval_value
        self.interval_unit = interval_unit.lower()
        self.anchor_time = anchor_time
        self.duration_column_name = duration_column_name

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="SparkEventSplitter",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=(
                f"start={start_time_col}, end={end_time_col}, "
                f"interval={interval_value} {interval_unit}, anchor_time={anchor_time}"),
            level="INFO"
        )

    @enforce_types(strict=True)
    @ensure_output_type(DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkEventSplitter",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError,
    )
    def transform(self, data: DataFrame, **kwargs) -> DataFrame:
        """
        Transforms the input DataFrame by expanding multi-day events into multiple single-day events based on
        specified interval settings.

        This method processes a DataFrame containing event data with start and end times. It splits multi-day
        events into multiple rows, each representing a single day or a fraction of a day as specified by the
        interval settings. Single-day events are processed to calculate their duration. The transformed DataFrame
        includes updated start and end times and a new column for event duration.

        Args:
            data (DataFrame): A DataFrame containing event data with columns corresponding to start and end times.
            **kwargs: Additional keyword arguments that can be used for future extensions or specific transformations.

        Returns:
            DataFrame: A DataFrame with expanded and transformed event data. Each row represents a single day or
            part of a day for multi-day events, and original rows for single-day events, with an additional column
            for event duration.

        Raises:
            ValueError: If 'start_time_col' or 'end_time_col' are not present in the DataFrame.
            TypeError: If the input 'data' is not a DataFrame.

        Example:
            Assuming `transformer` is an instance of a class containing this method, and `events_df` is a DataFrame
            with event data:

            ```python
            transformed_df = transformer.transform(events_df)
            print(transformed_df.show())
            ```

        Note:
            The method assumes 'start_time_col' and 'end_time_col' are initialized in the instance and are present
            in the input DataFrame. It also uses 'interval_value' and 'interval_unit' from the instance to determine
            the splitting intervals for multi-day events.
        """
        start_col = self.start_time_col
        end_col = self.end_time_col

        # Calculate number of days between start and end
        data = data.withColumn(
            "event_days",
            expr(f"datediff(to_date({end_col}), to_date({start_col})) + 1")
        )

        multi_day_events = data.filter(col("event_days") > 1)
        single_day_events = data.filter(col("event_days") == 1)

        exploded = multi_day_events.withColumn(
            "explode_day",
            explode(sequence(
                to_date(col(start_col)),
                to_date(col(end_col)),
                expr(f"interval {self.interval_value} {self.interval_unit}")
            ))
        )

        exploded = exploded.withColumn(
            start_col,
            when(
                to_date(col("explode_day")) == to_date(col(start_col)),
                col(start_col)
            ).otherwise(
                to_timestamp(concat_ws(" ", col("explode_day"), lit(self.anchor_time)))
            )
        ).withColumn(
            end_col,
            when(
                to_date(col("explode_day")) == to_date(col(end_col)),
                col(end_col)
            ).otherwise(
                to_timestamp(concat_ws(
                    " ",
                    (col("explode_day") + expr(f"INTERVAL {self.interval_value} {self.interval_unit.upper()}")),
                    lit(self.anchor_time)
                ))
            )
        )

        exploded = exploded \
            .withColumn(start_col, col(start_col).cast(TimestampType())) \
            .withColumn(end_col, col(end_col).cast(TimestampType())) \
            .withColumn(self.duration_column_name,
                        (unix_timestamp(col(end_col)) - unix_timestamp(col(start_col))) / 60) \
            .drop("explode_day", "event_days")

        single_day_events = single_day_events \
            .withColumn(self.duration_column_name,
                        (unix_timestamp(col(end_col)) - unix_timestamp(col(start_col))) / 60) \
            .drop("event_days")

        return single_day_events.unionByName(exploded)
