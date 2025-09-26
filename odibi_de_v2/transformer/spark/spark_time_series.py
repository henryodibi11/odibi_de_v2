from pyspark.sql import DataFrame, Window, SparkSession, Column
from pyspark.sql import functions as F
from typing import List, Optional, Literal

def rolling_window(
    df: DataFrame,
    ts_col: str,
    value_col: str,
    window_size: int,
    output_col: str,
    agg: Literal["sum","avg","min","max","count"] = "sum",
    partition_by: Optional[List[str]] = None,
    time_based: bool = False,
    time_unit: Literal["days","hours","seconds","minutes","weeks"] = "days"
) -> DataFrame:
    """
    Add a rolling aggregate column over a time-ordered window.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    ts_col : str
        Timestamp column to order by.
    value_col : str
        Column to aggregate.
    window_size : int
        Window size (rows or time units).
    output_col : str
        Name of the new column to create.
    agg : str
        Aggregate function: "sum", "avg", "min", "max", "count".
    partition_by : list[str], optional
        Keys to partition by (e.g., Plant, Asset).
    time_based : bool
        If True → rolling by time range instead of rows.
    time_unit : str
        Unit for time-based windows.

    Returns
    -------
    DataFrame
        DataFrame with new rolling aggregate column.

    Examples
    --------
    >>> from pyspark.sql import SparkSession, functions as F
    >>> from odibi_de_v2.transformer.spark.spark_time_series import rolling_window
    >>> spark = SparkSession.builder.getOrCreate()
    >>> data = [
    ...     ("CR", "Boiler1", "2025-01-01", 100),
    ...     ("CR", "Boiler1", "2025-01-02", 120),
    ...     ("CR", "Boiler1", "2025-01-03", 80),
    ...     ("CR", "Boiler1", "2025-01-04", 110),
    ... ]
    >>> df = spark.createDataFrame(data, ["Plant","Asset","Time_Stamp","Load"]) \\
    ...           .withColumn("Time_Stamp", F.to_timestamp("Time_Stamp"))
    >>> # Row-based 2-day rolling sum
    >>> df_out = rolling_window(
    ...     df, ts_col="Time_Stamp", value_col="Load",
    ...     window_size=2, agg="sum",
    ...     partition_by=["Plant","Asset"],
    ...     output_col="Load_roll2_sum"
    ... )
    >>> df_out.show()
    """
    func_map = {"sum": F.sum, "avg": F.avg, "min": F.min, "max": F.max, "count": F.count}
    df = df.withColumn(ts_col, F.to_timestamp(F.col(ts_col)))
    if agg not in func_map:
        raise ValueError(f"Unsupported agg '{agg}'. Must be one of {list(func_map.keys())}")

    if time_based:
        seconds_per_unit = {"seconds":1,"minutes":60,"hours":3600,"days":86400,"weeks":604800}
        if time_unit not in seconds_per_unit:
            raise ValueError("time_unit must be one of " + str(list(seconds_per_unit.keys())))
        w = (
            Window.partitionBy(*(partition_by or []))
            .orderBy(F.col(ts_col).cast("long"))
            .rangeBetween(-window_size * seconds_per_unit[time_unit], 0)
        )
    else:
        w = (
            Window.partitionBy(*(partition_by or []))
            .orderBy(ts_col)
            .rowsBetween(-window_size + 1, 0)
        )
    return df.withColumn(output_col, func_map[agg](F.col(value_col)).over(w))


def get_week_start(ts_col: Column, week_start: str = "MON") -> Column:
    """
    Get the start-of-week date for a given timestamp column.

    Parameters
    ----------
    ts_col : Column
        Timestamp/date column.
    week_start : str
        Day of week abbreviation ("SUN","MON","TUE","WED","THU","FRI","SAT").

    Returns
    -------
    Column
        Date column representing the week-start date.
    """
    week_start = week_start.upper()
    valid_days = ["SUN","MON","TUE","WED","THU","FRI","SAT"]
    if week_start not in valid_days:
        raise ValueError(f"week_start must be one of {valid_days}")
    return F.date_sub(F.next_day(ts_col, week_start), 7)


def get_month_start(ts_col: Column) -> Column:
    """Get the first day of the month for a given timestamp column."""
    return F.trunc(ts_col, "month")


def get_quarter_start(ts_col: Column) -> Column:
    """Get the first day of the quarter for a given timestamp column."""
    year = F.year(ts_col)
    quarter = F.quarter(ts_col)
    month = (quarter - 1) * 3 + 1
    return F.to_date(F.concat_ws("-", year, month, F.lit(1)))


def get_year_start(ts_col: Column) -> Column:
    """Get the first day of the year for a given timestamp column."""
    return F.trunc(ts_col, "year")


def period_to_date(
    df: DataFrame,
    ts_col: str,
    value_col: str,
    period: Literal["week","month","quarter","year"],
    output_col: str,
    agg: Literal["sum","avg","min","max","count"] = "sum",
    partition_by: Optional[List[str]] = None,
    week_start: str = "MON"
) -> DataFrame:
    """
    Compute period-to-date aggregates (WTD, MTD, QTD, YTD).

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    ts_col : str
        Timestamp column to order by.
    value_col : str
        Column to aggregate.
    period : str
        Calendar period: "week", "month", "quarter", "year".
    output_col : str
        Name of the new column to create.
    agg : str
        Aggregate function: "sum", "avg", "min", "max", "count".
    partition_by : list[str], optional
        Keys to partition by (e.g., Plant, Asset).
    week_start : str
        Week start day ("SUN","MON","TUE","WED","THU","FRI","SAT"). Default = "MON".

    Returns
    -------
    DataFrame
        DataFrame with period-to-date aggregate column.

    Examples
    --------
    >>> from pyspark.sql import SparkSession, functions as F
    >>> from odibi_de_v2.transformer.spark.spark_time_series import period_to_date
    >>> spark = SparkSession.builder.getOrCreate()
    >>> data = [
    ...     ("CR","Boiler1","2025-01-01",100),
    ...     ("CR","Boiler1","2025-01-02",120),
    ...     ("CR","Boiler1","2025-01-03",80),
    ...     ("CR","Boiler1","2025-01-04",110),
    ... ]
    >>> df = spark.createDataFrame(data, ["Plant","Asset","Time_Stamp","Load"]) \\
    ...           .withColumn("Time_Stamp", F.to_timestamp("Time_Stamp"))
    >>> df_out = period_to_date(
    ...     df, ts_col="Time_Stamp", value_col="Load",
    ...     period="week", agg="sum",
    ...     partition_by=["Plant","Asset"],
    ...     output_col="Load_WTD",
    ...     week_start="MON"
    ... )
    >>> df_out.show()
    """
    func_map = {"sum": F.sum, "avg": F.avg, "min": F.min, "max": F.max, "count": F.count}
    if agg not in func_map:
        raise ValueError(f"Unsupported agg '{agg}'")

    df = df.withColumn(ts_col, F.to_timestamp(F.col(ts_col)))

    if period == "week":
        period_start = get_week_start(F.col(ts_col), week_start)
    elif period == "month":
        period_start = get_month_start(F.col(ts_col))
    elif period == "quarter":
        period_start = get_quarter_start(F.col(ts_col))
    elif period == "year":
        period_start = get_year_start(F.col(ts_col))
    else:
        raise ValueError("period must be one of: 'week','month','quarter','year'")

    w = (
        Window.partitionBy(*(partition_by or []), period_start)
        .orderBy(ts_col)
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    return df.withColumn(output_col, func_map[agg](F.col(value_col)).over(w))


def fill_time_gaps(
    df: DataFrame,
    ts_col: str,
    partition_by: Optional[List[str]] = None,
    freq: Literal["second","minute","hour","day","week","month","quarter","year"] = "day",
    step: int = 1,
    fill_value: Optional[float] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    fill_strategy: Literal[
        "none", "constant", "forward", "backward",
        "forward_first", "backward_first", "nearest"
    ] = "forward_first",
    add_flag: bool = True
) -> DataFrame:
    """
    Fill missing time points in a partitioned time series DataFrame.
    Supports flexible frequencies and multiple filling strategies.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with timestamp column.
    ts_col : str
        Timestamp column name.
    partition_by : list[str], optional
        Keys to partition by (e.g., Plant, Asset).
    freq : str
        Frequency for calendar generation.
    step : int
        Step size between generated timestamps.
    fill_value : float, optional
        Value to use for "constant" fill strategy.
    start : str, optional
        Global calendar start timestamp.
    end : str, optional
        Global calendar end timestamp.
    fill_strategy : str
        One of:
          - "none" → leave NULLs
          - "constant" → replace with fill_value
          - "forward" → forward-fill only
          - "backward" → back-fill only
          - "forward_first" → forward then backward (default)
          - "backward_first" → backward then forward
          - "nearest" → closest available value in time
    add_flag : bool
        If True, adds `is_filled` column (1 = synthetic, 0 = original).

    Returns
    -------
    DataFrame
        DataFrame with gaps filled according to strategy.

    Examples
    --------
    >>> from pyspark.sql import SparkSession, functions as F
    >>> from odibi_de_v2.transformer.spark.spark_time_series import fill_time_gaps
    >>> spark = SparkSession.builder.getOrCreate()
    >>> data = [
    ...     ("CR", "Boiler1", "2025-01-01", 100),
    ...     ("CR", "Boiler1", "2025-01-04", 110)  # missing Jan 2–3
    ... ]
    >>> df = spark.createDataFrame(data, ["Plant","Asset","Time_Stamp","Load"]) \\
    ...           .withColumn("Time_Stamp", F.to_timestamp("Time_Stamp"))
    >>> df_filled = fill_time_gaps(
    ...     df, ts_col="Time_Stamp", partition_by=["Plant","Asset"],
    ...     freq="day", fill_strategy="forward", fill_value=0
    ... )
    >>> df_filled.orderBy("Time_Stamp").show()
    """
    df = df.withColumn(ts_col, F.to_timestamp(ts_col))

    # Compute min/max per partition
    if start and end:
        stats = df.select(*(partition_by or [])).distinct()
        stats = stats.withColumn("min_ts", F.lit(start).cast("timestamp"))
        stats = stats.withColumn("max_ts", F.lit(end).cast("timestamp"))
    else:
        stats = (
            df.groupBy(*(partition_by or []))
            .agg(F.min(ts_col).alias("min_ts"), F.max(ts_col).alias("max_ts"))
        )

    # Calendar backbone
    interval_str = f"interval {step} {freq}"
    cal = stats.withColumn("seq", F.expr(f"sequence(min_ts, max_ts, {interval_str})")) \
               .withColumn(ts_col, F.explode("seq")) \
               .drop("seq","min_ts","max_ts")

    df_out = cal.join(
        df.withColumn("_is_original", F.lit(1)),
        on=([*partition_by, ts_col] if partition_by else [ts_col]),
        how="left"
    )

    if add_flag:
        df_out = df_out.withColumn(
            "is_filled",
            F.when(F.col("_is_original").isNull(), F.lit(1)).otherwise(F.lit(0))
        ).drop("_is_original")

    # Define windows
    w_ff = Window.partitionBy(*(partition_by or [])).orderBy(ts_col).rowsBetween(Window.unboundedPreceding, 0)
    w_bf = Window.partitionBy(*(partition_by or [])).orderBy(ts_col).rowsBetween(0, Window.unboundedFollowing)

    # Apply strategy
    for c in df.columns:
        if c in (partition_by or []) + [ts_col]:
            continue

        if fill_strategy == "none":
            continue
        elif fill_strategy == "constant":
            df_out = df_out.withColumn(c, F.coalesce(F.col(c), F.lit(fill_value)))
        elif fill_strategy == "forward":
            df_out = df_out.withColumn(c, F.last(c, ignorenulls=True).over(w_ff))
        elif fill_strategy == "backward":
            df_out = df_out.withColumn(c, F.first(c, ignorenulls=True).over(w_bf))
        elif fill_strategy == "forward_first":
            df_out = df_out.withColumn(c, F.last(c, ignorenulls=True).over(w_ff))
            df_out = df_out.withColumn(c, F.first(c, ignorenulls=True).over(w_bf))
        elif fill_strategy == "backward_first":
            df_out = df_out.withColumn(c, F.first(c, ignorenulls=True).over(w_bf))
            df_out = df_out.withColumn(c, F.last(c, ignorenulls=True).over(w_ff))
        elif fill_strategy == "nearest":
            df_out = (
                df_out.withColumn("prev_val", F.last(c, ignorenulls=True).over(w_ff))
                      .withColumn("next_val", F.first(c, ignorenulls=True).over(w_bf))
                      .withColumn("prev_ts", F.last(ts_col, ignorenulls=True).over(w_ff))
                      .withColumn("next_ts", F.first(ts_col, ignorenulls=True).over(w_bf))
                      .withColumn(
                          c,
                          F.when(F.col(c).isNotNull(), F.col(c))
                           .when(
                               (F.col("prev_val").isNotNull()) & (F.col("next_val").isNotNull()),
                               F.when(
                                   (F.col(ts_col).cast("long") - F.col("prev_ts").cast("long"))
                                   <= (F.col("next_ts").cast("long") - F.col(ts_col).cast("long")),
                                   F.col("prev_val")
                               ).otherwise(F.col("next_val"))
                           )
                           .when(F.col("prev_val").isNotNull(), F.col("prev_val"))
                           .otherwise(F.col("next_val"))
                      )
                      .drop("prev_val","next_val","prev_ts","next_ts")
            )

    return df_out


def lag_lead_gap(
    df: DataFrame,
    ts_col: str,
    value_col: str,
    output_col: str,
    offset: int = 1,
    partition_by: Optional[List[str]] = None,
    direction: Literal["lag","lead"] = "lag",
    fillna: Optional[float] = None
) -> DataFrame:
    """
    Row-based lag/lead with gap (diff) calculation.
    Assumes rows are consistent (e.g., monthly totals, daily values).

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    ts_col : str
        Column to order by.
    value_col : str
        Column to lag/lead.
    output_col : str
        Base name of new column.
    offset : int, default=1
        Number of rows to look back/forward.
    partition_by : list[str], optional
        Keys to partition by.
    direction : str, default="lag"
        "lag" or "lead".
    fillna : scalar, optional
        Value to replace NULLs (e.g., 0).

    Returns
    -------
    DataFrame
        DataFrame with lag/lead value and diff column.

    Examples
    --------
    >>> from pyspark.sql import SparkSession
    >>> from odibi_de_v2.transformer.spark.spark_time_series import lag_lead_gap
    >>> spark = SparkSession.builder.getOrCreate()
    >>> data = [
    ...     ("CR","Boiler1","2024-01",100),
    ...     ("CR","Boiler1","2024-02",120),
    ...     ("CR","Boiler1","2024-03",140),
    ... ]
    >>> df = spark.createDataFrame(data, ["Plant","Asset","Month","Load"])
    >>> df_out = lag_lead_gap(
    ...     df, ts_col="Month", value_col="Load",
    ...     output_col="Load_prev_month",
    ...     offset=1, direction="lag",
    ...     fillna=0,
    ...     partition_by=["Plant","Asset"]
    ... )
    >>> df_out.show()
    """
    w = Window.partitionBy(*(partition_by or [])).orderBy(ts_col)

    if direction == "lag":
        shifted = F.lag(value_col, offset).over(w)
    elif direction == "lead":
        shifted = F.lead(value_col, offset).over(w)
    else:
        raise ValueError("direction must be 'lag' or 'lead'")

    df_out = (
        df.withColumn(f"{output_col}_raw", shifted)
          .withColumn(
              f"{output_col}_diff",
              F.when(shifted.isNull(), F.col(value_col))  # if no prior value, diff = full value
               .otherwise(F.col(value_col) - shifted)
          )
    )

    if fillna is not None:
        new_cols = [c for c in df_out.columns if c.startswith(output_col)]
        df_out = df_out.fillna({c: fillna for c in new_cols})

    return df_out


def cumulative_window(
    df: DataFrame,
    ts_col: str,
    value_col: str,
    output_col: str,
    agg: Literal["sum","avg","min","max","count"] = "sum",
    partition_by: Optional[List[str]] = None,
) -> DataFrame:
    """
    Add a cumulative aggregate column (running total, running avg, etc.).

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    ts_col : str
        Column to order by.
    value_col : str
        Column to aggregate.
    output_col : str
        Name of the new column.
    agg : str
        Aggregate function: "sum", "avg", "min", "max", "count".
    partition_by : list[str], optional
        Keys to partition by (e.g., Plant, Asset).

    Returns
    -------
    DataFrame
        DataFrame with new cumulative column.

    Examples
    --------
    >>> from pyspark.sql import SparkSession
    >>> from odibi_de_v2.transformer.spark.spark_time_series import cumulative_window
    >>> spark = SparkSession.builder.getOrCreate()
    >>> data = [
    ...     ("CR","Boiler1","2024-01",100),
    ...     ("CR","Boiler1","2024-02",120),
    ...     ("CR","Boiler1","2024-03",140),
    ... ]
    >>> df = spark.createDataFrame(data, ["Plant","Asset","Month","Load"])
    >>> df_out = cumulative_window(
    ...     df, ts_col="Month", value_col="Load",
    ...     output_col="Load_cum_sum", agg="sum",
    ...     partition_by=["Plant","Asset"]
    ... )
    >>> df_out.show()
    """
    func_map = {"sum": F.sum, "avg": F.avg, "min": F.min, "max": F.max, "count": F.count}
    if agg not in func_map:
        raise ValueError(f"Unsupported agg '{agg}'")

    w = (
        Window.partitionBy(*(partition_by or []))
              .orderBy(ts_col)
              .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return df.withColumn(output_col, func_map[agg](F.col(value_col)).over(w))


def add_period_columns(df: DataFrame, ts_col: str) -> DataFrame:
    """
    Add standard calendar period columns (Year, Quarter, Month, Week, Day)
    for use in partitioning time-based aggregations.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    ts_col : str
        Timestamp or date column.

    Returns
    -------
    DataFrame
        DataFrame with added columns:
          - Year
          - Quarter
          - Month
          - Week
          - Day
          - YearMonth (YYYY-MM string for convenience)
          - YearWeek (YYYY-WW string for convenience)

    Examples
    --------
    >>> from pyspark.sql import SparkSession, functions as F
    >>> from odibi_de_v2.transformer.spark.spark_time_series import add_period_columns
    >>> spark = SparkSession.builder.getOrCreate()
    >>> data = [
    ...     ("CR","Boiler1","2024-01-01",100),
    ...     ("CR","Boiler1","2024-02-01",120),
    ... ]
    >>> df = spark.createDataFrame(data, ["Plant","Asset","Time_Stamp","Load"]) \\
    ...           .withColumn("Time_Stamp", F.to_date("Time_Stamp"))
    >>> df_out = add_period_columns(df, "Time_Stamp")
    >>> df_out.show()
    """
    return (
        df.withColumn("Year", F.year(F.col(ts_col)))
          .withColumn("Quarter", F.quarter(F.col(ts_col)))
          .withColumn("Month", F.month(F.col(ts_col)))
          .withColumn("Week", F.weekofyear(F.col(ts_col)))
          .withColumn("Day", F.dayofmonth(F.col(ts_col)))
          .withColumn("YearMonth", F.date_format(F.col(ts_col), "yyyy-MM"))
          .withColumn(
              "YearWeek",
              F.concat_ws("-", F.year(F.col(ts_col)), F.format_string("%02d", F.weekofyear(F.col(ts_col))))
          )
    )


def generate_calendar_table(
    spark: SparkSession,
    start_date: str,
    end_date: str,
    freq: str = "day"
) -> DataFrame:
    """
    Generate a reusable calendar dimension table.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    start_date : str
        Start date in 'YYYY-MM-DD'.
    end_date : str
        End date in 'YYYY-MM-DD'.
    freq : str, default="day"
        Frequency: "day", "hour", "minute".

    Returns
    -------
    DataFrame
        Calendar dimension with common attributes.

    Examples
    --------
    >>> from pyspark.sql import SparkSession
    >>> from odibi_de_v2.transformer.spark.spark_time_series import generate_calendar_table
    >>> spark = SparkSession.builder.getOrCreate()
    >>> calendar_df = generate_calendar_table(
    ...     spark, start_date="2024-01-01", end_date="2024-01-05", freq="day"
    ... )
    >>> calendar_df.show()
    """
    if freq not in ["day", "hour", "minute"]:
        raise ValueError("freq must be 'day', 'hour', or 'minute'")

    df = spark.sql(f"""
        SELECT sequence(
            timestamp('{start_date}'),
            timestamp('{end_date}'),
            interval 1 {freq}
        ) AS ts_seq
    """).withColumn("Timestamp", F.explode(F.col("ts_seq"))).drop("ts_seq")

    df = (
        df.withColumn("Date", F.to_date("Timestamp"))
          .withColumn("Year", F.year("Timestamp"))
          .withColumn("Quarter", F.quarter("Timestamp"))
          .withColumn("Month", F.month("Timestamp"))
          .withColumn("Week", F.weekofyear("Timestamp"))
          .withColumn("Day", F.dayofmonth("Timestamp"))
          .withColumn("Hour", F.hour("Timestamp"))
          .withColumn("Minute", F.minute("Timestamp"))
          .withColumn("DayOfWeek", F.dayofweek("Timestamp"))
          .withColumn("DayOfWeekISO", ((F.dayofweek("Timestamp")+5)%7)+1)
          .withColumn("DayName", F.date_format("Timestamp", "E"))
          .withColumn("MonthName", F.date_format("Timestamp", "MMMM"))
          .withColumn("YearMonth", F.date_format("Timestamp", "yyyy-MM"))
          .withColumn("YearWeek", F.concat_ws("-", F.year("Timestamp"), F.format_string("%02d", F.weekofyear("Timestamp"))))
          # Period boundaries
          .withColumn("IsMonthStart", F.expr("day(Timestamp) = 1"))
          .withColumn("IsMonthEnd", F.expr("last_day(Timestamp) = to_date(Timestamp)"))
          .withColumn("IsQuarterStart", F.expr("month(Timestamp) in (1,4,7,10) and day(Timestamp)=1"))
          .withColumn("IsQuarterEnd", F.expr("to_date(Timestamp) = last_day(add_months(Timestamp, 3 - ((month(Timestamp)-1) % 3)))"))
          .withColumn("IsYearStart", F.expr("month(Timestamp)=1 and day(Timestamp)=1"))
          .withColumn("IsYearEnd", F.expr("month(Timestamp)=12 and day(Timestamp)=31"))
    )

    return df
