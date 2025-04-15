import pandas as pd
import pytest
from odibi_de_v2.pandas_utils.datetime_utils import convert_to_datetime, extract_date_parts
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth


def convert_to_datetime(df: DataFrame, column_name: str) -> DataFrame:
   """
   Convert the specified column to timestamp format in Spark.

   Args:
       df (DataFrame): Input Spark DataFrame.
       column_name (str): Name of the column to convert.

   Returns:
       DataFrame: DataFrame with column cast to timestamp.
   """
   return df.withColumn(column_name, to_timestamp(col(column_name)))


def extract_date_parts(df: DataFrame, column_name: str) -> DataFrame:
   """
   Extract year, month, and day from a timestamp column in Spark.

   Args:
       df (DataFrame): Input Spark DataFrame.
       column_name (str): Name of the timestamp column.

   Returns:
       DataFrame: DataFrame with new columns for year, month, and day.
   """
   return (
       df.withColumn("year", year(col(column_name)))
         .withColumn("month", month(col(column_name)))
         .withColumn("day", dayofmonth(col(column_name)))
   ) 
