import pytest
from pyspark.sql import SparkSession
from spark_utils.validation import (
    is_spark_dataframe,
    has_columns,
    is_empty,
    has_nulls_in_columns,
    has_duplicate_columns,
    is_flat_dataframe
)


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master(
        "local[1]").appName("TestSparkValidation").getOrCreate()


def test_is_spark_dataframe(spark):
    df = spark.createDataFrame([(1, "A")], ["id", "name"])
    assert is_spark_dataframe(df) is True
    assert is_spark_dataframe("not_a_df") is False


def test_has_columns(spark):
    df = spark.createDataFrame([(1, "A")], ["id", "name"])
    assert has_columns(df, ["id"]) is True
    assert has_columns(df, ["id", "name"]) is True
    assert has_columns(df, ["missing"]) is False


def test_is_empty(spark):
    df_empty = spark.createDataFrame([], "id INT, name STRING")
    df_filled = spark.createDataFrame([(1, "A")], ["id", "name"])
    assert is_empty(df_empty) is True
    assert is_empty(df_filled) is False


def test_has_nulls_in_columns(spark):
    df = spark.createDataFrame([(1, "A"), (2, None)], ["id", "name"])
    assert has_nulls_in_columns(df, ["name"]) is True
    assert has_nulls_in_columns(df, ["id"]) is False
    assert has_nulls_in_columns(df, ["nonexistent"]) is False


def test_has_duplicate_columns(spark):
    df = spark.createDataFrame([(1, 2)], ["a", "a"])
    assert has_duplicate_columns(df) is True

    df_clean = spark.createDataFrame([(1, 2)], ["x", "y"])
    assert has_duplicate_columns(df_clean) is False


def test_is_flat_dataframe(spark):
    flat_df = spark.createDataFrame([(1, "A")], ["id", "name"])
    nested_df = spark.read.json(spark.sparkContext.parallelize(
        ['{"id": 1, "info": {"score": 10}}']))

    assert is_flat_dataframe(flat_df) is True
    assert is_flat_dataframe(nested_df) is False
