import pytest
from pyspark.sql import SparkSession
from pathlib import Path
from odibi_de_v2.core.enums import DataType
from odibi_de_v2.storage.spark.spark_data_saver import SparkDataSaver

TEST_DIR = Path("/tmp/odibi_tests/spark_saver")
TEST_DIR.mkdir(parents=True, exist_ok=True)

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("SparkSaverTest").getOrCreate()

@pytest.fixture
def sample_df(spark):
    return spark.createDataFrame([
        (1, "Alice"),
        (2, "Bob")
    ], ["id", "name"])

@pytest.fixture
def saver():
    return SparkDataSaver()

def test_save_parquet(saver, sample_df):
    path = str(TEST_DIR / "test_output.parquet")
    saver.save_data(
        df=sample_df,
        data_type=DataType.PARQUET,
        file_path=path,
        mode="overwrite"
    )
    assert Path(path).exists()

def test_save_json(saver, sample_df):
    path = str(TEST_DIR / "test_output.json")
    saver.save_data(
        df=sample_df,
        data_type=DataType.JSON,
        file_path=path,
        mode="overwrite"
    )
    assert Path(path).exists()

def test_save_csv(saver, sample_df):
    path = str(TEST_DIR / "test_output.csv")
    saver.save_data(
        df=sample_df,
        data_type=DataType.CSV,
        file_path=path,
        mode="overwrite",
        option={"header": "true"}
    )
    assert Path(path).exists()

def test_invalid_format(saver, sample_df):
    with pytest.raises(Exception):
        saver.save_data(
            df=sample_df,
            data_type="xml",
            file_path="/tmp/invalid.xml"
        )
