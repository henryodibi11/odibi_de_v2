import pandas as pd
import pytest
from pathlib import Path
from odibi_de_v2.core.enums import DataType
from odibi_de_v2.storage import PandasDataSaver

TEST_DIR = Path("/tmp/odibi_tests/pandas_saver")
TEST_DIR.mkdir(parents=True, exist_ok=True)

@pytest.fixture
def sample_df():
    return pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

@pytest.fixture
def saver():
    return PandasDataSaver()

def test_save_csv(saver, sample_df):
    path = TEST_DIR / "test_output.csv"
    saver.save_data(sample_df, DataType.CSV, str(path), index=False)
    assert path.exists()

def test_save_json(saver, sample_df):
    path = TEST_DIR / "test_output.json"
    saver.save_data(sample_df, DataType.JSON, str(path), orient="records", lines=True)
    assert path.exists()

def test_save_parquet(saver, sample_df):
    path = TEST_DIR / "test_output.parquet"
    saver.save_data(sample_df, DataType.PARQUET, str(path))
    assert path.exists()

def test_save_excel(saver, sample_df):
    path = TEST_DIR / "test_output.xlsx"
    saver.save_data(sample_df, DataType.EXCEL, str(path), index=False)
    assert path.exists()

def test_save_avro(saver, sample_df):
    path = TEST_DIR / "test_output.avro"
    schema = {
        "name": "User",
        "type": "record",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"}
        ]
    }
    saver.save_data(sample_df, DataType.AVRO, str(path), schema=schema)
    assert path.exists()

def test_save_unsupported_format(saver, sample_df):
    with pytest.raises(NotImplementedError):
        saver.save_data(sample_df, "xml", "unsupported.xml")
