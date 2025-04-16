import pytest
import pandas as pd
from pathlib import Path
from odibi_de_v2.core.enums import DataType
from odibi_de_v2.ingestion import PandasDataReader

TEST_DIR = Path(__file__).parent / "data"

@pytest.fixture(scope="module")
def reader():
    return PandasDataReader()

def test_read_csv(reader):
    path = str(TEST_DIR / "sample.csv")
    df = reader.read_data(data_type=DataType.CSV, file_path=path)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty

def test_read_json(reader):
    path = str(TEST_DIR / "sample.json")
    df = reader.read_data(data_type=DataType.JSON, file_path=path)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty

def test_read_parquet(reader):
    path = str(TEST_DIR / "sample.parquet")
    df = reader.read_data(data_type=DataType.PARQUET, file_path=path)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty

def test_read_avro(reader):
    path = str(TEST_DIR / "sample.avro")
    df = reader.read_data(
        data_type=DataType.AVRO,
        file_path=path
    )
    assert isinstance(df, pd.DataFrame)
    assert not df.empty

# def test_unsupported_type(reader):
#     with pytest.raises(NotImplementedError):
#         reader.read_data(data_type="xml", file_path="dummy.xml")

# def test_file_not_found(reader):
#     with pytest.raises(FileNotFoundError):
#         reader.read_data(data_type=DataType.CSV, file_path="not_found.csv")
