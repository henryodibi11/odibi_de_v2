from utils.validation_utils import (
    validate_required_keys,
    validate_columns_exist,
    validate_supported_format
)


def test_validate_required_keys():
    data = {"id": 1, "name": "test", "value": 42}
    assert validate_required_keys(data, ["id", "name"]) is True
    assert validate_required_keys(data, ["id", "missing"]) is False
    assert validate_required_keys({}, ["a"]) is False
    assert validate_required_keys({"a": 1, "b": 2}, []) is True


def test_validate_columns_exist_with_columns_attr():
    class DummyDataFrame:
        columns = ["id", "name", "value"]

    df = DummyDataFrame()
    assert validate_columns_exist(df, ["id"]) is True
    assert validate_columns_exist(df, ["id", "value"]) is True
    assert validate_columns_exist(df, ["missing"]) is False


def test_validate_columns_exist_with_schema_names_attr():
    class DummySchema:
        names = ["id", "metric", "timestamp"]

    class DummySparkDF:
        schema = DummySchema()

    df = DummySparkDF()
    assert validate_columns_exist(df, ["id", "timestamp"]) is True
    assert validate_columns_exist(df, ["id", "missing"]) is False


def test_validate_supported_format():
    assert validate_supported_format("data/file.csv", ["csv", "json"]) is True
    assert (
        validate_supported_format("logs/file.parquet", ["csv", "json"])
        ) is False
    assert (
        validate_supported_format("folder/file.JSON", ["csv", "json"])
        ) is True
    assert validate_supported_format("no_ext", ["csv"]) is False
