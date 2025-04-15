from utils.validation_utils import (
    validate_required_keys,
    validate_columns_exist,
    validate_supported_format,
    validate_method_exists,
    validate_kwargs_match_signature
)
import pytest

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


class DummyObject:
    def foo(self):
        pass

def test_validate_method_exists_when_method_is_present(capsys):
    obj = DummyObject()
    validate_method_exists(obj, "foo", context="test_context")
    captured = capsys.readouterr()
    assert captured.err == ""  # Nothing should be logged

def test_validate_method_exists_when_method_is_missing(capsys):
    obj = DummyObject()
    validate_method_exists(obj, "bar", context="test_context")
    captured = capsys.readouterr()
    assert "Method 'bar' not found" in captured.err
    assert "test_context" in captured.err


def dummy_method(x, y=1, z=None):
    return x + y


def test_validate_kwargs_match_signature_valid(capsys):
    kwargs = {"x": 10, "y": 5}
    # Should not raise or log anything
    validate_kwargs_match_signature(
        method=dummy_method,
        kwargs=kwargs,
        method_name="dummy_method",
        context="test_context",
        raise_error=True
    )
    captured = capsys.readouterr()
    assert captured.err == ""


def test_validate_kwargs_match_signature_invalid(capsys):
    kwargs = {"x": 10, "foo": 99}
    with pytest.raises(ValueError) as e:
        validate_kwargs_match_signature(
            method=dummy_method,
            kwargs=kwargs,
            method_name="dummy_method",
            context="test_context",
            raise_error=True
        )
    assert "Invalid kwargs" in str(e.value)
    captured = capsys.readouterr()
    assert "foo" in captured.err
    assert "dummy_method" in captured.err