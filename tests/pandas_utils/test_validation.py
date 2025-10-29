import pandas as pd

from pandas_utils.validation import (
    is_pandas_dataframe,
    has_columns,
    is_list_of_strings,
    is_empty,
    has_nulls_in_columns,
    has_duplicate_columns,
    is_flat_dataframe
)


def test_is_pandas_dataframe():
    assert is_pandas_dataframe(pd.DataFrame()) is True
    assert is_pandas_dataframe([]) is False
    assert is_pandas_dataframe({"a": 1}) is False


def test_has_columns():
    df = pd.DataFrame({"id": [1], "name": ["Alice"]})
    assert has_columns(df, ["id"]) is True
    assert has_columns(df, ["id", "name"]) is True
    assert has_columns(df, ["missing"]) is False


def test_is_list_of_strings():
    assert is_list_of_strings(["a", "b", "c"]) is True
    assert is_list_of_strings(["a", 1, "b"]) is False
    assert is_list_of_strings("a,b,c") is False
    assert is_list_of_strings([]) is True


def test_is_empty():
    assert is_empty(pd.DataFrame()) is True
    assert is_empty(pd.DataFrame({"a": [1]})) is False


def test_has_nulls_in_columns():
    df = pd.DataFrame({"id": [1, 2, None], "name": ["A", None, "C"]})
    assert has_nulls_in_columns(df, ["id"]) is True
    assert has_nulls_in_columns(df, ["name"]) is True
    assert has_nulls_in_columns(df, ["id", "name"]) is True
    assert has_nulls_in_columns(df, ["nonexistent"]) is False


def test_has_duplicate_columns():
    df = pd.DataFrame([[1, 1]], columns=["a", "a"])
    assert has_duplicate_columns(df) is True
    df = pd.DataFrame({"a": [1], "b": [2]})
    assert has_duplicate_columns(df) is False


def test_is_flat_dataframe():
    df_flat = pd.DataFrame({"id": [1], "name": ["Alice"]})
    assert is_flat_dataframe(df_flat) is True
    df_nested = pd.DataFrame({"data": [{"a": 1}], "list_col": [[1, 2]]})
    assert is_flat_dataframe(df_nested) is False
