import pandas as pd
import pytest
from odibi_de_v2.utils import validate_input_types

@validate_input_types({"df": pd.DataFrame, "file_path": str})
def save_data(df, file_path, mode="overwrite"):
    return True

def test_validate_passes_with_correct_types():
    df = pd.DataFrame({"id": [1, 2]})
    assert save_data(df, "output.csv") is True

def test_validate_fails_with_wrong_df_type():
    with pytest.raises(ValueError, match="Argument 'df' must be of type DataFrame, but got list"):
        save_data(["not", "a", "df"], "output.csv")

def test_validate_fails_with_wrong_path_type():
    df = pd.DataFrame({"id": [1, 2]})
    with pytest.raises(ValueError, match="Argument 'file_path' must be of type str"):
        save_data(df, 123)

def test_validate_works_with_kwargs():
    df = pd.DataFrame({"id": [1]})
    assert save_data(df=df, file_path="ok.csv", mode="append") is True

def test_validate_allows_optional_params():
    @validate_input_types({"df": pd.DataFrame})
    def func(df, extra=None):
        return "ok"
    df = pd.DataFrame()
    assert func(df) == "ok"
