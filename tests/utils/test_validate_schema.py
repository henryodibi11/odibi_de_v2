import pytest
import pandas as pd
from odibi_de_v2.utils.decorators.validate_schema import validate_schema


@validate_schema(["id", "name"])
def process_data(df: pd.DataFrame):
    return df[df["id"] > 0]


def test_validate_schema_pass():
    df = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
    result = process_data(df)
    assert isinstance(result, pd.DataFrame)
    assert "id" in result.columns


def test_validate_schema_missing_column():
    df = pd.DataFrame({"name": ["X", "Y"]})  # missing 'id'
    with pytest.raises(ValueError):
        process_data(df)


def test_validate_schema_wrong_type():
    not_df = {"id": [1, 2], "name": ["A", "B"]}
    with pytest.raises(ValueError):
        process_data(not_df)
