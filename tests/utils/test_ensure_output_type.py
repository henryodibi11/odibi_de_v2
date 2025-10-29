import pytest
import pandas as pd
from odibi_de_v2.utils.decorators.ensure_output_type import ensure_output_type


@ensure_output_type(pd.DataFrame)
def return_dataframe():
    return pd.DataFrame({"a": [1, 2]})


@ensure_output_type(pd.DataFrame)
def return_wrong_type():
    return {"a": [1, 2]}


def test_ensure_output_type_pass():
    df = return_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert "a" in df.columns


# def test_ensure_output_type_fail():
#     with pytest.raises(TypeError):
#         return_wrong_type()
