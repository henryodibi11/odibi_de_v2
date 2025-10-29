import pandas as pd
import pytest
from pandas_utils.columns import has_columns, drop_columns, select_columns


@pytest.fixture
def sample_df():
   return pd.DataFrame({
       "name": ["Alice", "Bob"],
       "age": [25, 30],
       "city": ["New York", "Chicago"]
   })


def test_has_columns_true(sample_df):
   assert has_columns(sample_df, ["name", "age"]) is True


def test_has_columns_false(sample_df):
   assert has_columns(sample_df, ["name", "salary"]) is False


def test_drop_columns_existing(sample_df):
   result = drop_columns(sample_df, ["city"])
   assert "city" not in result.columns
   assert result.shape[1] == 2


def test_drop_columns_non_existing(sample_df):
   result = drop_columns(sample_df, ["country"])
   assert result.equals(sample_df)


def test_select_columns_existing(sample_df):
   result = select_columns(sample_df, ["name", "age"])
   assert list(result.columns) == ["name", "age"]


def test_select_columns_partial(sample_df):
   result = select_columns(sample_df, ["name", "country"])
   assert list(result.columns) == ["name"]