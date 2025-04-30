import pandas as pd
import pytest
from pandas_utils.datetime_utils import convert_to_datetime, extract_date_parts


@pytest.fixture
def sample_df():
   return pd.DataFrame({
       "event_time": ["2024-01-01", "2024-05-15", "invalid-date"]
   })


def test_convert_to_datetime(sample_df):
   df = convert_to_datetime(sample_df, "event_time")
   assert pd.api.types.is_datetime64_any_dtype(df["event_time"])
   assert df["event_time"].isna().sum() == 1


def test_extract_date_parts(sample_df):
   df = convert_to_datetime(sample_df, "event_time")
   df = extract_date_parts(df, "event_time")
   assert "year" in df.columns
   assert "month" in df.columns
   assert "day" in df.columns
   assert df.loc[0, "year"] == 2024
   assert pd.isna(df.loc[2, "year"])