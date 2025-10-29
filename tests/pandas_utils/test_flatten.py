import pandas as pd
from pandas_utils.flatten import flatten_json_columns


def test_flatten_json_columns_single():
   df = pd.DataFrame({
       "id": [1, 2],
       "metadata": [
           {"source": "sensor", "version": 3},
           {"source": "manual", "version": 2}
       ]
   })

   flat_df = flatten_json_columns(df)

   assert "metadata_source" in flat_df.columns
   assert "metadata_version" in flat_df.columns
   assert "metadata" not in flat_df.columns
   assert flat_df.shape[1] == 3


def test_flatten_json_columns_multi():
   df = pd.DataFrame({
       "id": [1],
       "meta": [{"a": 1, "b": {"x": 10}}],
       "info": [{"tag": "xyz"}]
   })

   flat_df = flatten_json_columns(df, json_columns=["meta", "info"])
   assert "meta_a" in flat_df.columns
   assert "meta_b.x" not in flat_df.columns  # only one level deep
   assert "meta_b_x" in flat_df.columns or "meta_b" in flat_df.columns  # depending on auto behavior
   assert "info_tag" in flat_df.columns 
