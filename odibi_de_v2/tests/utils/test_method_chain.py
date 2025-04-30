import pandas as pd
import pytest
from utils.method_chain import run_method_chain, apply_dynamic_method


@pytest.fixture
def sample_pandas_df():
   return pd.DataFrame({
       "name": ["Alice", "Bob", None],
       "score": [95, 85, None]
   })


def test_run_chain_valid_kwargs(sample_pandas_df):
   methods = {
       "fillna": {"value": {"name":"unknown", "score": 0}}}
   result = run_method_chain(sample_pandas_df, methods, validate=True)
   assert "unknown" in result["name"].values
   assert result.isnull().sum().sum() == 0


def test_run_chain_invalid_kwargs(sample_pandas_df):
   methods = {
       "dropna": {"bad_param": True}
   }
   with pytest.raises(ValueError):
       run_method_chain(sample_pandas_df, methods, validate=True)


def test_run_chain_single_arg(sample_pandas_df):
   methods = {
       "head": 2
   }
   result = run_method_chain(sample_pandas_df, methods)
   assert len(result) == 2


def test_run_chain_list_args(sample_pandas_df):
   methods = {
       "filter": [["score > 90"]]  # pandas doesn't support this, expect skip or error
   }
   result = run_method_chain(sample_pandas_df, methods)
   assert isinstance(result, pd.DataFrame)


## apply dynamic methods

def sample_fn(a, b=0, c=0):
    return a + b + c


def test_apply_dynamic_method_with_kwargs():
    result = apply_dynamic_method(None, sample_fn, {"a": 1, "b": 2, "c": 3})
    assert result == 6


def test_apply_dynamic_method_with_args_list():
    result = apply_dynamic_method(None, sample_fn, [1, 2, 3])
    assert result == 6


def test_apply_dynamic_method_with_args_tuple():
    result = apply_dynamic_method(None, sample_fn, (1, 2, 3))
    assert result == 6


def test_apply_dynamic_method_with_single_arg():
    result = apply_dynamic_method(None, sample_fn, 5)
    assert result == 5
