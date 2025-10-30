"""Comprehensive test of the ODIBI function framework."""

import sys
sys.path.insert(0, r'd:\projects\odibi_de_v2')

import pandas as pd
from odibi_de_v2.odibi_functions import (
    REGISTRY,
    odibi_function,
    spark_function,
    pandas_function,
    universal_function,
    with_context,
    ExecutionContext,
    set_current_context,
    get_current_context,
    clear_context,
)

print("=" * 60)
print("ODIBI Function Framework - Comprehensive Test")
print("=" * 60)

# Clear registry for clean test
REGISTRY.clear()

# Test 1: Basic Registration
print("\n[Test 1] Basic Registration")
@pandas_function(module="test", version="1.0")
def test_basic(df):
    """Basic test function."""
    return df

fn = REGISTRY.resolve(None, "test_basic", "pandas")
assert fn is not None, "Function not registered"
print("  [OK] Function registered and resolved")

# Test 2: Metadata
print("\n[Test 2] Metadata Retrieval")
meta = REGISTRY.get_metadata("test_basic", "pandas")
assert meta['module'] == "test", "Metadata incorrect"
assert meta['version'] == "1.0", "Version incorrect"
print(f"  [OK] Metadata: module={meta['module']}, version={meta['version']}")

# Test 3: Universal Fallback
print("\n[Test 3] Universal Fallback")
@universal_function(module="test")
def universal_test(df):
    """Universal function."""
    return list(df.columns)

spark_fn = REGISTRY.resolve(None, "universal_test", "spark")
pandas_fn = REGISTRY.resolve(None, "universal_test", "pandas")
assert spark_fn is not None, "Spark fallback failed"
assert pandas_fn is not None, "Pandas fallback failed"
assert spark_fn is pandas_fn, "Should be same function"
print("  [OK] Universal fallback works for both engines")

# Test 4: Engine-Specific Override
print("\n[Test 4] Engine-Specific Override")
@universal_function(name="count_rows", module="test")
def count_universal(df):
    return len(df)

@spark_function(name="count_rows", module="test")
def count_spark(df):
    return "SPARK_COUNT"

spark_fn = REGISTRY.resolve(None, "count_rows", "spark")
pandas_fn = REGISTRY.resolve(None, "count_rows", "pandas")
assert spark_fn is not pandas_fn, "Should be different functions"
print("  [OK] Engine-specific override works")

# Test 5: Context Injection
print("\n[Test 5] Context Injection")
@pandas_function(module="test")
@with_context()
def context_aware(df, context=None):
    """Context-aware function."""
    if context:
        return context.test_value
    return None

ctx = ExecutionContext(test_value="SUCCESS")
set_current_context(ctx)

# Create test dataframe
df = pd.DataFrame({'a': [1, 2, 3]})
result = context_aware(df)
assert result == "SUCCESS", "Context not injected"
print("  [OK] Context injection works")

# Test 6: Direct Function Call (Backward Compatibility)
print("\n[Test 6] Backward Compatibility")
@pandas_function(module="test")
def backward_compat(df):
    """Test backward compatibility."""
    return df.copy()

df_test = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
result = backward_compat(df_test)
assert isinstance(result, pd.DataFrame), "Function didn't work normally"
assert len(result) == 2, "Wrong result"
print("  [OK] Functions work normally without registry")

# Test 7: List Functions
print("\n[Test 7] Registry Operations")
all_funcs = REGISTRY.get_all()
pandas_funcs = REGISTRY.get_all(engine="pandas")
spark_funcs = REGISTRY.get_all(engine="spark")
any_funcs = REGISTRY.get_all(engine="any")

print(f"  Total functions: {len(all_funcs)}")
print(f"  Pandas functions: {len(pandas_funcs)}")
print(f"  Spark functions: {len(spark_funcs)}")
print(f"  Universal functions: {len(any_funcs)}")
assert len(all_funcs) > 0, "No functions registered"
print("  [OK] Registry listing works")

# Test 8: Unregister
print("\n[Test 8] Unregister Function")
@pandas_function(module="test")
def temp_function(df):
    return df

assert REGISTRY.resolve(None, "temp_function", "pandas") is not None
success = REGISTRY.unregister("temp_function", "pandas")
assert success, "Unregister failed"
assert REGISTRY.resolve(None, "temp_function", "pandas") is None
print("  [OK] Unregister works")

# Test 9: Convenience Decorators
print("\n[Test 9] Convenience Decorators")
@spark_function(module="convenience")
def spark_conv(df):
    return df

@pandas_function(module="convenience")
def pandas_conv(df):
    return df

assert REGISTRY.resolve(None, "spark_conv", "spark") is not None
assert REGISTRY.resolve(None, "pandas_conv", "pandas") is not None
print("  [OK] Convenience decorators work")

# Test 10: Real-World Example
print("\n[Test 10] Real-World Example")
@pandas_function(
    module="cleaning",
    description="Remove null rows",
    author="test_suite",
    tags=["cleaning", "quality"]
)
def remove_nulls(df):
    """Remove rows with any null values."""
    return df.dropna()

df_with_nulls = pd.DataFrame({
    'a': [1, 2, None, 4],
    'b': [5, None, 7, 8]
})

clean_df = remove_nulls(df_with_nulls)
assert len(clean_df) == 2, "Nulls not removed correctly"

# Via registry
fn = REGISTRY.resolve(None, "remove_nulls", "pandas")
result = fn(df_with_nulls)
assert len(result) == 2, "Registry resolution failed"
print("  [OK] Real-world function works")

# Summary
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"Total functions in registry: {len(REGISTRY.get_all())}")
print(f"Pandas functions: {len(REGISTRY.get_all(engine='pandas'))}")
print(f"Spark functions: {len(REGISTRY.get_all(engine='spark'))}")
print(f"Universal functions: {len(REGISTRY.get_all(engine='any'))}")
print("\n[SUCCESS] All 10 tests passed!")
print("=" * 60)

# Cleanup
clear_context()
REGISTRY.clear()
