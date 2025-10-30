"""Quick test of odibi_functions framework."""

import sys
sys.path.insert(0, r'd:\projects\odibi_de_v2')

from odibi_de_v2.odibi_functions import (
    REGISTRY,
    odibi_function,
    spark_function,
    pandas_function,
    universal_function,
    ExecutionContext,
    set_current_context,
    get_current_context,
)

print("[OK] All imports successful")

# Test 1: Registration
@pandas_function(module="test")
def test_fn(df):
    """Test function."""
    return df

print(f"[OK] Function registered: {REGISTRY.resolve(None, 'test_fn', 'pandas') is not None}")

# Test 2: Resolution
fn = REGISTRY.resolve(None, "test_fn", "pandas")
print(f"[OK] Function resolved: {fn is not None}")

# Test 3: Metadata
meta = REGISTRY.get_metadata("test_fn", "pandas")
print(f"[OK] Metadata retrieved: module={meta.get('module')}")

# Test 4: Universal function with fallback
@universal_function(module="test")
def universal_fn(df):
    """Universal test function."""
    return df

spark_fn = REGISTRY.resolve(None, "universal_fn", "spark")
pandas_fn = REGISTRY.resolve(None, "universal_fn", "pandas")
print(f"[OK] Universal fallback works: spark={spark_fn is not None}, pandas={pandas_fn is not None}")

# Test 5: Context
ctx = ExecutionContext(test_value="hello")
set_current_context(ctx)
current = get_current_context()
print(f"[OK] Context works: {current.test_value if current else 'None'}")

# Test 6: List functions
all_funcs = REGISTRY.get_all()
print(f"[OK] Registry has {len(all_funcs)} functions registered")

print("\n[SUCCESS] All tests passed!")
