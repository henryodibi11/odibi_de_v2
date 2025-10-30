"""Test the example functions from odibi_functions.examples module."""

import sys
sys.path.insert(0, r'd:\projects\odibi_de_v2')

import pandas as pd
from odibi_de_v2.odibi_functions import REGISTRY
from odibi_de_v2.odibi_functions import examples

print("=" * 60)
print("Testing ODIBI Function Examples")
print("=" * 60)

# Test 1: deduplicate_pandas
print("\n[Test 1] deduplicate_pandas")
df = pd.DataFrame({'id': [1, 1, 2, 3], 'val': ['a', 'a', 'b', 'c']})
result = examples.deduplicate_pandas(df)
assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
print(f"  [OK] Deduplicated {len(df)} rows to {len(result)} rows")

# Test 2: get_columns
print("\n[Test 2] get_columns (universal)")
df = pd.DataFrame({'a': [1], 'b': [2], 'c': [3]})
cols = examples.get_columns(df)
assert cols == ['a', 'b', 'c'], f"Expected ['a', 'b', 'c'], got {cols}"
print(f"  [OK] Got columns: {cols}")

# Test 3: validate_dataframe_schema
print("\n[Test 3] validate_dataframe_schema")
df = pd.DataFrame({'id': [1, 2], 'name': ['a', 'b'], 'value': [10, 20]})
is_valid = examples.validate_dataframe_schema(df, ['id', 'name'], raise_on_error=False)
assert is_valid, "Valid schema marked as invalid"
is_invalid = examples.validate_dataframe_schema(df, ['id', 'missing'], raise_on_error=False)
assert not is_invalid, "Invalid schema marked as valid"
print("  [OK] Schema validation works")

# Test 4: fill_nulls
print("\n[Test 4] fill_nulls")
import numpy as np
df = pd.DataFrame({'a': [1, 2, np.nan, 4], 'b': [10, np.nan, 30, 40]})
result = examples.fill_nulls(df, strategy="zero")
assert result['a'].isnull().sum() == 0, "Nulls not filled"
assert result.loc[2, 'a'] == 0, "Should be filled with zero"
print("  [OK] Null filling works")

# Test 5: Verify registry entries
print("\n[Test 5] Registry Verification")
all_examples = [
    ('deduplicate_pandas', 'pandas'),
    ('deduplicate_spark', 'spark'),
    ('get_columns', 'any'),
    ('load_table', 'spark'),
    ('validate_schema', 'pandas'),
    ('fill_nulls', 'pandas'),
]

found = 0
for name, engine in all_examples:
    fn = REGISTRY.resolve(None, name, engine)
    if fn is not None:
        found += 1
        print(f"  [OK] {name} ({engine}) registered")
    else:
        print(f"  [WARN] {name} ({engine}) not found")

print(f"\n  Found {found}/{len(all_examples)} example functions")

# Test 6: Metadata check
print("\n[Test 6] Metadata Check")
meta = REGISTRY.get_metadata('fill_nulls', 'pandas')
if meta:
    print(f"  Module: {meta.get('module')}")
    print(f"  Author: {meta.get('author')}")
    print(f"  Version: {meta.get('version')}")
    print("  [OK] Metadata present")

print("\n" + "=" * 60)
print("[SUCCESS] All example tests passed!")
print("=" * 60)
