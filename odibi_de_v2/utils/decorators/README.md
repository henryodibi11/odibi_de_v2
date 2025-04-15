# utils.decorators Module
The `utils.decorators/` module provides reusable decorators for validation, logging, and runtime enforcement in the `odibi_de_v2` framework.

---
## Modules
| Module                      | Description                                                                                    |
|-----------------------------|------------------------------------------------------------------------------------------------|
| `enforce_types.py`          | Validates that function arguments match the type annotations using `isinstance`.               |
| `validate_core_contracts.py`| String and column name transformations, normalization, and cleaning                            |
| `validate_non_empty.py`     | Checks that the given parameters are not `None`, empty strings, empty lists/dicts/sets.        |
| `log_call.py`               | Logs the function call and all arguments using the `log_info` helper.                          |
| `benchmark.py`              | Logs the runtime (in seconds) of the decorated function.                                       |
| `ensure_output_type.py`     | Validates that the return type of the function matches `expected_type`.                        |
| `validate_schema.py`        | Checks that a DataFrame has all the required columns.                                          |
---

## Available Decorators

### 1. `@enforce_types(strict=True)`
Validates that function arguments match the type annotations using `isinstance`.

- `strict=True`: raises `TypeError`
- `strict=False`: logs a warning but proceeds

**Example:**
```python
@enforce_types(strict=True)
def run_pipeline(site: str, config: dict): ...
```

---

### 2. `@validate_core_contracts({...}, allow_none=True)`
Ensures that parameters inherit from or are instances of required base classes.

**Example:**
```python
@validate_core_contracts({"connector": CloudConnector, "factory": ReaderFactory})
def __init__(self, connector, factory): ...
```

---

### 3. `@validate_non_empty(["param1", "param2"])`
Checks that the given parameters are not `None`, empty strings, empty lists/dicts/sets.

**Example:**
```python
@validate_non_empty(["path", "columns"])
def load_data(path: str, columns: list): ...
```

---

### 4. `@log_call(module="INGESTION", component="reader")`
Logs the function call and all arguments using the `log_info` helper.

**Example:**
```python
@log_call(module="INGESTION", component="reader")
def read_data(path: str): ...
```

---

### 5. `@benchmark(module="TRANSFORM", component="stage")`
Logs the runtime (in seconds) of the decorated function.

**Example:**
```python
@benchmark(module="TRANSFORM", component="stage")
def transform(df): ...
```

---

### 6. `@ensure_output_type(expected_type)`
Validates that the return type of the function matches `expected_type`.

**Example:**
```python
@ensure_output_type(pd.DataFrame)
def get_data(): ...
```

---

### 7. `@validate_schema(columns, param_name="df")`
Checks that a DataFrame has all the required columns.

- `param_name` specifies which parameter holds the DataFrame (default is "df")

**Example:**
```python
@validate_schema(["id", "plant", "timestamp"])
def clean_data(df: pd.DataFrame): ...
```

---

## Usage Tip
You can safely chain these decorators together in this order:

```python
@validate_core_contracts({...})
@enforce_types(strict=True)
@validate_non_empty([...])
@validate_schema([...])
@ensure_output_type(pd.DataFrame)
@benchmark(...)
@log_call(...)
def my_function(...):
    ...
```

---

## Logging
All decorators use the shared `log_exceptions` and `log_info` patterns to ensure structured, readable, and consistent logs across the entire framework.
