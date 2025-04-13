# Core Module
The `core` package defines the foundational abstract interfaces for the `odibi_de` framework.  
These classes provide standardized contracts for reading, saving, connecting to storage systems, and dispatching to appropriate concrete implementations (e.g., Pandas, Spark).
These are **not executable components** — they are **reusable base classes** for building modular, scalable data engineering solutions.
---
## Modules
| Module              | Description                                                                 |
|---------------------|-----------------------------------------------------------------------------|
| `reader.py`         | Abstract class `DataReader`, defines contract for all readers.              |
| `saver.py`          | Abstract class `DataSaver`, defines contract for all savers.                |
| `reader_factory.py` | Factory interface for reader creation (`CSV`, `JSON`, etc).                 |
| `saver_factory.py`  | Factory interface for saver creation.                                       |
| `cloud_connector.py`| Contract for connectors to Azure, AWS, etc.                                 |
| `reader_provider.py`| Uses factory + connector to generate ready-to-use readers.                  |
| `saver_provider.py` | Uses factory + connector to generate ready-to-use savers.                   |
| `enums.py`          | Enum for `DataType`, used throughout the framework.                         |
---
## Example Usage: Reader    
```python
# This is just the base class; actual usage requires a concrete implementation.
# Example below shows how a concrete reader might be constructed.

    import pandas as pd
    class CSVReader(DataReader):
        def read_data(self, **kwargs):
        return pd.read_csv(self.file_path)

    reader = CSVReader("example.csv")
    df = reader.read_sample_data(n=10)

```
## Example Usage: Saver
```python
# This is just the base class; actual usage requires a concrete implementation.
# Example below shows how a concrete reader might be constructed.

    import pandas as pd
    class CSVSaver(DataSaver):
        def save_data(self, data, **kwargs):
        return pd.to_csv(self.file_path, **kwargs)

    saver = CSVSaver("example.csv")
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    saver.save_data(df, index=False)
```