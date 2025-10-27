import pandas as pd
import json


class IngestionConfigConstructor:
    """
    Converts metadata DataFrames (from SQL config tables) into clean Python dictionaries.

    This class prepares **source** and **target** configuration dictionaries for ingestion and saving
    by parsing JSON strings, validating required fields, and returning Spark-ready Python dicts.

    ---
    🧩 **Quick Copy-Paste Example**

    ```python
    import pandas as pd, json
    from odibi_de_v2.ingestion.config_constructor import IngestionConfigConstructor

    # --- 1️⃣ Create sample source & target DataFrames (mimics SQL rows) ---
    source_df = pd.DataFrame([{
        "source_name": "calendar_data",
        "source_type": "local",
        "source_path_or_query": "FileStore/Calendar.json",
        "source_options": json.dumps({
            "databricks": {"batch": {"format": "json", "options": {"header": "true"}}}
        }),
        "connection_config": json.dumps({
            "storage_unit": "", "object_name": "FileStore/Calendar.json"
        })
    }])

    target_df = pd.DataFrame([{
        "target_name": "calendar_output",
        "target_type": "sql",
        "write_mode": "overwrite",
        "target_options": json.dumps({
            "databricks": {"batch": {"format": "delta", "mode": "overwrite"}}
        }),
        "connection_config": json.dumps({
            "database": "goat_dev", "table": "calendar_silver"
        }),
        "merge_config": json.dumps({"key": "Time_Stamp"})
    }])

    # --- 2️⃣ Construct and prepare configs ---
    constructor = IngestionConfigConstructor(source_df, target_df)
    source_cfg, target_cfg = constructor.prepare()

    # --- 3️⃣ Inspect results ---
    print(json.dumps(source_cfg, indent=2))
    print(json.dumps(target_cfg, indent=2))
    ```

    **Output:**
    ```json
    {
      "source_type": "local",
      "source_path_or_query": "FileStore/Calendar.json",
      "source_options": {
        "databricks": {"batch": {"format": "json", "options": {"header": "true"}}}
      },
      "connection_config": {"storage_unit": "", "object_name": "FileStore/Calendar.json"}
    }
    ```

    ---
    ⚙️ **Internal Flow**
    1️⃣ `_prepare_source()` and `_prepare_target()` extract the first row from each DataFrame.  
    2️⃣ `_parse_json_fields()` safely converts JSON strings into Python dicts.  
    3️⃣ `_validate_required_fields()` ensures keys like `source_type` and `write_mode` exist.  
    4️⃣ Returns ready-to-use Python dictionaries for Spark ingestion.

    ---
    🔗 **Typical Downstream Use**
    ```python
    reader = SparkDataReaderFromConfig(spark, source_cfg)
    df = reader.read_data()

    saver = SparkDataSaverFromConfig(df, target_cfg)
    saver.write_data()
    ```
    ---
    """

    def __init__(self, sources_df: pd.DataFrame, targets_df: pd.DataFrame):
        """Stores DataFrames for source and target configurations."""
        self.sources_df = sources_df
        self.targets_df = targets_df

    def prepare(self) -> tuple[dict, dict]:
        """Parses JSON and validates both source and target configurations."""
        source_config = self._prepare_source()
        target_config = self._prepare_target()
        return source_config, target_config

    def _prepare_source(self) -> dict:
        """Converts the first source row to a validated Python dictionary."""
        row = self.sources_df.to_dict("records")[0]
        return self._prepare_row(
            row,
            json_fields=["source_options", "connection_config"],
            required_fields=["source_type", "source_path_or_query"]
        )

    def _prepare_target(self) -> dict:
        """Converts the first target row to a validated Python dictionary."""
        row = self.targets_df.to_dict("records")[0]
        return self._prepare_row(
            row,
            json_fields=["target_options", "connection_config", "merge_config"],
            required_fields=["write_mode", "target_type"]
        )

    def _prepare_row(self, row: dict, json_fields: list, required_fields: list) -> dict:
        """Parses JSON fields and validates required fields."""
        row = self._parse_json_fields(row, json_fields)
        self._validate_required_fields(row, required_fields)
        return row

    @staticmethod
    def _parse_json_fields(row: dict, fields: list) -> dict:
        """Converts specified JSON string fields to Python dictionaries."""
        for field in fields:
            if field in row and isinstance(row[field], str):
                try:
                    row[field] = json.loads(row[field])
                except json.JSONDecodeError:
                    raise ValueError(f"Invalid JSON in field: '{field}'")
        return row

    @staticmethod
    def _validate_required_fields(row: dict, required_fields: list):
        """Ensures required fields exist and are non-empty."""
        missing = [f for f in required_fields if f not in row or row[f] in [None, ""]]
        if missing:
            raise ValueError(f"Missing required fields: {missing}")
