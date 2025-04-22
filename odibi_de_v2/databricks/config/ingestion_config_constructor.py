import pandas as pd
import json


class IngestionConfigConstructor:
    """
    Parses and prepares source and target ingestion configurations from provided DataFrames.

    This class is designed to take two pandas DataFrames containing configuration data for source and target systems,
    parse JSON fields, validate required fields, and prepare dictionaries that are ready for use in data ingestion
    processes.

    Attributes:
        sources_df (pd.DataFrame): DataFrame containing configuration rows for the source system.
        targets_df (pd.DataFrame): DataFrame containing configuration rows for the target system.

    Methods:
        prepare: Processes the source and target DataFrames to produce configuration dictionaries.

    Example:
        >>> constructor = IngestionConfigConstructor(source_df, target_df)
        >>> source_config, target_config = constructor.prepare()

    Returns:
        tuple[dict, dict]: A tuple containing two dictionaries:
                            - prepared_source_config: Configuration dictionary for the source.
                            - prepared_target_config: Configuration dictionary for the target.

    Raises:
        ValueError: If JSON parsing fails or if required fields are missing in the configuration data.
    """

    def __init__(self, sources_df: pd.DataFrame, targets_df: pd.DataFrame):
        """
        Initializes the data handler with source and target data.

        This constructor stores the provided pandas DataFrames for sources and targets into the instance for further
        processing and manipulation.

        Args:
            sources_df (pd.DataFrame): A DataFrame containing the source data.
            targets_df (pd.DataFrame): A DataFrame containing the target data.

        Returns:
            None

        Example:
            >>> sources_data = pd.DataFrame({'id': [1, 2], 'value': ['source1', 'source2']})
            >>> targets_data = pd.DataFrame({'id': [1, 2], 'value': ['target1', 'target2']})
            >>> handler = DataHandler(sources_df=sources_data, targets_df=targets_data)
        """

        self.sources_df = sources_df
        self.targets_df = targets_df

    def prepare(self) -> tuple[dict, dict]:
        """
        Prepares and returns configuration dictionaries for both source and target.

        This method calls internal methods to parse and validate configurations for both the source and target. It
        ensures that the configurations meet the required structure and validation rules before they are returned.

        Returns:
            tuple[dict, dict]: A tuple containing two dictionaries:
                - The first dictionary contains the prepared configuration for the source.
                - The second dictionary contains the prepared configuration for the target.

        Raises:
            ConfigurationError: If the configurations are invalid or cannot be parsed.

        Example:
            >>> config_manager = ConfigManager()
            >>> source_config, target_config = config_manager.prepare()
            >>> print(source_config, target_config)
        """
        source_config = self._prepare_source()
        target_config = self._prepare_target()
        return source_config, target_config

    def _prepare_source(self) -> dict:
        """
        Prepares the first row of the source DataFrame for further processing.

        This method converts the first row of the `sources_df` DataFrame to a dictionary, then processes
        it to handle JSON fields and ensure required fields are present.

        Returns:
            dict: A dictionary containing the processed source data.

        Raises:
            KeyError: If any of the required fields are missing in the row.
            IndexError: If `sources_df` is empty and no row can be accessed.
            JSONDecodeError: If JSON parsing fails for `source_options` or `connection_config`.

        Example:
            Assuming `sources_df` is a DataFrame with at least one row and the necessary columns:
            >>> self._prepare_source()
            {
                'source_type': 'database',
                'source_path_or_query': 'SELECT * FROM users',
                'source_options': {'timeout': 30},
                'connection_config': {'host': 'localhost', 'port': 5432}
            }
        """
        row = self.sources_df.to_dict("records")[0]
        return self._prepare_row(
            row,
            json_fields=["source_options", "connection_config"],
            required_fields=["source_type", "source_path_or_query"]
        )

    def _prepare_target(self) -> dict:
        """
        Prepares the first row of the target DataFrame for further processing.

        This method converts the first row of `self.targets_df` into a dictionary, then processes
        it by potentially deserializing JSON fields and ensuring required fields are present.

        Returns:
            dict: A dictionary representing the prepared target configuration.

        Raises:
            KeyError: If any of the required fields are missing in the row.
            ValueError: If JSON deserialization fails for any of the specified JSON fields.

        Example:
            Assuming `self.targets_df` is a DataFrame with at least one row and the necessary columns:
            >>> self._prepare_target()
            {
                'write_mode': 'append',
                'target_type': 'database',
                'target_options': {'batch_size': 1000},
                'connection_config': {'host': 'localhost', 'port': 5432},
                'merge_config': {'key': 'id'}
            }
        """
        row = self.targets_df.to_dict("records")[0]
        return self._prepare_row(
            row,
            json_fields=["target_options", "connection_config", "merge_config"],
            required_fields=["write_mode", "target_type"]
        )

    def _prepare_row(self, row: dict, json_fields: list, required_fields: list) -> dict:
        """
        Prepares a dictionary representing a row by parsing JSON fields and validating required fields.

        This method first parses fields in the row that are in JSON string format into actual JSON objects
        (dictionaries). It then checks if all required fields are present in the row and raises a ValueError
        if any are missing.

        Args:
            row (dict): The dictionary representing a row which may contain JSON strings in some fields.
            json_fields (list): A list of keys in the `row` dictionary whose values are JSON strings that
            need to be parsed.
            required_fields (list): A list of keys that must be present in the `row` dictionary.

        Returns:
            dict: The updated row dictionary with JSON fields parsed and all required fields present.

        Raises:
            ValueError: If any of the required fields are missing in the row.

        Example:
            row = {
                "data": '{"name": "John", "age": 30}',
                "id": "123",
                "timestamp": "2021-06-01"
            }
            json_fields = ["data"]
            required_fields = ["id", "data", "timestamp"]
            prepared_row = _prepare_row(row, json_fields, required_fields)
            # Output: {'data': {'name': 'John', 'age': 30}, 'id': '123', 'timestamp': '2021-06-01'}
        """
        row = self._parse_json_fields(row, json_fields)
        self._validate_required_fields(row, required_fields)
        return row

    @staticmethod
    def _parse_json_fields(row: dict, fields: list) -> dict:
        """
        Parses specified JSON-encoded fields of a dictionary.

        This function iterates over a list of fields and decodes the JSON-encoded string in each specified field of
        the input dictionary. If decoding fails for any field, it raises a ValueError.

        Args:
            row (dict): The dictionary containing potential JSON-encoded strings.
            fields (list): A list of keys that correspond to the fields in the `row` dictionary whose values are
            to be JSON-decoded.

        Returns:
            dict: The dictionary with the specified fields JSON-decoded.

        Raises:
            ValueError: If any specified field contains an invalid JSON string, indicating it cannot be decoded.

        Example:
            >>> row = {'name': 'Alice', 'data': '{"age": 30, "city": "New York"}'}
            >>> fields = ['data']
            >>> _parse_json_fields(row, fields)
            {'name': 'Alice', 'data': {'age': 30, 'city': 'New York'}}
        """
        for field in fields:
            if field in row and isinstance(row[field], str):
                try:
                    row[field] = json.loads(row[field])
                except json.JSONDecodeError:
                    raise ValueError(f"Invalid JSON in field: '{field}'")
        return row

    @staticmethod
    def _validate_required_fields(row: dict, required_fields: list):
        """
        Validates that all required fields are present and not empty in a given dictionary.

        This function checks if each field in `required_fields` exists in the `row` dictionary and that its value
        is neither `None` nor an empty string. If any required field is missing or empty, it raises a ValueError.

        Args:
            row (dict): The dictionary to validate.
            required_fields (list): A list of strings representing the keys that must be present and non-empty
            in the `row`.

        Raises:
            ValueError: If any of the required fields are missing from the `row` or their values are empty.

        Example:
            >>> _validate_required_fields({'name': 'Alice', 'age': 30}, ['name', 'age'])
            None  # No exception is raised

            >>> _validate_required_fields({'name': 'Alice'}, ['name', 'age'])
            ValueError: Missing required fields: ['age']
        """
        missing = [f for f in required_fields if f not in row or row[f] in [None, ""]]
        if missing:
            raise ValueError(f"Missing required fields: {missing}")
