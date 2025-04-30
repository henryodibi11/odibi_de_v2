from odibi_de_v2.connector import SQLDatabaseConnection
from odibi_de_v2.core.enums import Framework, DataType
from odibi_de_v2.ingestion import ReaderProvider
from odibi_de_v2.databricks import init_sql_config_connection
from pyspark.sql import SparkSession
import pandas as pd
import json

class ConfigUtils:
    """
    Fetches and prepares configuration data for a specified query type, ID, and project.

    This method retrieves configuration data based on the specified `query_type`, `id`, and `project`. It processes
    the data to ensure that it includes all required fields and that any fields specified as JSON strings are
    properly parsed into dictionaries. The method supports three types of queries: 'source', 'target', and
    'transformation', each requiring different fields and processing.

    Args:
        query_type (str): The type of configuration to retrieve. Valid options are 'source',
            'target', or 'transformation'.
        id (str): The identifier for the configuration record.
        project (str): The project name associated with the configuration.

    Returns:
        dict: A dictionary containing the prepared configuration data.

    Raises:
        ValueError: If `query_type` is not one of 'source', 'target', or 'transformation', or if
        required fields are missing
                    in the fetched configuration data.

    Example:
        >>> config_util = ConfigUtils(
            'my_secret_scope', 'host_key', 'my_database', 'user_key', 'password_key', spark_session)
        >>> config = config_util.get_config('source', '123', 'my_project')
        >>> print(config)
        {
            'source_type': 'database',
            'source_path_or_query': 'SELECT * FROM my_table',
            'source_options': {'option1': 'value1'},
            'connection_config': {'username': 'user', 'password': 'pass'}
        }
    """
    def __init__(
        self,
        secret_scope: str,
        host_key: str,
        database: str,
        user_key: str,
        password_key: str,
        spark: SparkSession
        ) -> None:
        """
        Initializes a new database connection using provided secret keys and a Spark session.

        This constructor initializes the database connection by retrieving necessary credentials
        from a specified secret scope and sets up a reader provider for database operations.

        Args:
            secret_scope (str): The name of the Databricks secret scope containing the database credentials.
            host_key (str): The key within the secret scope for the database host URL.
            database (str): The name of the database to connect to.
            user_key (str): The key within the secret scope for the database username.
            password_key (str): The key within the secret scope for the database password.
            spark (SparkSession): An instance of SparkSession to be used for database operations.

        Returns:
            None

        Raises:
            KeyError: If any of the keys do not exist in the specified secret scope.
            ConnectionError: If the connection to the database cannot be established.

        Example:
            >>> spark_session = SparkSession.builder.appName("MyApp").getOrCreate()
            >>> db_connector = DatabaseConnector(
                    secret_scope="mySecretScope",
                    host_key="dbHost",
                    database="myDatabase",
                    user_key="dbUser",
                    password_key="dbPassword",
                    spark=spark_session
                )
        """
        self.secret_scope = secret_scope
        self.host_key = host_key
        self.database = database
        self.user_key = user_key
        self.password_key = password_key
        self.spark = spark
        self.connection = self._get_connection()
        self.reader_provider = ReaderProvider(connector=self.connection,local_engine=Framework.SPARK)

    def get_config(self,query_type: str, id: str, project: str):
        """
        Retrieves and prepares configuration data for a specified query type related to data processing operations.

        This method processes input parameters to fetch configuration details from an internal dataframe
        based on the query type, ID, and project. It then formats the configuration data appropriately
        before returning it.

        Args:
            query_type (str): The type of query for which configuration is needed. Valid values are
            'source', 'target', or 'transformation'.
            id (str): The identifier used to locate the configuration data within the project.
            project (str): The name of the project from which to retrieve the configuration.

        Returns:
            dict: A dictionary containing the prepared configuration data relevant to the specified query type.

        Raises:
            ValueError: If the `query_type` is not one of 'source', 'target', or 'transformation'.

        Example:
            >>> config_manager = ConfigManager()
            >>> source_config = config_manager.get_config('source', '123', 'data_project')
            >>> print(source_config)
            {'source_type': 'database', 'source_path_or_query': 'SELECT * FROM table',
            'source_options': {...}, 'connection_config': {...}}
        """
        query_type = query_type.strip().lower()
        id = id.strip().lower()
        project = project.strip().lower()
        config_df = self._get_config_df(query_type, id, project)
        row = config_df.to_dict("records")[0]

        if query_type == "source":
            return self._prepare_row(
            row,
            json_fields=["source_options", "connection_config"],
            required_fields=["source_type", "source_path_or_query"])

        elif query_type == "target":
            return self._prepare_row(
                row,
                json_fields=["target_options", "connection_config", "merge_config"],
                required_fields=["write_mode", "target_type"])
        elif query_type == "transformation":
            return self._prepare_row(
                row,
                json_fields=["sql_transformer_config", "framework_transformer_config"],
                required_fields=["transformation_engine"])
        else:
            raise ValueError("Invalid query type. Must be 'source', 'target' or 'transformation'.")


    def _get_connection(self):
        """
        Establishes and returns a database connection using SQL configuration.

        This method initializes a connection to a SQL database by retrieving connection parameters from a
        secret management scope. It uses the class attributes to specify the secret scope and keys for the
        host, database, user, and password.

        Returns:
            A database connection object configured with the specified parameters.

        Raises:
            ConnectionError: If the connection to the database cannot be established.
            KeyError: If any required keys are missing in the secret scope.

        Example:
            # Assuming the class instance is properly initialized with the necessary attributes
            connection = instance._get_connection()
            # Use `connection` to perform database operations
        """
        return init_sql_config_connection(
            secret_scope = self.secret_scope,
            host_key=self.host_key,
            database=self.database,
            user_key=self.user_key,
            password_key=self.password_key
            )
    def _get_config_df(self, query_type: str, id: str, project: str, environment: str = 'qat'):
        """
        Fetches configuration data as a pandas DataFrame based on the specified query type.

        This method constructs a SQL query based on the provided `query_type`, `id`, `project`, and `environment`.
        It then reads the data using a `ReaderProvider` instance and returns the result as a pandas DataFrame.

        Args:
            query_type (str): The type of query to execute. Valid options are 'source', 'target', or 'transformation'.
            id (str): The identifier used to construct the query.
            project (str): The project context for the query.
            environment (str, optional): The environment to target, defaults to 'qat'.

        Returns:
            pandas.DataFrame: The configuration data fetched based on the constructed query.

        Raises:
            ValueError: If `query_type` is not one of 'source', 'target', or 'transformation'.

        Example:
            >>> config_df = instance._get_config_df('source', '123', 'project_alpha')
            >>> print(config_df.head())
        """
        query_type = query_type.strip().lower()
        if query_type == "source":
            query=self._get_source_query(id, project, environment)
        elif query_type == "target":
            query=self._get_target_query(id, project, environment)
        elif query_type == "transformation":
            query=self._get_transformation_query(id, project, environment)
        else:
            raise ValueError("Invalid query type. Must be 'source', 'target' or 'transformation'.")
        config_df = self.reader_provider.read(
            data_type=DataType.SQL,
            container="",  # Not used for SQL
            path_prefix="",  # Not used for SQL
            object_name=query,
            spark=self.spark  # ReaderProvider handles Spark internally
        ).toPandas()
        return config_df

    def _get_source_query(self, source_id: str, project: str, environment: str = 'qat'):
        """
        Generates a SQL query to retrieve configuration details for a specific data source from the database.

        This method constructs a SQL query string that selects various configuration details related to a data source
        from the 'IngestionSourceConfig' and 'SecretsConfig' tables. It filters the results based on the provided
        project, source ID, and environment parameters.

        Args:
            source_id (str): The unique identifier for the data source.
            project (str): The project name associated with the data source.
            environment (str): The environment of the data source, defaults to 'qat'.

        Returns:
            str: A SQL query string that can be executed to fetch the data source configuration.

        Example:
            >>> query = _get_source_query("12345", "DataProject")
            >>> print(query)
            This will output the SQL query string based on the provided source_id and project.
        """
        source_query = f"""
            SELECT
                isc.source_id, isc.project, isc.source_name, isc.source_type,
                isc.source_path_or_query, isc.file_format, isc.is_autoloader,
                isc.source_options, isc.connection_config,
                sc.secret_scope, sc.identifier_key, sc.credential_key,
                sc.server, sc.connection_string_key, sc.auth_type,
                sc.token_header_name, sc.[description]
            FROM IngestionSourceConfig isc
            LEFT JOIN SecretsConfig sc ON isc.secret_config_id = sc.secret_config_id
            WHERE isc.project = '{project}' AND isc.source_id = '{source_id}'
            AND isc.environment = '{environment.lower()}'
        """
        return source_query
    def _get_target_query(self, target_id: str, project: str, environment: str = 'qat'):
        """
        Generates an SQL query to retrieve configuration details for a specific data ingestion target
        based on the provided identifiers and environment.

        Args:
            target_id (str): The unique identifier for the target.
            project (str): The project name associated with the target.
            environment (str, optional): The environment of the target, defaults to 'qat'.

        Returns:
            str: A formatted SQL query string that can be executed to fetch the target configuration.

        Example:
            >>> query = _get_target_query("12345", "DataProject")
            >>> print(query)
            This will output the SQL query string with placeholders replaced by "12345", "DataProject",
            and the default environment "qat".

        Note:
            This function constructs an SQL query with direct string interpolation which can be prone to
            SQL injection if not handled properly elsewhere.
        """
        target_query = f"""
            SELECT
                itc.target_id, itc.project, itc.target_name, itc.target_type,
                itc.target_path_or_table, itc.write_mode, itc.target_options,
                itc.connection_config, itc.merge_config,
                sc.secret_scope, sc.identifier_key, sc.credential_key,
                sc.server, sc.connection_string_key, sc.auth_type,
                sc.token_header_name, sc.[description]
            FROM IngestionTargetConfig itc
            LEFT JOIN SecretsConfig sc ON itc.secret_config_id = sc.secret_config_id
            WHERE itc.project = '{project}' AND itc.target_id = '{target_id}'
            AND itc.environment = '{environment.lower()}'
        """
        return target_query
    def _get_transformation_query(self, transformation_id: str, project: str, environment: str = 'qat'):
        """
        Generates a SQL query to fetch transformation configuration based on specified criteria.

        This method constructs a SQL query to retrieve all columns from the `TransformationConfig` table
        in the database. The query filters the results by the transformation ID, project name, and environment.
        The environment parameter defaults to 'qat' if not specified.

        Args:
            transformation_id (str): The unique identifier for the transformation.
            project (str): The name of the project.
            environment (str, optional): The deployment environment. Defaults to 'qat'.

        Returns:
            str: A SQL query string that can be executed to fetch the transformation configuration.

        Example:
            >>> query = _get_transformation_query("001", "ProjectX")
            >>> print(query)
        """
        transformation_query = f"""
        SELECT
            *
        FROM [dbo].[TransformationConfig]
        WHERE transformation_id = '{transformation_id}' and project = '{project}' and environment = '{environment.lower()}'
            """
        return transformation_query
    def _prepare_row(self, row: dict, json_fields: list, required_fields: list) -> dict:
        """
        Prepares a dictionary representing a row by parsing JSON fields and validating required fields.

        This method processes a dictionary that represents a row from a dataset. It converts specified fields
        from JSON string format to dictionaries and checks for the presence of all required fields, raising
        an error if any are missing.

        Args:
            row (dict): The dictionary representing a row, which may contain JSON strings in some fields.
            json_fields (list): A list of keys in the `row` dictionary whose values are JSON strings that
                need to be parsed into dictionaries.
            required_fields (list): A list of keys that must be present in the `row` dictionary; a ValueError
                is raised if any are missing.

        Returns:
            dict: The updated row dictionary with JSON fields parsed into dictionaries and all required fields present.

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