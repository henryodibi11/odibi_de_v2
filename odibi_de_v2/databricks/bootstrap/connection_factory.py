from odibi_de_v2.databricks.bootstrap import (
    init_spark_with_azure_secrets,
    init_sql_config_connection,
    init_local_connection
)

class BuildConnectionFromConfig:
    """
    Initializes and manages connections to cloud services based on configuration.

    This class facilitates the creation of connections to various cloud platforms such as SQL databases or Azure Data Lake
    Storage (ADLS) by interpreting a configuration dictionary. It automatically determines the type of connection
    (source or target) and the specific cloud platform (e.g., 'sql' or 'adls'), and initializes the appropriate connection
    accordingly. It also handles secure retrieval of necessary credentials.

    Example:
        >>> config = {
            "source_name": "example_source",
            "source_type": "sql",
            "server": "example_server",
            "secret_scope": "example_scope",
            "identifier_key": "example_user",
            "credential_key": "example_password",
            "connection_config": {"database": "example_db"}
        }
        >>> builder = BuildConnectionFromConfig(config)
        >>> connection = builder.get_connection()
        >>> # Now `connection` can be used to interact with the specified SQL database

    Args:
        config (dict): Configuration dictionary specifying the connection parameters. This dictionary must include keys
                    that define whether it is a source or target, the type of platform, and other necessary
                    connection details.

    Returns:
        object: A connection object to the specified cloud service, ready for use.

    Raises:
        Exception: If the specified platform is not supported or if the configuration is incomplete or malformed.
    """

    def __init__(self, config: dict):
        """
        Initializes the configuration for a data handling object, determining source and target settings based on the
        provided configuration dictionary.

        Args:
            config (dict): A dictionary containing configuration options which may include keys like 'source_name',
            'source_type', 'target_name', and 'target_type'.

        Attributes:
            is_source (bool): Determines if the configuration pertains to a source based on the presence of
                'source_name' in the config.
            name_field (str): Sets to 'source_name' if it's a source configuration, otherwise 'target_name'.
            platform (str): Retrieves and stores the platform type in lowercase, based on whether it's a source or
                target configuration.

        Example:
            >>> config = {
                "source_name": "DatabaseA",
                "source_type": "SQL",
                "target_name": "DatabaseB",
                "target_type": "NoSQL"
            }
            >>> handler = DataHandler(config)
            >>> print(handler.platform)
            'sql'
        """
        self.config = config
        self.is_source = "source_name" in config
        self.name_field = "source_name" if self.is_source else "target_name"
        self.platform = self.config.get("source_type" if self.is_source else "target_type", "").lower()

    def get_connection(self):
        """
        Establishes a connection based on the platform specified in the instance.

        This method selects the appropriate connection method (`_build_sql_connection` or `_build_adls_connection`)
        based on the `platform` attribute of the instance. It supports connections for SQL and ADLS platforms.

        Returns:
            object: A connection object to the specified platform. The type of the object depends on the platform.

        Raises:
            Exception: If the `platform` attribute is not recognized (i.e., not 'sql' or 'adls').

        Example:
            Assuming an instance `conn_manager` of a class with this method and `platform` set to "sql":
            >>> connection = conn_manager.get_connection()
            This would use `_build_sql_connection` to establish and return a SQL connection.
        """
        match self.platform:
            case "sql":
                return self._build_sql_connection()
            case "adls":
                return self._build_adls_connection()
            case "local":
                return self._build_local_connection()
            case _:
                raise Exception(f"Unsupported platform: {self.platform}")

    def _build_sql_connection(self):
        """
        Builds and returns a SQL connection using the configuration stored in the instance.

        This method constructs a connection to a SQL database using parameters specified in the instance's configuration
        attribute. It retrieves connection details such as the database server, database name, user identifier, and
        credentials from the configuration and uses these to initialize the SQL connection.

        Returns:
            A database connection object initialized with the specified parameters.

        Raises:
            KeyError: If required keys ('secret_scope', 'server', 'identifier_key', 'credential_key') are missing in
            the configuration.

        Example:
            Assuming the instance `db_instance` is properly configured:
            >>> connection = db_instance._build_sql_connection()
            This would initialize a SQL connection based on the configuration provided in `db_instance.config`.
        """
        cfg = self.config
        conn_cfg = cfg.get("connection_config", {})

        return init_sql_config_connection(
            secret_scope=cfg["secret_scope"],
            host_key=cfg["server"],
            database=conn_cfg.get("database"),
            user_key=cfg["identifier_key"],
            password_key=cfg["credential_key"]
        )

    def _build_adls_connection(self):
        """
        Initializes and returns a connection to Azure Data Lake Storage (ADLS) using Spark.

            This method configures a Spark session to connect to ADLS by utilizing Azure secrets for authentication.
            It extracts necessary configuration details from the instance's `config` attribute to set up the connection.

            Returns:
                pyspark.sql.session.SparkSession: A configured Spark session object connected to ADLS.

            Raises:
                KeyError: If required keys are missing in the `config` dictionary.
                Exception: If the Spark session initialization fails for reasons related to Azure credentials or
                network issues.

            Example:
                Assuming an instance `ingestor` of a class with this method and properly set `config`:
                >>> connection = ingestor._build_adls_connection()
                >>> type(connection)
                <class 'pyspark.sql.session.SparkSession'>
        """
        cfg = self.config

        _, connection = init_spark_with_azure_secrets(
            app_name="ADLS_Ingestion",
            secret_scope=cfg["secret_scope"],
            account_name_key=cfg["identifier_key"],
            account_key_key=cfg["credential_key"],
            logger_metadata={
                "project": cfg.get("project"),
                "table": cfg.get(self.name_field),
                "step": "INGESTION"
            }
        )
        return connection

    def _build_local_connection(self):
        """
        Builds and returns a LocalConnection for file-based ingestion.

        This allows ingestion of CSV, JSON, or Delta files stored locally or in DBFS.
        """
        conn_cfg = self.config.get("connection_config", {})
        base_path = conn_cfg.get("base_path", "/dbfs/FileStore")
        connection = init_local_connection(
            app_name="Local_Ingestion",
            base_path=base_path
        )
        return connection
