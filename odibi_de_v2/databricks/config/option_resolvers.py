class SourceOptionsResolver:
    """
    Resolves and updates source options based on the provided configuration and mode, handling schema locations
    and sanitizing options.

    This method processes the `source_options` from the `config` dictionary specific to the provided `engine` and
    `mode`. It resolves file paths for schema locations using the given `connector` and removes any options that
    are empty or unset.

    Args:
        None

    Returns:
        dict: A dictionary of resolved and sanitized source options.

    Raises:
        KeyError: If expected keys are missing in the `config` dictionary.

    Example:
        Assuming `connector` is an instance of a Connector class with a method `get_file_path`, and `config` is
        a dictionary containing necessary configurations:
        resolver = SourceOptionsResolver(connector, config, 'batch')
        resolved_options = resolver.resolve()
        print(resolved_options)
    """
    def __init__(self, connector, config: dict, mode: str, engine: str = "databricks"):
        """
        Initializes a new instance of the class with specified configuration and operational mode.

        This constructor sets up the necessary properties for the instance based on the provided connector,
        configuration dictionary, operational mode, and optionally the processing engine. It also prepares
        the options specific to the chosen engine and mode from the configuration.

        Args:
            connector: An object that must have a `get_file_path()` method. This connector is used to interface
                with data sources.
            config (dict): Configuration dictionary containing all settings, including nested dictionaries for
                source options specific to different engines and modes.
            mode (str): The mode of operation, which should be either 'streaming' or 'batch'. This determines
                how data will be processed.
            engine (str, optional): The processing engine to be used. Defaults to 'databricks'. This specifies
                the backend technology for data processing.

        Raises:
            KeyError: If the specified `engine` or `mode` keys are not found in the `config['source_options']`.

        Example:
            >>> connector_instance = MyConnector()
            >>> config = {
                "source_options": {
                    "databricks": {
                        "streaming": {"option1": "value1"},
                        "batch": {"option1": "value2"}
                    }
                }
            }
            >>> instance = MyClass(connector_instance, config, "streaming")
            >>> print(instance.options)
            {'option1': 'value1'}
        """
        self.connector = connector
        self.config = config
        self.engine = engine
        self.mode = mode.lower()
        self.options = config["source_options"][self.engine][self.mode].copy()
    def resolve(self) -> dict:
        """
        Resolves and updates the schema location in the options dictionary based on the configuration settings.

        This method modifies the 'cloudFiles.schemaLocation' in the 'options' dictionary by resolving the
        relative path to an absolute path using the storage unit defined in the configuration. It also
        cleans up the 'options' dictionary by removing any top-level fields that are empty or set to None.

        Returns:
            dict: The updated options dictionary with resolved schema location and without empty fields.

        Example:
            Assuming the instance has been properly configured:
            >>> resolver = Resolver(config, options, connector)
            >>> updated_options = resolver.resolve()
            >>> print(updated_options)
            {'options': {'cloudFiles.schemaLocation': '/absolute/path/to/schema'}}
        """
        storage_unit = self.config["connection_config"]["storage_unit"]
        # Resolve schema location if applicable
        schema_rel_path = self.options.get("options", {}).get("cloudFiles.schemaLocation")
        if schema_rel_path:
            resolved_path = self.connector.get_file_path(storage_unit, schema_rel_path, "")
            self.options["options"]["cloudFiles.schemaLocation"] = resolved_path
        # Remove blank top-level fields
        self.options = {
            k: v for k, v in self.options.items()
            if v not in (None, "", [], {})
        }
        return self.options

class TargetOptionsResolver:
    """
    Resolves and sanitizes target write options based on the provided configuration, mode, and engine.

    This class processes the target configuration to extract and clean up the options relevant to the specified mode
    (e.g., 'streaming' or 'batch') and engine (e.g., 'databricks'). It filters out any options that are `None`, empty
    strings, empty lists, or empty dictionaries.

    Attributes:
        config (dict): A dictionary containing the target configuration.
        mode (str): The mode of operation, typically 'streaming' or 'batch'.
        engine (str): The processing engine, defaults to 'databricks'.

    Methods:
        resolve(): Returns a dictionary of sanitized options for the target based on the mode and engine.

    Returns:
        dict: A dictionary containing the sanitized options for the target.

    Example:
        >>> config = {
            "target_options": {
                "databricks": {
                    "streaming": {
                        "option1": "value1",
                        "option2": "",
                        "option3": None
                    }
                }
            }
        }
        >>> resolver = TargetOptionsResolver(config, "streaming")
        >>> resolver.resolve()
        {'option1': 'value1'}
    """
    def __init__(self, config: dict, mode: str, engine: str = "databricks"):
        """
        Initializes the configuration for a specific engine and operational mode.

        This constructor initializes an instance with the specified configuration, operational mode, and engine type.
        It also extracts and stores options specific to the given engine and mode from the configuration.

        Args:
            config (dict): A dictionary containing various settings and options, potentially nested by engine and mode.
            mode (str): The operational mode, which could influence behavior or settings (e.g., 'test', 'production').
            engine (str, optional): The type of engine to be used. Defaults to 'databricks'.

        Attributes:
            config (dict): Stores the provided configuration dictionary.
            mode (str): Stores the operational mode in lowercase.
            engine (str): Stores the type of engine being used.
            options (dict): Extracted options specific to the provided engine and mode.

        Example:
            >>> config_dict = {
                'target_options': {
                    'databricks': {
                        'test': {'option1': 'value1'},
                        'production': {'option2': 'value2'}
                    }
                }
            }
            >>> instance = MyClass(config=config_dict, mode='Test', engine='databricks')
            >>> print(instance.options)
            {'option1': 'value1'}
        """
        self.config = config
        self.mode = mode.lower()
        self.engine = engine
        self.options = config.get("target_options", {}).get(self.engine, {}).get(self.mode, {}).copy()
    def resolve(self) -> dict:
        """
        Removes null or empty values from the options dictionary of the instance.

        This method filters out any key-value pairs from the instance's `options` dictionary where the value is `None`,
        an empty string, list, or dictionary. It then updates the `options` dictionary to only include the valid,
        non-empty items.

        Returns:
            dict: A dictionary containing only the non-null and non-empty key-value pairs from the original `options`
            dictionary.

        Example:
            Assuming an instance `config` of a class with an `options` attribute initially set as:
            `config.options = {'name': 'example', 'timeout': None, 'tags': [], 'settings': {}}`

            After calling `config.resolve()`, the `options` attribute would be:
            `{'name': 'example'}`
        """
        self.options = {
            k: v for k, v in self.options.items()
            if v not in (None, "", [], {})
        }
        return self.options