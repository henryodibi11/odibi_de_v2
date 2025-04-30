from typing import Dict, Any, Optional
from odibi_de_v2.sql_builder.select_query_builder import SelectQueryBuilder
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.utils import log_call
from odibi_de_v2.utils import enforce_types
from odibi_de_v2.core.enums import ErrorType

class SQLQueryProvider:
    """
    Provides the appropriate SQL query builder based on configuration.

    This class is designed to generate SQL query builders, specifically tailored to
    the type of SQL operation specified in the configuration. It currently supports
    only SELECT queries but is structured to be extended to other types such as INSERT, UPDATE, and DELETE.

    Attributes:
        config (dict): Configuration dictionary specifying query type and settings.
        query_type (str): The type of SQL query to build, derived from `config`.

    Methods:
        build_query: Constructs and returns a query builder instance based on the `query_type`.
        _build_select_query: Private method to construct a SelectQueryBuilder based on the provided configuration.

    Example:
        >>> config = {'query_type': 'select', 'table': 'users'}
        >>> query_provider = SQLQueryProvider(config)
        >>> query_builder = query_provider.build_query()
        >>> sql_query = query_builder.build_sql()
    """

    @log_call(module="SQL_BUILDER", component="SQLQueryProvider")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SQLQueryProvider",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def __init__(self, config: dict):
        """
        Initializes a new instance of the provider with specified configuration settings.

        This constructor stores the provided configuration dictionary and initializes the
        query type based on the configuration. If 'query_type' is not specified in the configuration,
        it defaults to 'select'.

        Args:
            config (dict): A dictionary containing configuration settings for the provider. It should
            include keys like 'query_type' which defines the type of query to be executed.

        Attributes:
            self.config (dict): Stores the configuration settings passed to the provider.
            self.query_type (str): Defines the type of query to be executed, derived from the 'config' dictionary.

        Example:
            >>> provider_config = {'query_type': 'update'}
            >>> provider_instance = Provider(provider_config)
            >>> print(provider_instance.query_type)
            'update'
        """
        self.config = config
        self.query_type: str = config.get("query_type", "select").lower()

    @log_call(module="SQL_BUILDER", component="SQLQueryProvider")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SQLQueryProvider",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def build_query(self) -> Any:
        """
        Builds and returns the appropriate query builder instance based on the query type.

        This method determines the type of query needed (currently only supports 'select') and returns the
        corresponding query builder instance. It raises an exception if the query type is unsupported.

        Returns:
            Any: An instance of a query builder, specifically a SelectQueryBuilder when the query type is 'select'.

        Raises:
            ValueError: If the `query_type` attribute of the instance is not supported.

        Example:
            Assuming an instance `query_builder` of a class with this method, and `query_builder.query_type`
            is set to 'select':

            >>> query_builder.build_query()
            <SelectQueryBuilder instance>
        """
        if self.query_type == "select":
            return self._build_select_query()
        else:
            raise ValueError(f"Unsupported query_type: {self.query_type}")

    def _build_select_query(self) -> SelectQueryBuilder:
        """
        Constructs a basic SelectQueryBuilder instance using the 'table' specified in the configuration.

        This method initializes a SelectQueryBuilder with the table name retrieved from the instance's configuration.
        It is designed to be used internally within the class to facilitate the creation of a SELECT query builder.
        The method does not handle the application of select fields, joins, or other SQL clauses, which should be
        managed by another component, such as SQLTransformerFromConfig.

        Returns:
            SelectQueryBuilder: An instance of SelectQueryBuilder initialized with the table name from the config.

        Raises:
            ValueError: If the 'table' key is missing in the configuration dictionary.

        Example:
            Assuming an instance `instance` of the class with appropriate configuration:
            >>> query_builder = instance._build_select_query()
            >>> print(type(query_builder))
            <class 'SelectQueryBuilder'>
        """
        table = self.config.get("table")
        if not table:
            raise ValueError("Config must include a 'table' key for SELECT queries.")
        builder = SelectQueryBuilder(table)

        # Note: we intentionally do NOT apply select/joins/etc here
        # SQLTransformerFromConfig will orchestrate the builder

        return builder
