from typing import Optional, List, Union, Dict, Any
from odibi_de_v2.sql_builder.sql_query_provider import SQLQueryProvider
from odibi_de_v2.sql_builder.select_query_builder import SelectQueryBuilder
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.utils import log_call
from odibi_de_v2.utils.decorators import enforce_types
from odibi_de_v2.core.enums import ErrorType

class SQLGeneratorFromConfig:
    """
    Dynamically constructs a SQL query from a provided configuration dictionary.

    This class allows for the creation of complex SQL queries by specifying components such as
    select columns, joins, conditions, and more through a structured configuration dictionary.
    It supports a wide range of SQL functionalities including conditional statements, aggregation,
    window functions, and set operations like UNION, INTERSECT, and EXCEPT.

    Attributes:
        config (dict): Configuration dictionary specifying the components of the SQL query.
        builder (Optional[Any]): An instance of a query builder class, initialized based on the provided configuration.

    Methods:
        generate_query: Constructs and returns the complete SQL query as a string based on the `config`.

    Raises:
        RuntimeError: If any errors occur during the initialization or query building process.

    Example:
        >>> config = {
        ... "columns": ["id", "name"],
        ... "table": "users",
        ... "where": "age > 18",
        ... "order_by": "name"}
        >>> sql_generator = SQLGeneratorFromConfig(config)
        >>> query = sql_generator.generate_query()
        >>> print(query)  # Outputs the constructed SQL query string based on the configuration.
    """

    @log_call(module="SQL_BUILDER", component="SQLGeneratorFromConfig")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SQLGeneratorFromConfig",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def __init__(self, config: dict):
        """
        Initializes a new instance of the transformer with specified configuration settings.

        This constructor stores the configuration provided and initializes the builder attribute to None.
        The configuration should include necessary details such as table names, columns, joins, filters, etc.,
        which are essential for building SQL queries.

        Args:
            config (dict): A dictionary containing necessary configuration details such as 'table', 'columns', 'joins',
            'filters', etc. This information is used to construct SQL queries.

        Returns:
            None

        Example:
            >>> transformer_config = {
            ... 'table': 'users',
            ... 'columns': ['id', 'name', 'email'],
            ... 'joins': [
            ...    {'table': 'orders', 'on': 'users.id = orders.user_id'}],
            ... 'filters': 'age > 18'}
            >>> transformer = Transformer(config=transformer_config)
        """
        self.config = config
        self.builder: Optional[Any] = None  # Could be SelectQueryBuilder, InsertQueryBuilder, etc.


    @log_call(module="SQL_BUILDER", component="SQLGeneratorFromConfig")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SQLGeneratorFromConfig",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def generate_query(self) -> str:
        """
        Generates and returns a complete SQL query string based on the internal configuration of the instance.

        This method orchestrates the construction of a SQL query by sequentially invoking various internal methods
        to apply different SQL clauses such as SELECT, FROM, WHERE, etc., based on the current state and configuration
        of the instance. The method ensures that all necessary SQL components are integrated in the correct order to
        form a valid SQL query.

        Returns:
            str: A string representing the complete SQL query ready to be executed.

        Example:
            >>> Assuming an instance `query_builder` of a class with this method:
            >>> sql_query = query_builder.generate_query()
            >>> print(sql_query)

        Note:
            This method assumes that all internal methods (like `_apply_select`, `_apply_where`, etc.) modify the state
            of `self.builder` to progressively build the SQL query.
        """
        self._initialize_builder()
        self._apply_ctes()
        self._apply_select()
        self._apply_case_when()
        self._apply_joins()
        self._apply_where()
        self._apply_group_by()
        self._apply_having()
        self._apply_order_by()
        self._apply_distinct()
        self._apply_pivot()
        self._apply_unpivot()
        self._apply_window_functions()
        self._apply_limit()
        self._apply_unions()
        self._apply_intersects()
        self._apply_excepts()

        return self.builder.build()

    def _initialize_builder(self):
        """
        Initializes the SQL query builder for the current instance.

        This method sets up the `builder` attribute by creating an instance of `SQLQueryProvider` using the instance's
        configuration (`self.config`), and then calling its `build_query` method.

        Args:
            None

        Returns:
            None

        Raises:
            AttributeError: If `self.config` is not set or is invalid for `SQLQueryProvider`.
            Exception: If the `build_query` method of `SQLQueryProvider` fails.

        Example:
            >>> Assuming an instance `obj` of the class containing this method:
            >>> obj._initialize_builder()
            >>> print(obj.builder)  # Outputs the SQL query builder instance
        """
        self.builder = SQLQueryProvider(self.config).build_query()

    def _apply_select(self):
        """
        Applies a selection of columns to the builder based on the configuration.

        This method reads the 'columns' from the instance's configuration and, if specified, applies them to the
        builder's select method. This is typically used to filter out which columns to retrieve or manipulate in
        a database query or a data processing pipeline.

        Raises:
            AttributeError: If the builder object does not support the 'select' method.

        Example:
            >>> Assuming `self.config` is set to `{"columns": ["name", "age"]}` and `self.builder` has a method
            ... `select`, this method will execute `self.builder.select(["name", "age"])`.
        """
        columns = self.config.get("columns")
        if columns and hasattr(self.builder, "select"):
            self.builder.select(columns)

    def _apply_case_when(self):
        """
        Applies conditional logic defined in the configuration to the builder object.

        This method retrieves a 'case_when' configuration from the instance's config attribute,
        which specifies multiple conditional cases. For each case, it adds the conditional logic
        to the builder using the builder's `add_case` method, if available.

        Arguments:
            None

        Returns:
            None

        Raises:
            AttributeError: If the builder object does not have the 'add_case' method.

        Example:
            Assuming the builder object supports `add_case` and the config is properly set,
            this method might be used as follows:

            >>> config = {
            ... "case_when": [
            ...    {
            ...        "cases": [("status", "new"), ("priority", "high")],
            ...        "else_value": "default",
            ...        "alias": "case_1"}]}
            example_instance = ClassName(config=config)
            example_instance._apply_case_when()  # Applies the defined case logic to the builder
        """
        case_whens = self.config.get("case_when")
        if case_whens and hasattr(self.builder, "add_case"):
            for case in case_whens:
                cases = case.get("cases", [])
                else_value = case.get("else_value")
                alias = case.get("alias")
                if cases and alias:
                    self.builder.add_case(cases, else_value, alias)

    def _apply_joins(self):
        """
        Applies join operations to a query builder based on configuration settings.

        This method reads join configurations from the instance's `config` attribute and applies them to the
        `builder` attribute if it supports the join operation. Each join configuration should specify the table
        to join, the condition on which to join, and optionally the type of join (defaults to "INNER").

        Args:
            None

        Returns:
            None

        Raises:
            AttributeError: If the `builder` attribute does not support the `join` method.

        Example:
            >>> Assuming `self.config` contains:
            ... {
            ... "joins": [
            ...    {"table": "users", "condition": "users.id = orders.user_id", "type": "INNER"},
            ...    {"table": "products", "condition": "products.id = orders.product_id"}]}
            This method will configure the builder to perform an INNER join on the "users" table and a default INNER
            join on the "products" table based on the specified conditions.
        """
        joins = self.config.get("joins")
        if joins and hasattr(self.builder, "join"):
            for join in joins:
                table = join.get("table")
                condition = join.get("condition")
                join_type = join.get("type", "INNER")
                if table and condition:
                    self.builder.join(table, condition, join_type)

    def _apply_where(self):
        """
        Applies filtering conditions to a query builder based on the configuration provided.

        This method retrieves 'where' conditions from the instance's configuration and applies them to the query builder
        if it supports the 'where' method. The conditions can be specified either as a single string or a list of
        strings, each representing a condition.

        Args:
            None: This method operates on the instance's attributes and does not take arguments directly.

        Returns:
            None: This method does not return any value; it modifies the builder in place.

        Raises:
            AttributeError: If the builder does not have a 'where' method, an AttributeError will be raised.

        Example:
            Assuming `self.config` contains `{"where": ["age > 30", "status = 'active'"]}` and `self.builder` is an
            instance of a query builder class with a `where` method:

            self._apply_where()

            This will apply the conditions "age > 30" and "status = 'active'" to the builder.
        """
        where_conditions = self.config.get("where")
        if where_conditions and hasattr(self.builder, "where"):
            if isinstance(where_conditions, list):
                for condition in where_conditions:
                    self.builder.where(condition)
            elif isinstance(where_conditions, str):
                self.builder.where(where_conditions)

    def _apply_group_by(self):
        """
        Applies a 'group by' operation to the builder based on the configuration.

        This method retrieves the 'group_by' columns from the instance's configuration and, if present, applies them to
        the builder's 'group_by' method. It is intended to be used internally within the class, hence the leading
        underscore in the method name.

        Args:
            None: This method does not take parameters; it uses the instance's attributes.

        Returns:
            None: This method does not return any value.

        Raises:
            AttributeError: If the 'group_by' attribute is set in the configuration but the builder does not support the
            'group_by' method.

        Example:
            Assuming an instance `instance` of the class has been properly configured and initialized:
            >>> instance._apply_group_by()
            This would apply the configured group by columns to the builder's group by method if applicable.
        """
        group_by_columns = self.config.get("group_by")
        if group_by_columns and hasattr(self.builder, "group_by"):
            self.builder.group_by(group_by_columns)

    def _apply_having(self):
        """
        Applies the 'HAVING' conditions specified in the configuration to the query builder.

        This method retrieves 'having' conditions from the instance's configuration and applies them to the query
        builder if it supports the 'having' method. The conditions can be specified as either a single string or a
        list of strings.

        Args:
            None

        Returns:
            None

        Raises:
            AttributeError: If the builder does not support the 'having' method.

        Example:
            Assuming `self.config` contains `{"having": ["SUM(revenue) > 1000", "COUNT(customer_id) > 10"]}` and
            `self.builder` supports 'having', this method will apply these conditions to filter the query results based
            on the aggregated functions.

        Note:
            This method is intended to be used internally within the class and should not be accessed directly from
            outside.
        """
        having_conditions = self.config.get("having")
        if having_conditions and hasattr(self.builder, "having"):
            if isinstance(having_conditions, list):
                for condition in having_conditions:
                    self.builder.having(condition)
            elif isinstance(having_conditions, str):
                self.builder.having(having_conditions)

    def _apply_order_by(self):
        """
        Applies ordering to the query builder based on configuration settings.

        This private method retrieves the 'order_by' configuration, if present, and applies it to the query builder to
        order the results according to the specified columns.

        Args:
            None

        Returns:
            None

        Raises:
            AttributeError: If the 'builder' object does not have an 'order_by' method.

        Example:
            Assuming `self.config` contains {'order_by': 'name DESC'} and `self.builder` supports the 'order_by' method:

            self._apply_order_by()  # This will order the query results by the 'name' column in descending order.
        """
        order_by_columns = self.config.get("order_by")
        if order_by_columns and hasattr(self.builder, "order_by"):
            self.builder.order_by(order_by_columns)

    def _apply_limit(self):
        """
        Applies a limit to the query builder based on the configuration.

        This method retrieves a 'limit' value from the instance's configuration and, if present and applicable, applies
        this limit to the query builder associated with the instance.

        Args:
            None

        Returns:
            None

        Raises:
            AttributeError: If the 'builder' attribute does not support the 'limit' method.

        Example:
            Assuming `self.config` contains {'limit': 10} and `self.builder` supports a `limit` method:

            >>> self._apply_limit()
            This would set the limit of the builder's query to 10.
        """
        limit_value = self.config.get("limit")
        if limit_value is not None and hasattr(self.builder, "limit"):
            self.builder.limit(limit_value)

    def _apply_distinct(self):
        """
        Applies distinct settings to the query builder based on the configuration.

        This method checks the configuration for 'distinct' and 'distinct_on' settings. If 'distinct' is set to True
        and the builder supports the 'set_distinct' method, it will apply a general distinct filter. If 'distinct_on'
        is provided and supported by the builder, it will apply a distinct filter based on specific fields.

        Args:
            None

        Returns:
            None

        Raises:
            AttributeError: If the builder does not support the necessary distinct methods when they are configured to
            be used.

        Example:
            Assuming `self.config` is set to `{"distinct": True, "distinct_on": ["name", "date"]}` and the builder has
            the appropriate methods:

            self._apply_distinct()

            This will configure the builder to filter query results to distinct rows, specifically on the 'name' and
            'date' fields.
        """
        if self.config.get("distinct") and hasattr(self.builder, "set_distinct"):
            self.builder.set_distinct()
        distinct_on = self.config.get("distinct_on")
        if distinct_on and hasattr(self.builder, "distinct_on"):
            self.builder.distinct_on(distinct_on)

    def _apply_ctes(self):
        """
        Applies Common Table Expressions (CTEs) to a SQL query builder based on the configuration provided.

        This method retrieves CTE configurations from the instance's configuration dictionary under the 'ctes' key.
        Each CTE configuration must include a 'name' and a 'query'. The 'query' can be either a string representing a
        SQL query or a dictionary that can be transformed into a SQL query using `SQLTransformerFromConfig`. It then
        applies these CTEs to the query builder if it supports the `with_cte` method.

        Args:
            None. The method operates on the instance's attributes.

        Returns:
            None. The method modifies the builder attribute in-place.

        Raises:
            AttributeError: If the builder does not support CTEs (i.e., lacks a `with_cte` method).

        Example:
            Assuming `self.config` contains:
            {
                "ctes": [
                    {"name": "temporary_data", "query": "SELECT * FROM my_table"},
                    {"name": "filtered_data", "query": {"table": "temporary_data", "filter": "value > 10"}}
                ]
            }
            and `self.builder` is an instance of a class with a `with_cte` method, calling `_apply_ctes()` will
            configure the builder to include the specified CTEs.
        """
        ctes = self.config.get("ctes")
        if ctes and hasattr(self.builder, "with_cte"):
            for cte in ctes:
                name = cte.get("name")
                query_config = cte.get("query")
                if isinstance(query_config, dict):
                    cte_query = SQLGeneratorFromConfig(query_config).transform()
                    self.builder.with_cte(name, cte_query)
                elif isinstance(query_config, str):
                    self.builder.with_cte(name, query_config)

    def _apply_unions(self):
        """
        Applies union operations to the current SQL query builder based on the configuration specified in `self.config`.

        This method retrieves union configurations from `self.config` under the 'unions' key. Each union configuration
        should specify a sub-query and may indicate whether it's a UNION ALL through the 'all' key. It constructs a
        query using these configurations and applies them to the current builder object.

        Args:
            None

        Returns:
            None: This method does not return anything but modifies the builder in-place.

        Raises:
            AttributeError: If the builder object does not support the 'union' method.

        Example:
            Assuming `self.config` contains:
            {
                "unions": [
                    {"query": {"table": "employees", "fields": ["id", "name"]}, "all": true},
                    {"query": {"table": "contractors", "fields": ["id", "name"]}}
                ]
            }
            This would configure the builder to perform a UNION ALL with a query built from the first sub-config,
            and a standard UNION with the second.
        """
        unions = self.config.get("unions")
        if unions and hasattr(self.builder, "union"):
            for union_entry in unions:
                query_config = union_entry.get("query")
                all_flag = union_entry.get("all", False)
                if query_config:
                    union_query = SQLGeneratorFromConfig(query_config).transform()
                    union_builder = SelectQueryBuilder("dummy").select(["*"])
                    union_builder.build = lambda: union_query
                    self.builder.union(union_builder, all=all_flag)

    def _apply_intersects(self):
        """
        Applies intersection operations to the builder based on the configuration provided.

        This method retrieves intersection configurations from the `intersects` key in the instance's config attribute.
        If intersections are defined, it dynamically constructs SQL queries using these configurations and applies
        them to the builder's `intersect` method. Each intersection operation modifies the builder's state by intersecting
        it with a dynamically generated SQL query.

        Attributes:
            None explicitly required as parameters, but the method operates on the instance's `config` and `builder` attributes.

        Returns:
            None: This method modifies the builder in-place and does not return any value.

        Raises:
            AttributeError: If the `builder` attribute does not have an `intersect` method.

        Example:
            Assuming an instance `processor` of a class with this method, and it has appropriate `config` and `builder`
            attributes set:
            >>> processor._apply_intersects()
            This would apply all configured intersection operations to the `builder`.
        """
        intersects = self.config.get("intersects")
        if intersects and hasattr(self.builder, "intersect"):
            for intersect_entry in intersects:
                query_config = intersect_entry.get("query")
                if query_config:
                    intersect_query = SQLGeneratorFromConfig(query_config).transform()
                    intersect_builder = SelectQueryBuilder("dummy").select(["*"])
                    intersect_builder.build = lambda: intersect_query
                    self.builder.intersect(intersect_builder)

    def _apply_excepts(self):
        """
        Applies exception queries to the main query builder based on configuration settings.

        This method retrieves exception configurations from the instance's config attribute, constructs SQL queries
        for each exception, and applies these queries to the main query builder using its `except_query` method. The
        method assumes that the main query builder has an `except_query` method capable of integrating additional SQL
        queries as exceptions.

        Attributes:
            None explicitly required as parameters, but the method operates on the instance's `config` and `builder`
            attributes.

        Returns:
            None: This method modifies the builder in-place and does not return any value.

        Raises:
            AttributeError: If the builder does not have an `except_query` method.
            KeyError: If the configuration for any exception entry is incomplete or improperly formatted.

        Example:
            Assuming an instance `query_modifier` of a class with this method, and the instance has appropriate `config`
            and `builder` attributes set:

            >>> query_modifier.config = {
            ... "excepts": [
            ...    {"query": {"table": "restricted_data", "filter": "user_id = 1"}}]
            ... }
            >>> query_modifier.builder = SomeQueryBuilder()
            >>> query_modifier._apply_excepts()
        """
        excepts = self.config.get("excepts")
        if excepts and hasattr(self.builder, "except_query"):
            for except_entry in excepts:
                query_config = except_entry.get("query")
                if query_config:
                    except_query = SQLGeneratorFromConfig(query_config).transform()
                    except_builder = SelectQueryBuilder("dummy").select(["*"])
                    except_builder.build = lambda: except_query
                    self.builder.except_query(except_builder)

    def _apply_pivot(self):
        """
        Applies a pivot transformation to the data using the builder's pivot method, based on the configuration
        settings.

        This method reads the pivot configuration from `self.config` and applies a pivot operation if the configuration
        is present and valid. The pivot operation restructures the data by transforming values from one or more columns
        into a new set of columns, aggregating data as specified.

        No arguments are explicitly taken by this method as it operates directly on the instance's attributes.

        Returns:
            None: This method does not return anything but modifies the builder's state.

        Raises:
            AttributeError: If the 'pivot' configuration is set but the builder does not support the pivot operation.

        Example:
            Assuming `self.config` is set up with a valid pivot configuration and `self.builder` supports pivoting:
            >>> self.config = {
            ... "pivot": {
            ...    "column": "date",
            ...    "values": "sales",
            ...    "aggregate_function": "sum",
            ...    "alias": "daily_sales"
            ... }
            ... }
            self._apply_pivot()
        """
        pivot = self.config.get("pivot")
        if pivot and hasattr(self.builder, "pivot"):
            self.builder.pivot(
                column=pivot["column"],
                values=pivot["values"],
                aggregate_function=pivot["aggregate_function"],
                alias=pivot.get("alias")
            )

    def _apply_unpivot(self):
        """
        Applies the unpivot transformation to the builder based on the configuration settings.

        This method retrieves the unpivot configuration from the instance's config attribute and, if present, applies the
        unpivot transformation using the builder's unpivot method. The transformation parameters such as value_column,
        category_column, columns, and an optional alias are specified in the configuration.

        Raises:
            AttributeError: If the builder object does not have an 'unpivot' method.

        Example:
            Assuming the instance has a valid builder and config attribute set:
            instance._apply_unpivot()
        """
        unpivot = self.config.get("unpivot")
        if unpivot and hasattr(self.builder, "unpivot"):
            self.builder.unpivot(
                value_column=unpivot["value_column"],
                category_column=unpivot["category_column"],
                columns=unpivot["columns"],
                alias=unpivot.get("alias")
            )

    def _apply_window_functions(self):
        """
        Applies configured window functions to a query builder based on the instance's configuration.

        This method retrieves a list of window functions from the instance's configuration and applies each
        one to the query builder if the builder supports window functions. Each window function can specify
        the column to operate on, the function to apply, and optional partitioning and ordering criteria.

        Raises:
            AttributeError: If the builder does not support window functions but window functions are configured.

        Example:
            Assuming `self.builder` is an instance of a query builder that supports window functions and
            `self.config` contains appropriate window function configurations, this method will configure
            the builder with those window functions.
        """
        window_functions = self.config.get("window_functions")
        if window_functions and hasattr(self.builder, "with_window_function"):
            for window in window_functions:
                self.builder.with_window_function(
                    column=window["column"],
                    function=window["function"],
                    partition_by=window.get("partition_by"),
                    order_by=window.get("order_by"),
                    alias=window.get("alias")
                )
