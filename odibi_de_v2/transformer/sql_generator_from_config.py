from typing import Optional, List, Union, Dict, Any
from odibi_de_v2.sql_builder.sql_query_provider import SQLQueryProvider
from odibi_de_v2.sql_builder.select_query_builder import SelectQueryBuilder
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.utils import log_call
from odibi_de_v2.utils.decorators import enforce_types
from odibi_de_v2.core.enums import ErrorType

class SQLGeneratorFromConfig:
    """
    Dynamically construct SQL queries from a configuration dictionary.

    This class provides a high-level interface for turning structured Python
    dictionaries into valid SQL queries. It orchestrates an underlying
    query builder (currently `SelectQueryBuilder`) and applies operations
    such as SELECT, JOIN, WHERE, GROUP BY, HAVING, ORDER BY, DISTINCT,
    PIVOT, UNPIVOT, window functions, and set operations (UNION, INTERSECT,
    EXCEPT).

    Attributes:
        config (dict): The configuration dictionary that defines the query.
        builder (Optional[Any]): The underlying query builder instance (e.g.,
            SelectQueryBuilder).

    Supported Config Keys:
        - "query_type": The type of query (currently only "select").
        - "table": The base table name (may include alias).
        - "columns": List of columns or dicts for SELECT.
        - "case_when": CASE expressions with conditions and aliases.
        - "joins": List of join definitions (table, condition, type, auto_quote).
        - "where": List or str of WHERE conditions.
        - "group_by": List of GROUP BY columns.
        - "having": HAVING condition(s).
        - "order_by": List of str/dict columns for ORDER BY.
        - "limit": Row limit (int).
        - "distinct": Boolean flag for DISTINCT.
        - "distinct_on": List of columns for DISTINCT ON.
        - "ctes": Common Table Expressions (name + query).
        - "unions": UNION or UNION ALL queries.
        - "intersects": INTERSECT queries.
        - "excepts": EXCEPT queries.
        - "pivot": Pivot definition (column, values, aggregate_function, alias).
        - "unpivot": Unpivot definition (value_column, category_column, columns, alias).
        - "window_functions": List of window function definitions.

    Methods:
        transform() -> str:
            Generate and return the complete SQL query string.

    Example:
        >>> config = {
        ...     "query_type": "select",
        ...     "table": "orders o",
        ...     "columns": [
        ...         {"column": "o.id", "alias": "order_id"},
        ...         {"column": "o.total", "alias": "order_total"},
        ...         {"column": "u.name", "alias": "customer_name"}
        ...     ],
        ...     "joins": [
        ...         {
        ...             "table": "users u",
        ...             "condition": "o.user_id = u.id",
        ...             "type": "INNER",
        ...             "auto_quote": True
        ...         }
        ...     ],
        ...     "where": ["o.status = 'shipped'", "o.total > 100"],
        ...     "group_by": ["u.name"],
        ...     "having": "SUM(order_total) > 500",
        ...     "order_by": [{"column": "o.total", "order": "DESC"}],
        ...     "limit": 50
        ... }
        >>> generator = SQLGeneratorFromConfig(config)
        >>> sql_query = generator.transform()
        >>> print(sql_query)

        SELECT `o.id` AS `order_id`,
               `o.total` AS `order_total`,
               `u.name` AS `customer_name`
        FROM `orders` o
        INNER JOIN `users` u ON `o`.`user_id` = `u`.`id`
        WHERE o.status = 'shipped' AND o.total > 100
        GROUP BY `u.name`
        HAVING SUM(order_total) > 500
        ORDER BY `o.total` DESC
        LIMIT 50

    Notes:
        - Each config key maps directly to one `_apply_*` method.
        - Only applies supported operations if present in the config.
        - Keeps query building declarative: no need to write SQL strings manually.
        - Acts as a bridge for metadata-driven pipelines, config files, or APIs.
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
    def transform(self) -> str:
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
            >>> sql_query = query_builder.transform()
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

        return self.builder.pretty_print(return_string=True)

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
        
        Official key is 'columns'; 'select' is accepted for backward compatibility.

        Raises:
            AttributeError: If the builder object does not support the 'select' method.

        Example:
            >>> Assuming `self.config` is set to `{"columns": ["name", "age"]}` and `self.builder` has a method
            ... `select`, this method will execute `self.builder.select(["name", "age"])`.
        """
        columns = self.config.get("columns") or self.config.get("select")
        if columns and hasattr(self.builder, "select"):
            self.builder.select(columns)

    def _apply_case_when(self):
        """
        Applies CASE WHEN expressions from the configuration to the query builder.

        This method looks for a "case_when" key in the config. Each entry must
        define:
            - cases (List[Dict[str, str]]): Each dict requires:
                * "condition" (str): The WHEN condition.
                * "result" (str): The THEN result.
            - alias (str): Alias for the CASE expression.
            - else_value (Optional[str]): Optional ELSE value.

        Example config:
            "case_when": [
                {
                    "cases": [
                        {"condition": "status = 'active'", "result": "'Active'"},
                        {"condition": "status = 'inactive'", "result": "'Inactive'"}
                    ],
                    "else_value": "'Unknown'",
                    "alias": "status_label"
                }
            ]

        Example generated SQL:
            CASE
                WHEN status = 'active' THEN 'Active'
                WHEN status = 'inactive' THEN 'Inactive'
                ELSE 'Unknown'
            END AS status_label

        Notes:
            - The CASE expression is added to the SELECT clause.
            - All conditions and results must be valid SQL expressions.
            - Aliases are quoted automatically by the builder for safety.
        """

        case_whens = self.config.get("case_when")
        if case_whens and hasattr(self.builder, "add_case"):
            for case in case_whens:
                cases = case.get("cases", [])
                alias = case.get("alias")
                else_value = case.get("else_value")

                if not cases or not alias:
                    raise ValueError("Each CASE WHEN block must include 'cases' and 'alias'.")

                # Validate each case dict
                for c in cases:
                    if "condition" not in c or "result" not in c:
                        raise ValueError("Each case must have 'condition' and 'result' keys.")

                self.builder.add_case(cases, else_value, alias)

    def _apply_joins(self):
        """
        Applies JOIN clauses from the configuration to the query builder.

        This method looks for a "joins" key in the config. Each entry must
        define:
            - table (str): The table name, optionally with alias
            (e.g., "users u", "sales.orders o").
            - condition (Union[str, List[Union[str, tuple]]]): The ON condition(s).
            Supported formats:
                * str → "t1.id = t2.id"
                * List[str] → ["t1.id = t2.id", "t2.active = 1"]
                * List[tuple] → [("t1.id = t2.id", "AND", "t2.active = 1")]
            - type (Optional[str]): The join type, default "LEFT".
            Supported: "INNER", "LEFT", "RIGHT", "FULL OUTER".
            - auto_quote (Optional[bool]): If True, automatically quote
            identifiers in the condition. Defaults to False.

        Example config:
            "joins": [
                {
                    "table": "users u",
                    "condition": "o.user_id = u.id",
                    "type": "INNER",
                    "auto_quote": True
                },
                {
                    "table": "products p",
                    "condition": ["o.product_id = p.id", "p.active = 1"]
                }
            ]

        Example generated SQL:
            INNER JOIN "users" u ON "o"."user_id" = "u"."id"
            LEFT JOIN "products" p ON o.product_id = p.id AND p.active = 1

        Notes:
            - The config is passed directly into `SelectQueryBuilder.join`.
            - Duplicate JOINs are automatically avoided.
        """

        joins = self.config.get("joins")
        if joins and hasattr(self.builder, "join"):
            for join in joins:
                table = join.get("table")
                condition = join.get("condition")
                join_type = join.get("type", "INNER")
                if table:
                    if join_type.upper() == "CROSS" and not condition:
                        # Allow CROSS JOIN with no condition
                        self.builder.join(table, None, join_type)
                    elif condition:
                        # Normal join with condition
                        self.builder.join(table, condition, join_type)


    def _apply_where(self):
        """
        Applies WHERE conditions from the configuration to the query builder.

        This method looks for a "where" key in the config. Conditions can be
        provided as either a string or a list of strings. Each condition is
        passed into `SelectQueryBuilder.where`.

        Example config:
            "where": "status = 'active'"
            "where": ["age > 30", "country = 'US'"]

        Example generated SQL:
            WHERE status = 'active'
            WHERE age > 30 AND country = 'US'

        Notes:
            - Conditions are validated by `SQLUtils.validate_conditions`.
            - Conditions may include parameterized queries (e.g., "age > ?").
            - Subquery conditions using tuples
            (e.g., ("EXISTS", subquery_builder)) are supported directly by
            `SelectQueryBuilder.where`, though this method currently assumes
            strings in config.
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
        Applies GROUP BY columns from the configuration to the query builder.

        This method looks for a "group_by" key in the config and, if present,
        applies it to the builder’s `group_by` method.

        Example config:
            "group_by": ["department_id", "job_title"]

        Example generated SQL:
            GROUP BY department_id, job_title

        Notes:
            - Columns in `group_by` must either appear in the SELECT clause,
            be valid raw expressions, or be aliased expressions.
            - If a schema is set on the builder, columns are validated
            against it before being applied.
            - Validation is handled by `SQLUtils.validate_group_by`.
        """
        group_by_columns = self.config.get("group_by")
        if group_by_columns and hasattr(self.builder, "group_by"):
            self.builder.group_by(group_by_columns)

    def _apply_having(self):
        """
        Applies HAVING conditions from the configuration to the query builder.

        This method looks for a "having" key in the config and applies it to
        the builder’s `having` method. Conditions can be provided as either
        a single string or a list of strings.

        Example config:
            "having": "SUM(salary) > 50000"
            "having": ["COUNT(*) > 10", "AVG(bonus) > 1000"]

        Example generated SQL:
            HAVING SUM(salary) > 50000
            HAVING COUNT(*) > 10 AND AVG(bonus) > 1000

        Notes:
            - HAVING conditions are validated against GROUP BY columns and
            selected aggregates by `SQLUtils.validate_having`.
            - Conditions must reference either:
                * Columns in the GROUP BY clause
                * Aggregate functions (SUM, COUNT, etc.)
                * Aliases from the SELECT clause
            - Parameters are supported (e.g., "SUM(salary) > ?", params=[50000]),
            though this config-based method assumes inline SQL strings.
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
        Applies ORDER BY clauses from the configuration to the query builder.

        This method looks for an "order_by" key in the config and applies it
        to the builder’s `order_by` method. Ordering can be provided as:
        - A simple string column name (default ASC).
        - A list of strings (multiple columns, all ASC).
        - A list of dicts specifying column, order, and optional null handling.

        Example config:
            "order_by": "name"
            "order_by": ["name", "age"]
            "order_by": [
                {"column": "name", "order": "DESC"},
                {"column": "age", "order": "ASC"},
                {"column": "score", "order": "DESC", "nulls": "LAST"}
            ]

        Example generated SQL:
            ORDER BY "name" ASC
            ORDER BY "name" ASC, "age" ASC
            ORDER BY "name" DESC, "age" ASC, "score" DESC NULLS LAST

        Notes:
            - Uses `SQLUtils.build_order_by_clause` internally to ensure
            correct quoting and validation.
            - Invalid order values raise a ValueError (only ASC/DESC allowed).
            - Nulls handling must be "FIRST" or "LAST" if specified.
        """
        order_by_columns = self.config.get("order_by")
        if order_by_columns and hasattr(self.builder, "order_by"):
            self.builder.order_by(order_by_columns)

    def _apply_limit(self):
        """
        Applies a LIMIT clause from the configuration to the query builder.

        This method looks for a "limit" key in the config and, if present,
        applies it using the builder’s `limit` method. It restricts the number
        of rows returned by the query.

        Example config:
            "limit": 10

        Example generated SQL:
            LIMIT 10

        Notes:
            - The value must be a non-negative integer.
            - Validation is handled by `SQLUtils.validate_limit`.
            - If no "limit" is provided, the query returns all rows by default.
        """
        limit_value = self.config.get("limit")
        if limit_value is not None and hasattr(self.builder, "limit"):
            self.builder.limit(limit_value)

    def _apply_distinct(self):
        """
        Applies DISTINCT or DISTINCT ON clauses from the configuration.

        This method looks for either a "distinct" or "distinct_on" key in the
        config and applies them to the builder.

        Example configs:
            "distinct": True
            "distinct_on": ["id", "created_at"]

        Example generated SQL:
            SELECT DISTINCT "id", "name" FROM "employees"
            SELECT DISTINCT ON ("id", "created_at") "id", "created_at", "name" FROM "orders"

        Notes:
            - "distinct" applies globally across all selected columns.
            - "distinct_on" is supported only in certain databases (e.g., PostgreSQL).
            - Columns in "distinct_on" must also appear in the SELECT clause.
            - If both are provided, DISTINCT ON takes precedence.
        """
        if self.config.get("distinct") and hasattr(self.builder, "set_distinct"):
            self.builder.set_distinct()
        distinct_on = self.config.get("distinct_on")
        if distinct_on and hasattr(self.builder, "distinct_on"):
            self.builder.distinct_on(distinct_on)

    def _apply_ctes(self):
        """
        Applies Common Table Expressions (CTEs) from the configuration.

        This method looks for a "ctes" key in the config. Each CTE must define:
        - "name": the alias for the CTE.
        - "query": either a raw SQL string or a nested config dict.

        Example config:
            "ctes": [
                {"name": "recent_orders", "query": "SELECT * FROM orders WHERE order_date > '2023-01-01'"},
                {"name": "totals", "query": {
                    "query_type": "select",
                    "table": "orders",
                    "select": ["user_id", {"column": "SUM(total)", "alias": "total"}],
                    "group_by": ["user_id"]
                }}
            ]

        Example generated SQL:
            WITH "recent_orders" AS (
                SELECT * FROM orders WHERE order_date > '2023-01-01'
            ),
            "totals" AS (
                SELECT "user_id", SUM("total") AS "total"
                FROM "orders"
                GROUP BY "user_id"
            )

        Notes:
            - Nested config dicts are transformed recursively with SQLGeneratorFromConfig.
            - Aliases are quoted for safety.
            - Duplicate CTE names are ignored.
            - Multiple CTEs are comma-separated in the WITH clause.
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
        Applies UNION or UNION ALL operations from the configuration.

        This method looks for a "unions" key in the config. Each entry must define:
        - "query": either a raw SQL string or a nested config dict.
        - "all" (optional): if True, uses UNION ALL instead of UNION.

        Example config:
            "unions": [
                {
                    "query": {
                        "query_type": "select",
                        "table": "employees",
                        "select": ["id", "name"]
                    },
                    "all": True
                },
                {
                    "query": "SELECT id, name FROM contractors"
                }
            ]

        Example generated SQL:
            SELECT "id", "name" FROM "employees"
            UNION ALL (SELECT "id", "name" FROM "employees")
            UNION (SELECT id, name FROM contractors)

        Notes:
            - Nested config dicts are transformed recursively with SQLGeneratorFromConfig.
            - The `all` flag defaults to False (regular UNION).
            - Queries in the union must project the same number of columns with compatible types.
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
        Applies INTERSECT operations from the configuration.

        This method looks for an "intersects" key in the config. Each entry must
        define a "query", which can be either:
        - A raw SQL string, or
        - A nested config dict that will be transformed into a query.

        Example config:
            "intersects": [
                {
                    "query": {
                        "query_type": "select",
                        "table": "orders",
                        "select": ["id"]
                    }
                },
                {
                    "query": "SELECT id FROM archived_orders"
                }
            ]

        Example generated SQL:
            SELECT "id" FROM "employees"
            INTERSECT (SELECT "id" FROM "orders")
            INTERSECT (SELECT id FROM archived_orders)

        Notes:
            - Nested config dicts are transformed recursively with SQLGeneratorFromConfig.
            - INTERSECT returns only rows common to both queries.
            - Queries must project the same number of columns with compatible types.
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
        Applies EXCEPT operations from the configuration.

        This method looks for an "excepts" key in the config. Each entry must
        define a "query", which can be either:
        - A raw SQL string, or
        - A nested config dict that will be transformed into a query.

        Example config:
            "excepts": [
                {
                    "query": {
                        "query_type": "select",
                        "table": "employees",
                        "select": ["id"]
                    }
                },
                {
                    "query": "SELECT id FROM terminated_employees"
                }
            ]

        Example generated SQL:
            SELECT "id" FROM "employees"
            EXCEPT (SELECT id FROM terminated_employees)

        Notes:
            - Nested config dicts are transformed recursively with SQLGeneratorFromConfig.
            - EXCEPT removes rows returned by the subquery from the main query.
            - Queries must project the same number of columns with compatible types.
            - Behavior may vary slightly by database (e.g., EXCEPT vs MINUS in Oracle).
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
        Applies a PIVOT operation from the configuration.

        Looks for a "pivot" key in the config. The entry must include:
        - "column": the column whose unique values become new columns
        - "values": list of values to pivot into columns
        - "aggregate_function": aggregate to apply to each pivoted column
        - "alias" (optional): alias for the pivoted result

        Example config:
            "pivot": {
                "column": "month",
                "values": ["Jan", "Feb", "Mar"],
                "aggregate_function": "SUM",
                "alias": "monthly_sales"
            }

        Example generated SQL:
            SELECT ...
            FROM (
                "sales"
            ) PIVOT (
                SUM("month") FOR "month" IN ('Jan', 'Feb', 'Mar')
            ) AS "monthly_sales"

        Notes:
            - PIVOT replaces the base table in the FROM clause.
            - Only works in databases that support PIVOT (e.g., SQL Server, Oracle).
            - Use with caution if targeting multiple dialects.
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
        Applies an UNPIVOT operation from the configuration.

        Looks for an "unpivot" key in the config. The entry must include:
        - "value_column": column to hold the unpivoted values
        - "category_column": column to hold the original column names
        - "columns": list of columns to unpivot
        - "alias" (optional): alias for the unpivoted result

        Example config:
            "unpivot": {
                "value_column": "sales",
                "category_column": "month",
                "columns": ["Jan", "Feb", "Mar"],
                "alias": "unpivoted_sales"
            }

        Example generated SQL:
            SELECT ...
            FROM (
                "sales"
            ) UNPIVOT (
                "sales" FOR "month" IN ("Jan", "Feb", "Mar")
            ) AS "unpivoted_sales"

        Notes:
            - UNPIVOT replaces the base table in the FROM clause.
            - Converts wide tables into long (tall) format.
            - Use with caution across dialects (only supported in SQL Server, Oracle).
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
        Applies window functions from the configuration.

        Looks for a "window_functions" key in the config. Each entry must include:
        - "function": the window function name (e.g., ROW_NUMBER, RANK, SUM)
        - "alias": the alias for the computed column
        - "column" (optional): column argument for the function
            * None → functions with no args (e.g., ROW_NUMBER)
            * "*" → COUNT(*)
            * str → SUM("salary"), AVG("score"), etc.
        - "partition_by" (optional): list of columns for PARTITION BY
        - "order_by" (optional): list of dicts for ORDER BY, each dict may include:
            - "column": column name
            - "order": "ASC" or "DESC"
            - "nulls": "FIRST" or "LAST"

        Example config:
            "window_functions": [
                {
                    "function": "ROW_NUMBER",
                    "alias": "row_num",
                    "order_by": [{"column": "id"}]
                },
                {
                    "function": "SUM",
                    "column": "salary",
                    "alias": "total_salary",
                    "partition_by": ["department_id"]
                }
            ]

        Example generated SQL:
            SELECT ...,
                ROW_NUMBER() OVER (ORDER BY "id" ASC) AS "row_num",
                SUM("salary") OVER (PARTITION BY "department_id") AS "total_salary"
            FROM "employees"

        Notes:
            - Multiple window functions can be added in one config.
            - Each is added as a separate column in the SELECT clause.
            - Useful for ranking, running totals, moving averages, etc.
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
