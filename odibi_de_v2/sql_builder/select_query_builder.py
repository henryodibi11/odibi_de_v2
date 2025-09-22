from odibi_de_v2.logger import (
    log_exceptions, log_and_optionally_raise)
from odibi_de_v2.utils import (
    enforce_types, log_call)
from odibi_de_v2.core.enums import ErrorType
from typing import Dict, List, Optional, Union
from odibi_de_v2.core import BaseQueryBuilder
from odibi_de_v2.sql_builder import SQLUtils
import sqlparse

class SelectQueryBuilder(BaseQueryBuilder):
    """
    A helper class for constructing SQL SELECT queries programmatically.

    This class provides methods to dynamically build SELECT queries with
    support for common SQL operations such as JOINs, WHERE clauses, GROUP BY,
    HAVING, ORDER BY, and more. It enables the creation of modular and
    reusable SQL query components, ensuring that queries are well-structured
    and easy to maintain.

    Attributes:
        table_name (str): The main table for the SELECT query.
        quote_style (str): The style of quoting for table and column names
            (default: '"').
        raw_columns (List[str]): The raw column names provided for the SELECT
            clause.
        columns (List[str]): The formatted columns for the SELECT clause,
        including aliasing and aggregates.
        raw_expressions (List[str]): Any raw SQL expressions used in the query.
        joins (List[str]): List of JOIN clauses for the query.
        conditions (List[str]): WHERE conditions applied to the query.
        group_by_columns (List[str]): List of columns used in GROUP BY.
        having_conditions (List[str]): HAVING conditions applied after GROUPBY.
        order_by_clause (str): The ORDER BY clause for sorting.
        limit_value (int): LIMIT value for restricting rows.
        distinct (bool): Flag indicating if the query is DISTINCT.
        parameters (List): Parameters for parameterized queries.
        distinct_on_columns (List[str]): Columns for DISTINCT ON.
        set_operations (List[Tuple[str, SelectQueryBuilder]]): Set operations
            (e.g., UNION, INTERSECT) applied to the query.
        schema (List[str]): List of valid column names for schema validation.
        ctes (List[str]): Common Table Expressions (CTEs) added to the query.

    Example Usage:
        # Initialize the query builder
        builder = SelectQueryBuilder("orders")

        # Add SELECT columns
        builder.select(["id", "customer_id", "total"])

        # Add a WHERE condition
        builder.where("status = 'shipped'")

        # Add a JOIN clause
        builder.join("customers", "orders.customer_id = customers.id")

        # Add GROUP BY and aggregate function
        builder.group_by(
            ["customer_id"]).add_aggregate("SUM", "total", "total_sum")

        # Build the final query
        query = builder.build()
        print(query)

    Generated Query:
        SELECT id, customer_id, total, SUM(total) AS total_sum
        FROM "orders"
        INNER JOIN "customers" ON orders.customer_id = customers.id
        WHERE status = 'shipped'
        GROUP BY customer_id
    """
    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def __init__(self, table_name: str, quote_style: str = '`'):
        log_and_optionally_raise(
            module="SQL_BUILDER",
            component="SelectQueryBuilder",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=f"Initializing SelectQueryBuilder for table: {table_name}",
            level="INFO"
        )
        SQLUtils.validate_table_name(table_name)
        self.quote_style = quote_style
        self.table_name = self._quote_table_with_alias(table_name)
        self.raw_columns = []
        self.columns = []
        self.raw_expressions = []
        self.joins = []
        self.conditions = []
        self.group_by_columns = []
        self.having_conditions = []
        self.order_by_clause = None
        self.limit_value = None
        self.distinct = False
        self.parameters = []
        self.distinct_on_columns = []
        self.set_operations = []
        self.schema = []
        self.ctes = []
        log_and_optionally_raise(
            module="SQL_BUILDER",
            component="SelectQueryBuilder",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=f"Initialized SelectQueryBuilder with table: {self.table_name}",
            level="INFO"
        )
        """
    Initialize the SelectQueryBuilder instance.

    This constructor sets up the query builder by specifying the main table
    and the quoting style for table and column names. It also initializes
    various attributes used to construct the SQL SELECT query.

    Args:
        table_name (str): The name of the main table for the query. It must
            be a valid SQL identifier.
        quote_style (str, optional): The quoting style for table and column
            names. Defaults to double quotes (").
            Supported styles:
                - '"': Double quotes (standard SQL).
                - '[': Square brackets (e.g., for MS SQL Server).

    Attributes:
        table_name (str): The main table for the SELECT query, quoted based on
            the specified quote_style.
        quote_style (str): The chosen quoting style for table and column names.
        raw_columns (List[str]): Unprocessed column names provided for the
            SELECT clause.
        columns (List[str]): Formatted SELECT clause columns, including
            aliasing or aggregates.
        raw_expressions (List[str]): Raw SQL expressions added to the SELECT
            clause.
        joins (List[str]): List of JOIN clauses for the query.
        conditions (List[str]): WHERE conditions applied to the query.
        group_by_columns (List[str]): List of columns used in the GROUP BY
            clause.
        having_conditions (List[str]): HAVING conditions applied after
            GROUP BY.
        order_by_clause (Optional[str]): The ORDER BY clause for sorting rows.
        limit_value (Optional[int]): The LIMIT value for restricting the
            number of rows.
        distinct (bool): Indicates whether the query is DISTINCT.
        parameters (List): Parameters for use in parameterized queries.
        distinct_on_columns (List[str]): Columns used in DISTINCT ON (if any).
        set_operations (List[Tuple[str, SelectQueryBuilder]]):
        List of set operations (e.g., UNION, INTERSECT) applied to the query.
        schema (List[str]): List of valid column names for schema validation.
        ctes (List[str]): List of Common Table Expressions (CTEs) added to
            the query.

    Example:
        # Create a query builder for the "employees" table
        builder = SelectQueryBuilder("employees", quote_style='"')

        # Start building the query
        builder.select(["id", "name"]).where("status = 'active'")

        # Output:
        # SELECT id, name FROM "employees" WHERE status = 'active'
    """
    def _quote_table_with_alias(self, table_name: str) -> str:
            """
            Safely quote schema-qualified table names and handle aliases.
            Supports multi-word table names (e.g., 'kids products kp').

            Rules:
                - If one token → just a table name.
                - If two tokens → first = table, second = alias.
                - If more than two tokens → last = alias, everything before = table.
            """
            parts = table_name.split()
            if len(parts) == 1:
                ident, alias = parts[0], None
            elif len(parts) == 2:
                ident, alias = parts[0], parts[1]
            else:
                ident = " ".join(parts[:-1])
                alias = parts[-1]
            # Quote schema-qualified identifiers
            if "." in ident:
                dotted = ".".join(SQLUtils.quote_column(p, self.quote_style) for p in ident.split("."))
            else:
                dotted = SQLUtils.quote_column(ident, self.quote_style)

            return f"{dotted} {alias}" if alias else dotted
    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def subquery(self, alias: str) -> str:
        """
    Build the current query as a subquery with an alias.

    This method allows you to use the current query as a subquery within a
    larger query. It wraps the generated SQL query in parentheses and assigns
    an alias to it.

    Args:
        alias (str): The alias to assign to the subquery. Must be a valid SQL
            identifier.

    Returns:
        str: The SQL representation of the subquery with the alias.

    Raises:
        ValueError: If the `alias` is not provided or is invalid.

    Examples:
        # Use a query as a subquery
        subquery = SelectQueryBuilder("orders")\
            .select(["user_id", "SUM(total)"])\
            .group_by(["user_id"])\
            .subquery("order_totals")

        # Combine a subquery with another query
        subquery = SelectQueryBuilder("orders")\
            .select(["user_id", "SUM(total)"])\
            .group_by(["user_id"])\
            .subquery("order_totals")
        query = SelectQueryBuilder("users")\
            .select(["users.id", "order_totals.total"])\
            .join(subquery, "users.id = order_totals.user_id")


    Notes:
        - The alias is mandatory and should follow SQL naming conventions.
        - Ensure the query being used as a subquery is fully built and valid.
        - This method is useful for composing complex queries with reusable
            components.
    """
        if not alias:
            raise ValueError("Alias is required for a subquery.")
        return f"({self.build()}) AS {SQLUtils.quote_column(alias,self.quote_style)}"

    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def set_schema(self, schema: List[str]) -> 'SelectQueryBuilder':
        """
    Set the schema to validate column names in the query.

    This method allows you to define a schema (a list of valid column names)
    that will be used to validate column references throughout the query.
    Schema validation ensures that only columns present in the schema can
    be used in SELECT, WHERE, GROUP BY, HAVING, and other clauses.

    Args:
        schema (List[str]): A list of valid column names for the schema.

    Returns:
        SelectQueryBuilder: The updated query builder instance.

    Examples:
        # Set a schema for column validation
        builder.set_schema(["id", "name", "age", "salary", "department_id"])
        builder.select(["id", "name"]).where("age > 30")
        # SELECT id, name
        # FROM "employees"
        # WHERE age > 30

        # Invalid column reference
        builder.select(["invalid_column"])  # Raises ValueError

        # Combine schema validation with other clauses
        builder.set_schema(
            ["id", "name", "department_id"]
            ).group_by(["department_id"])
        # SELECT ...
        # GROUP BY department_id

    Notes:
        - The schema is used for validation in methods like `select`, `where`,
        `group_by`, and `having`.
        - Schema validation raises a `ValueError` if an invalid column is
            referenced.
        - Setting a schema does not modify the query directly; it only affects
            validation.
    """
        self.schema = schema
        return self
    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def select(
        self,
        columns: Optional[List[Union[str, dict, tuple]]] = None
    ) -> "SelectQueryBuilder":
        """
        Add columns to the SELECT clause of the query.

        This method allows you to specify the columns to include in the SELECT
        clause. It supports basic column selection, aliasing, and raw SQL
        expressions for advanced queries. If no columns are specified, a wildcard
        (`*`) is used to select all columns.

        Args:
            columns (Optional[List[Union[str, dict, tuple]]]): A list of columns
                to include in the SELECT clause.
                Supported formats:
                    - `str`: A column name or raw SQL expression
                        (e.g., "id", "SUM(salary)").
                    - `dict`: {"column": str, "alias": str} for aliasing
                        (e.g., {"column": "id", "alias": "employee_id"}).
                    - `tuple`: (column, alias) shorthand for aliasing
                        (e.g., ("id", "employee_id")).
                    - If `None`, selects all columns using `*`.

        Returns:
            SelectQueryBuilder: The updated query builder instance.

        Raises:
            ValueError: If a column specification is invalid or does not match
            the schema (if schema validation is enabled).

        Examples:
            # Basic column selection
            builder.select(["id", "name", "email"])

            # Aliased columns using dictionaries
            builder.select([
                {"column": "id", "alias": "employee_id"},
                {"column": "name", "alias": "full_name"}
            ])

            # Aliased columns using tuples
            builder.select([("id", "employee_id"), ("name", "full_name")])

            # Raw SQL expressions
            builder.select(["SUM(salary)", {"column": "COUNT(*)", "alias": "emp_count"}])

            # Select all columns
            builder.select()

        Notes:
            - If a schema is set using `set_schema`, plain identifiers are validated
            against the schema.
            - Plain identifiers are quoted automatically; raw SQL expressions are
            left untouched.
            - Supports mixing formats (str, dict, tuple) in the same call.
        """
        log_and_optionally_raise(
            module="SQL_BUILDER",
            component="SelectQueryBuilder",
            method="select",
            error_type=ErrorType.NO_ERROR,
            message=f"Adding SELECT columns: {columns}",
            level="INFO"
        )

        if not columns:
            self.raw_columns = ["*"]
            self.columns = ["*"]
        else:
            # Track raw column names for validation
            self.raw_columns = [
                col["column"] if isinstance(col, dict) else
                col[0] if isinstance(col, tuple) else col
                for col in columns
            ]

            # Schema validation (only plain identifiers)
            if self.schema:
                identifiers = [
                    c for c in self.raw_columns
                    if isinstance(c, str) and c.isidentifier()
                ]
                SQLUtils.validate_column_names(identifiers, self.schema)

            # Build formatted columns
            formatted = []
            for col in columns:
                if isinstance(col, dict):
                    base = col["column"]
                    alias = col.get("alias")
                    base_expr = (
                        SQLUtils.quote_column(base, self.quote_style)
                        if base.isidentifier() or "." in base
                        else base
                    )
                    if alias:
                        formatted.append(
                            f"{base_expr} AS {SQLUtils.quote_column(alias, self.quote_style)}"
                        )
                    else:
                        formatted.append(base_expr)

                elif isinstance(col, tuple) and len(col) == 2:
                    base, alias = col
                    base_expr = (
                        SQLUtils.quote_column(base, self.quote_style)
                        if base.isidentifier() or "." in base
                        else base
                    )
                    formatted.append(
                        f"{base_expr} AS {SQLUtils.quote_column(alias, self.quote_style)}"
                    )
                    self.raw_columns.append(alias)

                elif isinstance(col, str):
                    # Quote plain identifiers, keep raw expressions untouched
                    if col.isidentifier() or "." in col:
                        formatted.append(SQLUtils.quote_column(col, self.quote_style))
                    else:
                        formatted.append(col)

                else:
                    raise ValueError(f"Invalid column format: {col}")

            self.columns = formatted

        log_and_optionally_raise(
            module="SQL_BUILDER",
            component="SelectQueryBuilder",
            method="select",
            error_type=ErrorType.NO_ERROR,
            message=f"Formatted SELECT columns: {self.columns}",
            level="INFO"
        )
        return self

    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def add_aggregate(
        self,
        function: str,
        column: str,
        alias: Optional[str] = None
    ) -> 'SelectQueryBuilder':
        """
        Add an aggregate function to the SELECT clause.

        This method allows you to include aggregate functions such as
        `SUM`, `COUNT`, or `AVG` in the SELECT clause. You can optionally
        provide an alias for the aggregate result.

        Args:
            function (str): The aggregate function to apply
                (e.g., "SUM", "COUNT", "AVG").
            column (str): The column to aggregate. This can be a raw column name,
                a valid expression, or '*' for all rows.
            alias (Optional[str], optional): An alias for the aggregate result.
                Defaults to None.

        Returns:
            SelectQueryBuilder: The updated query builder instance.

        Raises:
            ValueError: If `function` or `column` is not provided.

        Examples:
            # Add a SUM aggregate
            builder.add_aggregate("SUM", "salary", "total_salary")

            # Add a COUNT aggregate
            builder.add_aggregate("COUNT", "id", "employee_count")

            # Combine multiple aggregates
            builder.add_aggregate("SUM", "salary", "total_salary"
            ).add_aggregate("COUNT", "*", "total_employees")

        Notes:
            - The column name is quoted using `SQLUtils.quote_column` to handle
            reserved keywords or special characters.
            - If `alias` is not provided, the aggregate will appear without an
            alias in the SELECT clause.
            - Special case: `COUNT(*)` and similar functions are supported directly
            without quoting the asterisk.
        """
        if not function or column is None:
            raise ValueError("Function and column are required for aggregates.")

        # Handle COUNT(*) and similar
        if column == "*":
            agg_col = "*"
            log_and_optionally_raise(
                module="SQL_BUILDER",
                component="SelectQueryBuilder",
                method="add_aggregate",
                error_type=ErrorType.NO_ERROR,
                message="Aggregate column is '*', no quoting applied.",
                level="DEBUG"
            )
        else:
            agg_col = SQLUtils.quote_column(column, self.quote_style)
            log_and_optionally_raise(
                module="SQL_BUILDER",
                component="SelectQueryBuilder",
                method="add_aggregate",
                error_type=ErrorType.NO_ERROR,
                message=f"Quoted column: {column} -> {agg_col}",
                level="DEBUG"
            )

        # Build the aggregate expression
        aggregate_expression = f"{function}({agg_col})"

        # Quote alias if provided
        if alias:
            quoted_alias = SQLUtils.quote_column(alias, self.quote_style)
            aggregate_expression += f" AS {quoted_alias}"
            log_and_optionally_raise(
                module="SQL_BUILDER",
                component="SelectQueryBuilder",
                method="add_aggregate",
                error_type=ErrorType.NO_ERROR,
                message=f"Quoted alias: {alias} -> {quoted_alias}",
                level="DEBUG"
            )

        # Avoid duplicates in the SELECT clause
        if aggregate_expression not in self.columns:
            self.columns.append(aggregate_expression)
            log_and_optionally_raise(
                module="SQL_BUILDER",
                component="SelectQueryBuilder",
                method="add_aggregate",
                error_type=ErrorType.NO_ERROR,
                message=f"Added aggregate: {aggregate_expression}",
                level="INFO"
            )
        else:
            log_and_optionally_raise(
                module="SQL_BUILDER",
                component="SelectQueryBuilder",
                method="add_aggregate",
                error_type=ErrorType.NO_ERROR,
                message=f"Aggregate already exists, skipping: {aggregate_expression}",
                level="INFO"
            )

        return self
    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def add_case(
        self,
        cases: List[Dict[str, str]],
        else_value: Optional[str] = None,
        alias: Optional[str] = None
    ) -> 'SelectQueryBuilder':
        """
        Add a CASE statement to the SELECT clause.

        This method allows you to define conditional logic using a CASE
        statement. You can specify multiple WHEN-THEN conditions, an optional
        ELSE value, and an alias for the result.

        Args:
            cases (List[Dict[str, str]]): A list of dictionaries defining the
            CASE conditions.
                Each dictionary must include:
                - "condition" (str): The WHEN condition.
                - "result" (str): The THEN result.
            else_value (Optional[str], optional):
            The value to use if no conditions are met.
                Defaults to None, which excludes an ELSE clause.
            alias (Optional[str], optional): An alias for the CASE result.
                Defaults to None.

        Returns:
            SelectQueryBuilder: The updated query builder instance.

        Raises:
            ValueError: If no conditions are provided or if any entry in
            `cases` is missing a "condition" or "result" key.

        Examples:
            # Simple CASE statement
            builder.add_case(
                cases=[
                    {"condition": "status = 'active'",
                    "result": "'Active'"},
                    {"condition": "status = 'inactive'",
                    "result": "'Inactive'"}
                ],
                else_value="'Unknown'",
                alias="status_label"
            )

            # Combine with other SELECT clauses
            builder.select(["id", "name"]).add_case(
                cases=[
                    {"condition": "age < 18", "result": "'Minor'"},
                    {"condition": "age >= 18", "result": "'Adult'"}
                ],
                alias="age_group"
            )

        Notes:
            - The CASE statement is added to the SELECT clause alongside other
            columns.
            - Conditions and results must be valid SQL expressions.
            - The alias is quoted using `SQLUtils.quote_column` to handle
            reserved keywords or special characters.
        """
        if not cases:
            raise ValueError(
                "At least one WHEN condition is required for a CASE statement."
                )

        case_expression = "CASE"
        for case in cases:
            if "condition" not in case or "result" not in case:
                raise ValueError(
                    "Each CASE entry must have 'condition' and 'result' keys.")
            case_expression += f" WHEN {case['condition']} THEN {case['result']}"

        if else_value:
            case_expression += f" ELSE {else_value}"

        case_expression += " END"
        if alias:
            case_expression += f" AS {SQLUtils.quote_column(alias, self.quote_style)}"
        if case_expression not in self.columns:
            self.columns.append(case_expression)
            log_and_optionally_raise(
                module="SQL_BUILDER",
                component="SelectQueryBuilder",
                method="add_case",
                error_type=ErrorType.NO_ERROR,
                message=f"Added CASE statement: {case_expression}",
                level="INFO"
            )
        else:
            log_and_optionally_raise(
                module="SQL_BUILDER",
                component="SelectQueryBuilder",
                method="add_case",
                error_type=ErrorType.NO_ERROR,
                message=f"CASE statement already exists, skipping: {case_expression}",
                level="INFO"
            )
        return self
    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def where(
        self,
        condition: Union[str, tuple],
        params: Optional[List] = None
    ) -> 'SelectQueryBuilder':
        """
    Add a WHERE condition to the query.

    This method allows you to specify conditions for filtering rows in the
    query. It supports raw SQL conditions as strings, parameterized queries,
    and subquery conditions using tuples.

    Args:
        condition (Union[str, tuple]): The condition to apply.
        Supported formats:
            - `str`: A raw SQL condition (e.g., "status = 'active'").
            - `tuple`: A subquery condition where the tuple format is:
                (operator: str, subquery: SelectQueryBuilder).
                Example: ("EXISTS", subquery_instance)
        params (Optional[List]): A list of parameters to include with a
        parameterized query. Defaults to None.

    Returns:
        SelectQueryBuilder: The updated query builder instance.

    Raises:
        ValueError: If the condition is invalid
            (e.g., empty or not a string/tuple).

    Examples:
        # Simple condition
        builder.where("status = 'active'")
        # WHERE status = 'active'

        # Adding multiple conditions
        builder.where("age > 30").where("country = 'US'")
        # WHERE age > 30 AND country = 'US'

        # Using a subquery with EXISTS
        subquery = SelectQueryBuilder("orders")
        .select(["1"]).where("amount > 100")
        builder.where(("EXISTS", subquery))
        # WHERE EXISTS (SELECT 1 FROM "orders" WHERE amount > 100)

        # Parameterized query
        builder.where("age > ?", params=[30])
        # WHERE age > ?

    Notes:
        - The method validates all conditions using
            `SQLUtils.validate_conditions`.
        - When using subqueries, ensure that the subquery is fully built before
            adding it to the main query.
        - The `params` argument supports SQL injection protection for
        parameterized queries.
    """
        log_and_optionally_raise(
            module="SQL_BUILDER",
            component="SelectQueryBuilder",
            method="where",
            error_type=ErrorType.NO_ERROR,
            message=f"Adding WHERE condition: {condition}",
            level="INFO"
        )
        if isinstance(condition, tuple):  # Subquery condition
            operator, subquery = condition
            condition_str = f"{operator} ({subquery.build()})"
        else:
            condition_str = condition
        # Validate and avoid duplicate conditions
        SQLUtils.validate_conditions([condition_str])
        if condition_str not in self.conditions:
            self.conditions.append(condition_str)

        if params:
            self.parameters.extend(params)
        log_and_optionally_raise(
            module="SQL_BUILDER",
            component="SelectQueryBuilder",
            method="where",
            error_type=ErrorType.NO_ERROR,
            message=f"WHERE conditions: {self.conditions}",
            level="INFO"
        )
        return self

    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def join(
        self,
        table: str,
        condition: Union[str, List[Union[str, tuple]]],
        join_type: str = "LEFT",
        auto_quote: bool = False
    ) -> 'SelectQueryBuilder':
        """
        Add a JOIN clause to the query.

        This method allows you to specify JOIN operations to combine data from
        multiple tables. It supports INNER, LEFT, RIGHT, and FULL OUTER JOIN types,
        and allows for complex join conditions.

        Args:
            table (str): The name of the table to join with. This can include
                aliases (e.g., "users u").
            condition (Union[str, List[Union[str, tuple]]]): The join condition(s).
                Supported formats:
                    - `str`: A simple join condition
                        (e.g., "users.id = orders.user_id").
                    - `List[str]`: A list of AND-ed conditions
                        (e.g., ["t1.id = t2.id", "t1.status = 'active'"]).
                    - `List[tuple]`: A nested condition structure for complex logic.
                        Example:
                            [("t1.id = t2.id", "AND", "t1.status = 'active'")].
            join_type (str, optional): The type of JOIN operation. Defaults to "LEFT".
                Supported types:
                    - "INNER"
                    - "LEFT"
                    - "RIGHT"
                    - "FULL OUTER"
            auto_quote (bool, optional): If True, automatically quote identifiers
                inside the join condition (e.g., "users.id" → "\"users\".\"id\"").
                Defaults to False (raw passthrough).

        Returns:
            SelectQueryBuilder: The updated query builder instance.

        Raises:
            ValueError: If the `join_type` is invalid or the `condition` is
                not properly formatted.

        Examples:
            # Simple INNER JOIN (raw conditions)
            builder.join("users", "orders.user_id = users.id", join_type="INNER")

            # LEFT JOIN with multiple conditions
            builder.join(
                "products",
                ["orders.product_id = products.id", "products.active = 1"],
                join_type="LEFT"
            )

            # Complex condition using a nested structure
            builder.join(
                "transactions",
                [("t1.id = t2.id", "AND", "t1.status = 'active'")],
                join_type="RIGHT"
            )

            # Auto-quoted identifiers
            builder.join("orders o", "users.id = o.user_id", auto_quote=True)
            # Produces: LEFT JOIN "orders" o ON "users"."id" = "o"."user_id"

        Notes:
            - The `condition` argument is parsed using `SQLUtils.parse_conditions`,
            which supports nested logic and optional auto-quoting.
            - The `table` argument is quoted and may include schema and/or alias.
        """
        log_and_optionally_raise(
            module="SQL_BUILDER",
            component="SelectQueryBuilder",
            method="join",
            error_type=ErrorType.NO_ERROR,
            message=f"Adding JOIN: {table}, Condition: {condition}, Type: {join_type}, AutoQuote: {auto_quote}",
            level="INFO"
        )

        parsed_condition = SQLUtils.parse_conditions(condition, self.quote_style, auto_quote=auto_quote)
        join_clause = f"{join_type.upper()} JOIN {self._quote_table_with_alias(table)} ON {parsed_condition}"

        # Avoid duplicate JOINs
        if join_clause not in self.joins:
            self.joins.append(join_clause)
            log_and_optionally_raise(
                module="SQL_BUILDER",
                component="SelectQueryBuilder",
                method="join",
                error_type=ErrorType.NO_ERROR,
                message=f"JOIN clauses: {self.joins}",
                level="INFO"
            )

        return self


    def having(
        self,
        condition: str,
        params: Optional[List] = None
    ) -> "SelectQueryBuilder":
        """
        Add a HAVING clause to the query.

        This method allows you to specify conditions for filtering grouped
        results in the query. The HAVING clause is applied after the GROUP BY
        clause and is typically used with aggregate functions.

        Args:
            condition (str): The condition to apply in the HAVING clause. This
                can include aggregate functions (e.g., "SUM(salary) > 50000").
            params (Optional[List], optional): A list of parameters for a
                parameterized query. Defaults to None.

        Returns:
            SelectQueryBuilder: The updated query builder instance.

        Raises:
            ValueError: If the condition is invalid or references columns not
            included in the GROUP BY clause (when validation is enabled).

        Examples:
            # Simple HAVING clause
            builder.group_by(
                ["department_id"]).add_aggregate(
                    "SUM", "salary",
                    "total_salary").having("SUM(salary) > 50000")

            # Parameterized query
            builder.having("SUM(salary) > ?", params=[50000])

            # Combining GROUP BY and HAVING with validation
            builder.set_schema(
                ["department_id", "salary"]
                ).group_by(["department_id"]).having("COUNT(*) > 10")

        Notes:
            - If `group_by_columns` are set, this method validates that the
                HAVING
            condition references valid columns or aggregates using
                `SQLUtils.validate_having`.
            - Parameters passed via `params` can be used to construct secure,
            parameterized queries.
        """
        SQLUtils.validate_conditions([condition])

        selected_aggregates = [
            col.split(" AS ")[-1].strip() if " AS " in col else col
            for col in self.columns
        ]
        selected_aggregates.extend(
            [
                col.split(" AS ")[0].strip() if " AS " in col else col
                for col in self.columns
            ]
        )

        # Validate HAVING against GROUP BY and selected aggregates
        if self.group_by_columns:
            SQLUtils.validate_having(
                [condition],
                self.group_by_columns,
                selected_aggregates
            )

        # Avoid adding duplicate conditions
        if condition not in self.having_conditions:
            self.having_conditions.append(condition)

        if params:
            self.parameters.extend(params)

        return self
    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def group_by(self, columns: List[str]) -> 'SelectQueryBuilder':
        """
    Add a GROUP BY clause to the query.

    This method specifies the columns for grouping results in the query.
    It supports validation against a schema if one is set and ensures that all
    GROUP BY columns are either part of the SELECT clause or valid raw
    expressions.

    Args:
        columns (List[str]): A list of column names to group by. Each column
            must be valid in the context of the query.

    Returns:
        SelectQueryBuilder: The updated query builder instance.

    Raises:
        ValueError: If any column is invalid or missing from the SELECT clause
        and raw expressions (when schema validation is enabled).

    Examples:
        # Simple GROUP BY
        builder.group_by(["department_id", "job_title"])

        # Adding GROUP BY to an existing query
        builder.select(["department_id", "job_title", "SUM(salary)"])
        .group_by(["department_id", "job_title"])

        # Schema validation enabled
        builder.set_schema(["department_id", "job_title", "salary"])
        .group_by(["department_id"])

    Notes:
        - When a schema is set with `set_schema`, this method validates that
        all columns exist in the schema.
        - GROUP BY columns must also appear in the SELECT clause or as raw
        expressions to be valid in SQL.
        - The columns are formatted using the `SQLUtils.format_columns` method.
    """
        log_and_optionally_raise(
            module="SQL_BUILDER",
            component="SelectQueryBuilder",
            method="group_by",
            error_type=ErrorType.NO_ERROR,
            message=f"Adding GROUP BY columns: {columns}",
            level="INFO"
        )
        if self.schema:
            SQLUtils.validate_column_names(columns, self.schema)
        SQLUtils.validate_group_by(
            self.raw_columns, columns, self.raw_expressions)
        self.group_by_columns = SQLUtils.format_columns(
            columns, self.quote_style)
        log_and_optionally_raise(
            module="SQL_BUILDER",
            component="SelectQueryBuilder",
            method="group_by",
            error_type=ErrorType.NO_ERROR,
            message=f"Formatted GROUP BY columns: {self.group_by_columns}",
            level="INFO"
        )
        return self
    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def order_by(
        self,
        columns: List[Union[str, dict]]
    ) -> 'SelectQueryBuilder':
        """
        Add an ORDER BY clause to the query.

        This method specifies how the results should be ordered by one or more
        columns. It supports specifying the order (ASC or DESC) for each
        column.

        Args:
            columns (List[Union[str, dict]]): A list of columns or
            dictionaries specifying
                order. Supported formats:
                - `str`: A single column name (default order: ASC).
                - `dict`: A dictionary specifying the column and order.
                Example:
                    {"column": "name", "order": "DESC"}.

        Returns:
            SelectQueryBuilder: The updated query builder instance.

        Raises:
            ValueError: If the column specification is invalid or if the order
            is not "ASC" or "DESC".

        Examples:
            # Simple ORDER BY with default ASC
            builder.order_by(["name", "age"])
            # ORDER BY name ASC, age ASC

            # ORDER BY with custom sorting
            builder.order_by(
                [{"column": "name", "order": "DESC"},
                {"column": "age", "order": "ASC"}])
            # ORDER BY name DESC, age ASC

            # Combine ORDER BY with other clauses
            builder.select(["id", "name"]).order_by(["name"])
            # SELECT id, name
            # ORDER BY name ASC

        Notes:
            - The `SQLUtils.build_order_by_clause` method is used to construct
            the ORDER BY clause, ensuring proper quoting and validation of
            column names.
            - If no columns are specified, the query will not include an ORDER
            BY clause.
        """
        self.order_by_clause = SQLUtils.build_order_by_clause(
            columns, self.quote_style)
        return self
    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def pivot(
        self,
        column: str,
        values: List[str],
        aggregate_function: str,
        alias: Optional[str] = None
    ) -> 'SelectQueryBuilder':
        """
        Add a PIVOT operation to the query.

        This method allows you to transform rows into columns based on the
        unique values of a specified column, applying an aggregate function to
        the pivoted values. You can also provide an alias for the pivoted
        result.

        Args:
            column (str): The column whose unique values will become new
                columns.
            values (List[str]): A list of values from the `column` to pivot
                into new columns.
            aggregate_function (str): The aggregate function to apply to the
                pivoted values (e.g., "SUM", "COUNT").
            alias (Optional[str], optional): An alias for the pivoted result.
                Defaults to None.

        Returns:
            SelectQueryBuilder: The updated query builder instance.

        Raises:
            ValueError: If any required argument is missing or if the
            `values` list is empty.

        Examples:
            # Simple pivot
            builder.pivot(
                column="month",
                values=["Jan", "Feb", "Mar"],
                aggregate_function="SUM",
                alias="monthly_totals"
            )

            # Combine PIVOT with other clauses
            builder.select(["product_id"]).pivot(
                column="month",
                values=["Jan", "Feb", "Mar"],
                aggregate_function="SUM"
            ).where("year = 2023")

        Notes:
            - The `column` and `values` arguments define the pivot structure.
            - The aggregate function must be valid for the database engine
                used.
            - The alias, if provided, is quoted using `SQLUtils.quote_column`
                for safety.
            - PIVOT modifies the query's `FROM` clause, replacing the original
                table reference.
        """
        if not column or not values or not aggregate_function:
            raise ValueError(
                "Column, values, and aggregate_function are required for pivot.")

        pivot_columns = ", ".join(f"'{v}'" for v in values)
        pivot_expression = (
            f"PIVOT ({aggregate_function}({SQLUtils.quote_column(column, self.quote_style)}) "
            f"FOR {SQLUtils.quote_column(column, self.quote_style)} IN ({pivot_columns}))"
        )

        if alias:
            pivot_expression += f" AS {SQLUtils.quote_column(alias, self.quote_style)}"

        # Wrap the original table in the pivot expression
        self.table_name = f"({self.table_name}) {pivot_expression}"
        log_and_optionally_raise(
            module="SQL_BUILDER",
            component="SelectQueryBuilder",
            method="pivot",
            error_type=ErrorType.NO_ERROR,
            message=f"Added PIVOT operation: {pivot_expression}",
            level="INFO"
        )

        return self
    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def unpivot(
        self,
        value_column: str,
        category_column: str,
        columns: List[str],
        alias: Optional[str] = None
    ) -> 'SelectQueryBuilder':
        """
    Add an UNPIVOT operation to the query.

    This method allows you to transform columns into rows, creating a key-value
    representation of the specified columns. You can provide an alias for the
    unpivoted result.

    Args:
        value_column (str): The name of the column to hold the unpivoted
            values.
        category_column (str): The name of the column to hold the category
            labels (original column names).
        columns (List[str]): A list of column names to unpivot.
        alias (Optional[str], optional): An alias for the unpivoted table.
            Defaults to None.

    Returns:
        SelectQueryBuilder: The updated query builder instance.

    Raises:
        ValueError: If `columns` is empty or any required argument is missing.

    Examples:
        # Simple unpivot
        builder.unpivot(
            value_column="sales",
            category_column="month",
            columns=["Jan", "Feb", "Mar"],
            alias="unpivoted_sales"
        )
        # SELECT ...
        # UNPIVOT (sales FOR month IN (Jan, Feb, Mar)) AS unpivoted_sales

        # Combine UNPIVOT with other clauses
        builder.select(["product_id"]).unpivot(
            value_column="sales",
            category_column="month",
            columns=["Jan", "Feb", "Mar"]
        ).where("year = 2023")
        # SELECT product_id, ...
        # UNPIVOT (sales FOR month IN (Jan, Feb, Mar))
        # WHERE year = 2023

    Notes:
        - The `value_column` and `category_column` define the structure of the
        unpivoted table.
        - The column names in `columns` must exist in the current table or
        schema.
        - The alias, if provided, is quoted using `SQLUtils.quote_column` for
            safety.
        - UNPIVOT modifies the query’s `FROM` clause, replacing the original
            table reference.
    """
        if not value_column or not category_column or not columns:
            raise ValueError(
                "Value column, category column, and columns are required for unpivot.")

        unpivot_columns = ", ".join(SQLUtils.quote_column(col, self.quote_style) for col in columns)
        unpivot_expression = (
            f"UNPIVOT ({SQLUtils.quote_column(value_column, self.quote_style)} "
            f"FOR {SQLUtils.quote_column(category_column, self.quote_style)} IN ({unpivot_columns}))"
        )

        if alias:
            unpivot_expression += f" AS {SQLUtils.quote_column(alias, self.quote_style)}"

        # Wrap the original table in the unpivot expression
        self.table_name = f"({self.table_name}) {unpivot_expression}"
        log_and_optionally_raise(
            module="SQL_BUILDER",
            component="SelectQueryBuilder",
            method="unpivot",
            error_type=ErrorType.NO_ERROR,
            message=f"Added UNPIVOT operation: {unpivot_expression}",
            level="INFO"
        )

        return self

    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def limit(self, value: int) -> 'SelectQueryBuilder':
        """
    Add a LIMIT clause to the query.

    This method restricts the number of rows returned by the query. It ensures
    the LIMIT value is a non-negative integer.

    Args:
        value (int): The maximum number of rows to return.
        Must be a non-negative integer.

    Returns:
        SelectQueryBuilder: The updated query builder instance.

    Raises:
        ValueError: If the `value` is not a non-negative integer.

    Examples:
        # Limit the results to 10 rows
        builder.limit(10)
        # LIMIT 10

        # Combine LIMIT with other clauses
        builder.select(["id", "name"]).where("age > 30").limit(5)
        # SELECT id, name
        # WHERE age > 30
        # LIMIT 5

    Notes:
        - The `SQLUtils.validate_limit` method ensures the `value` is valid
        before adding it to the query.
        - If no LIMIT is specified, the query will return all rows by default.
    """
        SQLUtils.validate_limit(value)
        self.limit_value = value
        return self

    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def union(
        self,
        query: 'SelectQueryBuilder',
        all: bool = False
    ) -> 'SelectQueryBuilder':
        """
    Combine the current query with another using a UNION operation.

    This method allows you to merge the results of two queries.
    The `UNION` operator removes duplicate rows by default. Use `UNION ALL` to
    include all rows, including  duplicates.

    Args:
        query (SelectQueryBuilder): The query to combine with the current
        query. Must be an instance of `SelectQueryBuilder`.
        all (bool, optional): If True, uses `UNION ALL` to include duplicate
            rows. Defaults to False.

    Returns:
        SelectQueryBuilder: The updated query builder instance.

    Raises:
        ValueError: If the provided `query` is not a valid instance of
        `SelectQueryBuilder`.

    Examples:
        # Basic UNION
        query1 = SelectQueryBuilder("employees").select(["id", "name"])
        query2 = SelectQueryBuilder("contractors").select(["id", "name"])
        query1.union(query2)

        # UNION ALL
        query1.union(query2, all=True)


        # Combine UNION with other clauses
        query1.union(query2).order_by(["name"])


    Notes:
        - The `UNION` operation requires that both queries have the same
            number of columns with compatible data types.
        - Ensure the `query` passed to this method is fully built before
            calling `union`.
        - This method appends the UNION clause to the query and combines any
            parameters from the second query with the current query's
            parameters.
    """
        operation = "UNION ALL" if all else "UNION"
        self.set_operations.append((operation, query))
        self.parameters.extend(query.get_parameters())  # Merge parameters
        return self

    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def intersect(self, query: 'SelectQueryBuilder') -> 'SelectQueryBuilder':
        """
    Combine the current query with another using an INTERSECT operation.

    This method allows you to find the common rows between two queries. The
    `INTERSECT` operator includes only rows that are present in both queries.

    Args:
        query (SelectQueryBuilder): The query to intersect with the current
        query. Must be an instance of `SelectQueryBuilder`.

    Returns:
        SelectQueryBuilder: The updated query builder instance.

    Raises:
        ValueError: If the provided `query` is not a valid instance of
        `SelectQueryBuilder`.

    Examples:
        # Basic INTERSECT
        query1 = SelectQueryBuilder("employees").select(["id", "name"])
        query2 = SelectQueryBuilder("contractors").select(["id", "name"])
        query1.intersect(query2)

        # Combine INTERSECT with other clauses
        query1.intersect(query2).order_by(["name"])

    Notes:
        - The `INTERSECT` operation requires that both queries have the same
        number of columns with compatible data types.
        - Ensure the `query` passed to this method is fully built before
            calling `intersect`.
        - This method appends the INTERSECT clause to the query and combines
            any parameters from the second query with the current query's
            parameters.
    """
        self.set_operations.append(("INTERSECT", query))
        self.parameters.extend(query.get_parameters())  # Merge parameters
        return self

    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def distinct_on(self, columns: List[str]) -> 'SelectQueryBuilder':
        """
    Add a DISTINCT ON clause to the query.

    This method enables the query to return only the first row for each unique
    combination of values in the specified columns. It is particularly useful
    for deduplication based on specific columns.

    Args:
        columns (List[str]): A list of column names to apply the DISTINCT ON
            operation. These columns must exist in the SELECT clause.

    Returns:
        SelectQueryBuilder: The updated query builder instance.

    Raises:
        ValueError: If no columns are provided or if any specified column
        is invalid.

    Examples:
        # Apply DISTINCT ON to a single column
        builder.distinct_on(["id"])
        # SELECT DISTINCT ON (id) ...

        # Apply DISTINCT ON to multiple columns
        builder.distinct_on(["id", "created_at"])
        # SELECT DISTINCT ON (id, created_at) ...

        # Combine DISTINCT ON with other clauses
        builder.select(
            ["id", "name", "created_at"]).distinct_on(["id"]).order_by(
                [{"column": "created_at", "order": "DESC"}])
        # SELECT DISTINCT ON (id) id, name, created_at
        # ORDER BY created_at DESC

    Notes:
        - The DISTINCT ON clause is only supported by certain databases like
        PostgreSQL.
        - The columns specified in `distinct_on` must be included in the
        SELECT clause and in the ORDER BY clause for correct results.
        - This method is a variant of DISTINCT that allows finer control over
        deduplication.
    """
        log_and_optionally_raise(
            module="SQL_BUILDER",
            component="SelectQueryBuilder",
            method="distinct_on",
            error_type=ErrorType.NO_ERROR,
            message=f"Adding DISTINCT ON columns: {columns}",
            level="INFO"
        )
        if not columns or not all(isinstance(col, str) for col in columns):
            raise ValueError("DISTINCT ON requires a non-empty list of string column names.")
        self.distinct_on_columns = SQLUtils.format_columns(
            columns, self.quote_style)
        log_and_optionally_raise(
            module="SQL_BUILDER",
            component="SelectQueryBuilder",
            method="distinct_on",
            error_type=ErrorType.NO_ERROR,
            message=f"Formatted DISTINCT ON columns: {self.distinct_on_columns}",
            level="INFO"
        )
        return self
    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def set_distinct(self) -> "SelectQueryBuilder":
        """
    Add a DISTINCT clause to the query.

    This method ensures that the query returns only unique rows by adding a
    `DISTINCT` keyword to the SELECT clause.

    Returns:
        SelectQueryBuilder: The updated query builder instance.

    Examples:
        # Use DISTINCT in a query
        builder.select(["id", "name"]).distinct()
        # SELECT DISTINCT id, name
        # FROM "employees"

        # Combine DISTINCT with other clauses
        builder.select(["id", "name"]).distinct().where("status = 'active'")
        # SELECT DISTINCT id, name
        # FROM "employees"
        # WHERE status = 'active'

    Notes:
        - The `DISTINCT` keyword applies to all selected columns.
        - DISTINCT is typically used to remove duplicate rows from the result
        set.
        - This method modifies the SELECT clause without affecting other parts
        of the query.
    """
        self.distinct = True
        log_and_optionally_raise(
            module="SQL_BUILDER",
            component="SelectQueryBuilder",
            method="set_distinct",
            error_type=ErrorType.NO_ERROR,
            message="Enabled DISTINCT for the query.",
            level="INFO"
        )
        return self

    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def except_query(
        self,
        query: 'SelectQueryBuilder'
    ) -> 'SelectQueryBuilder':
        """
    Combine the current query with another using an EXCEPT operation.

    This method allows you to exclude rows in the second query from the current
    query. The `EXCEPT` operator returns rows present in the current query but
    not in the second query.

    Args:
        query (SelectQueryBuilder): The query to exclude from the current
            query. Must be an instance of `SelectQueryBuilder`.

    Returns:
        SelectQueryBuilder: The updated query builder instance.

    Raises:
        ValueError: If the provided `query` is not a valid instance of
        `SelectQueryBuilder`.

    Examples:
        # Basic EXCEPT
        query1 = SelectQueryBuilder("employees").select(["id", "name"])
        query2 = SelectQueryBuilder(
            "terminated_employees").select(["id", "name"])
        query1.except_query(query2)

        # Combine EXCEPT with other clauses
        query1.except_query(query2).order_by(["name"])

    Notes:
        - The `EXCEPT` operation requires that both queries have the same
        number of columns with compatible data types.
        - Ensure the `query` passed to this method is fully built before
            calling `except_query`.
        - This method appends the EXCEPT clause to the query and combines any
            parameters from the second query with the current query's
            parameters.
    """
        self.set_operations.append(("EXCEPT", query))
        self.parameters.extend(query.get_parameters())  # Merge parameters
        return self

    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def with_cte(
        self, name: str,
        query: Union['SelectQueryBuilder', str]
    ) -> 'SelectQueryBuilder':
        """
        Add a Common Table Expression (CTE) to the query.

        This method allows you to define and use a CTE in the query. CTEs are
        temporary result sets that can be referenced in the main query for
        improved readability and modularity.

        Args:
            name (str): The name of the CTE. Must be a valid SQL identifier.
            query (Union[SelectQueryBuilder, str]): The CTE query.
            Can be either:
                - An instance of `SelectQueryBuilder`, which will be built
                into a subquery.
                - A raw SQL string representing the CTE.

        Returns:
            SelectQueryBuilder: The updated query builder instance.

        Raises:
            ValueError: If the `name` is invalid or if the `query` is not a
            valid `SelectQueryBuilder` or string.

        Examples:
            # Using a subquery as a CTE
            subquery = SelectQueryBuilder(
                "orders").select(["user_id",
                "SUM(total)"]).group_by(["user_id"])
            builder.with_cte(
                "order_totals",
                subquery).select(["*"])

            # Using a raw SQL string as a CTE
            builder.with_cte(
                "recent_orders",
                "SELECT * FROM orders WHERE order_date > '2023-01-01'").select(
                    ["*"])

        Notes:
            - Multiple CTEs can be added by calling `with_cte` repeatedly.
            - The `name` is quoted using `SQLUtils.quote_column` to handle
            reserved keywords or special characters.
            - Ensure the main query references the CTE name in the FROM clause
            or a JOIN.
        """
        # Validate inputs
        if not name:
            raise ValueError("CTE name cannot be empty.")
        if not isinstance(query, (SelectQueryBuilder, str)):
            raise ValueError(
                "Query must be a SelectQueryBuilder or a SQL string.")

        # Build the CTE query string
        if isinstance(query, SelectQueryBuilder):
            query = query.build()

        cte_string = f"{SQLUtils.quote_column(name, self.quote_style)} AS ({query})"

        # Avoid duplicate CTEs
        if cte_string not in self.ctes:
            self.ctes.append(cte_string)

        return self
    @log_call(module="SQL_BUILDER", component="SelectQueryBuilder")
    @enforce_types()
    @log_exceptions(
        module="SQL_BUILDER",
        component="SelectQueryBuilder",
        error_type=ErrorType.Runtime_Error,
        raise_type=RuntimeError)
    def with_window_function(
        self,
        column: Optional[str],
        function: str,
        alias: str,   # <-- moved up
        partition_by: Optional[List[str]] = None,
        order_by: Optional[List[Union[str, dict]]] = None,
    ) -> "SelectQueryBuilder":
        """
        Add a window function to the SELECT clause.

        This method allows you to apply analytic functions (e.g., ROW_NUMBER,
        RANK, SUM) over a specified window of rows. It supports partitioning and
        ordering to define the scope of the window and allows advanced ordering
        options like null handling.

        Args:
            column (Optional[str]): The column to apply the window function to.
                - None for functions with no argument (e.g., ROW_NUMBER()).
                - "*" for COUNT(*).
                - A column name for functions like SUM, AVG, RANK, etc.
            function (str): The window function to apply (e.g., "ROW_NUMBER",
                "RANK", "SUM").
            alias (str): An alias for the window function result.
            partition_by (Optional[List[str]], optional): A list of columns to
                partition the data. Defaults to None.
            order_by (Optional[List[Union[str, dict]]], optional): A list of
                columns or dictionaries specifying ordering within each partition.
                Supported formats:
                    - `str`: A column name (default order: ASC).
                    - `dict`: {"column": str, "order": "ASC|DESC", "nulls": "FIRST|LAST"}.
                Defaults to None.

        Returns:
            SelectQueryBuilder: The updated query builder instance.

        Raises:
            ValueError: If `function` or `alias` is missing, or if `order_by`
            has an invalid format.

        Examples:
            # ROW_NUMBER() window function with ORDER BY
            builder.with_window_function(
                column=None,
                function="ROW_NUMBER",
                alias="row_num",
                order_by=[{"column": "id"}]
            )
            # SELECT ..., ROW_NUMBER() OVER (ORDER BY "id" ASC) AS "row_num"

            # SUM() window function with PARTITION BY
            builder.with_window_function(
                column="salary",
                function="SUM",
                alias="total_salary",
                partition_by=["department_id"]
            )
            # SELECT ..., SUM("salary") OVER (PARTITION BY "department_id") AS "total_salary"

            # RANK with advanced ORDER BY
            builder.with_window_function(
                column="salary",
                function="RANK",
                alias="rank",
                partition_by=["department_id"],
                order_by=[{"column": "hire_date", "order": "ASC", "nulls": "LAST"}]
            )
            # SELECT ..., RANK("salary") OVER (PARTITION BY "department_id" ORDER BY "hire_date" ASC NULLS LAST) AS "rank"

        Notes:
            - If `partition_by` is provided, the PARTITION BY clause is included.
            - If `order_by` is provided, the ORDER BY clause can include null handling.
            - The alias is mandatory for readability and must be valid SQL syntax.
        """
        if not function:
            raise ValueError("Function is required for window functions.")
        if not alias:
            raise ValueError("Alias is required for window functions.")

        # Handle column argument
        if column is None:
            arg = ""   # e.g., ROW_NUMBER()
        elif column == "*":
            arg = "*"  # e.g., COUNT(*)
        else:
            arg = SQLUtils.quote_column(column, self.quote_style)

        # Build PARTITION BY
        part_clause = ""
        if partition_by:
            quoted_parts = [SQLUtils.quote_column(c, self.quote_style) for c in partition_by]
            part_clause = f"PARTITION BY {', '.join(quoted_parts)}"

        # Build ORDER BY
        order_clause = ""
        if order_by:
            order_clause = SQLUtils.build_order_by_clause(order_by, self.quote_style)
            order_clause = order_clause.replace("ORDER BY ", "ORDER BY ")

        # Build OVER clause
        over_parts = [p for p in [part_clause, order_clause] if p]
        over_clause = f"OVER ({' '.join(over_parts)})" if over_parts else "OVER ()"

        # Build final expression
        func_call = f"{function}({arg})" if arg else f"{function}()"
        expr = f"{func_call} {over_clause} AS {SQLUtils.quote_column(alias, self.quote_style)}"

        if expr not in self.columns:
            self.columns.append(expr)

        return self


    def get_parameters(self) -> List:
        """
        Retrieve all parameters for the query.

        Returns:
            List: A list of query parameters.
        """
        return self.parameters

    def build(self) -> str:
        """
    Build and return the complete SQL query as a string.

    This method constructs the final SQL query by combining all added clauses,
    including SELECT, FROM, JOIN, WHERE, GROUP BY, HAVING, ORDER BY, and LIMIT.
    It also includes any Common Table Expressions (CTEs) defined with
    `with_cte`.

    Returns:
        str: The generated SQL query as a string.

    Raises:
        ValueError: If no columns are specified in the SELECT clause.

    Examples:
        # Basic query with SELECT and WHERE
        builder.select(["id", "name"]).where("age > 30").build()
        # SELECT id, name
        # FROM "employees"
        # WHERE age > 30

        # Complex query with CTEs, JOINs, and aggregates
        subquery = SelectQueryBuilder(
            "orders").select(["user_id", "SUM(total)"]).group_by(["user_id"])
        builder.with_cte("order_totals", subquery)\
            .select(["user_id", "total"])\
            .join("users", "order_totals.user_id = users.id")\
            .where("users.active = 1")\
            .group_by(["user_id"])\
            .having("total > 5000")\
            .order_by([{"column": "total", "order": "DESC"}])\
            .limit(10)\
            .build()
        # WITH "order_totals" AS (
        #     SELECT user_id, SUM(total)
        #     FROM "orders"
        #     GROUP BY user_id
        # )
        # SELECT user_id, total
        # FROM "order_totals"
        # INNER JOIN "users" ON order_totals.user_id = users.id
        # WHERE users.active = 1
        # GROUP BY user_id
        # HAVING total > 5000
        # ORDER BY total DESC
        # LIMIT 10

    Notes:
        - CTEs, if present, are prepended to the query as a WITH clause.
        - SELECT columns are mandatory; the method raises a `ValueError` if no
        columns are specified.
        - Each clause is constructed dynamically based on the attributes of
        the query builder.
    """
        log_and_optionally_raise(
            module="SQL_BUILDER",
            component="SelectQueryBuilder",
            method="build",
            error_type=ErrorType.NO_ERROR,
            message="Building query...",
            level="INFO"
        )
        if not self.columns:
            raise ValueError("No columns specified in SELECT clause.")

        # Add WITH clause for CTEs
        cte_clause = ""
        if hasattr(self, "ctes") and self.ctes:
            cte_clause = f"WITH {', '.join(self.ctes)} "

        # Add DISTINCT ON or DISTINCT clause
        distinct_clause = ""
        if self.distinct_on_columns:
            distinct_clause = "DISTINCT ON "f"({', '.join(self.distinct_on_columns)}) "
        elif self.distinct:
            distinct_clause = "DISTINCT "

        # Build SELECT clause with all columns
        query = f"{cte_clause}SELECT {distinct_clause}{', '.join(self.columns)} FROM {self.table_name}"

        # Add JOIN clauses
        if self.joins:
            query += f" {' '.join(self.joins)}"

        # Add WHERE clause
        if self.conditions:
            query += f" WHERE {' AND '.join(self.conditions)}"

        # Add GROUP BY clause
        group_by_clause = SQLUtils.build_clause(
            "GROUP BY", self.group_by_columns)
        if group_by_clause:
            query += f" {group_by_clause}"

        # Add HAVING clause
        having_clause = SQLUtils.build_clause("HAVING", self.having_conditions)
        if having_clause:
            query += f" {having_clause}"

        # Add ORDER BY clause
        if self.order_by_clause:
            query += f" {self.order_by_clause}"

        # Add LIMIT clause
        if self.limit_value is not None:
            query += f" LIMIT {self.limit_value}"

        # Add set operations (UNION, INTERSECT, EXCEPT)
        for operation, subquery in self.set_operations:
            query += f" {operation} ({subquery.build()})"
        log_and_optionally_raise(
            module="SQL_BUILDER",
            component="SelectQueryBuilder",
            method="build",
            error_type=ErrorType.NO_ERROR,
            message=f"Generated query: {query}",
            level="INFO"
        )
        return query

    def pretty_print(self, return_string: bool = False) -> Optional[str]:
        """
    Format and display the generated SQL query for better readability.

    This method formats the SQL query using standard SQL formatting rules,
    including proper indentation and keyword casing. It can either print the
    formatted query directly or return it as a string.

    Args:
        return_string (bool, optional): If True, returns the formatted SQL
            query as a string instead of printing it. Defaults to False.

    Returns:
        Optional[str]: The formatted SQL query if `return_string` is True,
        otherwise None.

    Examples:
        # Pretty print a query
        builder.select(
            ["id", "name"]).where("status = 'active'").pretty_print()
        # Output:
        # SELECT id,
        #        name
        # FROM "employees"
        # WHERE status = 'active'

        # Return the formatted query as a string
        formatted_query = builder.select(
            ["id", "name"]).pretty_print(return_string=True)

    Notes:
        - This method uses the `sqlparse` library to format the query. Ensure
        the library is installed in your environment.
        - The `build` method is called internally to generate the query before
        formatting.
        - If the query is invalid or incomplete, this method will raise an
        exception from the `build` method.
    """
        query = self.build()
        formatted_query = sqlparse.format(
            query, reindent=True, keyword_case="upper")
        if return_string:
            return formatted_query
        print(formatted_query)
        return None
