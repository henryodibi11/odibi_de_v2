from typing import List, Union


class SQLUtils:
    @staticmethod
    def quote_column(column: str, quote_style: str = '"', always_quote: bool = True) -> str:
        """
        Quote a SQL identifier safely.
    
        Args:
            column (str): The column or identifier to quote.
            quote_style (str, optional): The quote style to use. Defaults to '"'.
                Supported: '"', '[', '`'.
            always_quote (bool, optional): If True, always quote identifiers.
                If False, quote only when necessary (reserved words, spaces, etc.).
    
        Returns:
            str: Quoted identifier or expression.
    
        Notes:
            - '*' is never quoted.
            - SQL expressions like COUNT(col) are returned unchanged.
            - Reserved keywords are always quoted regardless of `always_quote`.
        """
        RESERVED_KEYWORDS = {
            "select", "from", "where", "join", "group", "by", "order", "limit",
            "distinct", "on", "union", "intersect", "except", "insert",
            "update", "delete"
        }
    
        if column == "*":
            return column
    
        # Already quoted
        if (quote_style == '"' and column.startswith('"')) or \
           (quote_style == '[' and column.startswith('[')) or \
           (quote_style == '`' and column.startswith('`')):
            return column
    
        # SQL expression (e.g., COUNT(...))
        if "(" in column and ")" in column:
            return column
    
        # Always quote mode
        if always_quote:
            if quote_style == '"':
                return f'"{column}"'
            elif quote_style == "[":
                return f'[{column}]'
            elif quote_style == "`":
                return f'`{column}`'
    
        # Selective quoting mode
        if (" " in column or
            column.lower() in RESERVED_KEYWORDS or
            not column.isidentifier()):
            if quote_style == '"':
                return f'"{column}"'
            elif quote_style == "[":
                return f'[{column}]'
            elif quote_style == "`":
                return f'`{column}`'
    
        return column

    @staticmethod
    def parse_conditions(
        conditions: Union[str, List[Union[str, tuple]]],
        quote_style: str = '"',
        auto_quote: bool = False
    ) -> str:
        """
        Parse and format conditions for WHERE or JOIN clauses.
    
        This method converts conditions provided as strings, lists, or tuples
        into valid SQL expressions. It supports nested logical operations such as
        `AND` and `OR`, and can optionally auto-quote identifiers for consistency.
    
        Args:
            conditions (Union[str, List[Union[str, tuple]]]): The conditions to parse.
                Supported formats:
                - `str`: A simple SQL condition (e.g., "age > 30").
                - `List[str]`: A list of AND-ed conditions
                    (e.g., ["age > 30", "status = 'active'"]).
                - `List[tuple]`: A nested structure for logical conditions
                    (e.g., [("age > 30", "AND", "status = 'active'")]).
                - `tuple`: A single logical condition in the form
                    `(left_condition, operator, right_condition)`.
    
            quote_style (str, optional): Quoting style for identifiers. Defaults to '"'.
                Supported: '"', '[', '`'.
    
            auto_quote (bool, optional): If True, automatically quote identifiers
                inside conditions (e.g., "users.id" → "\"users\".\"id\"").
                Defaults to False (raw passthrough).
    
        Returns:
            str: The parsed SQL condition as a string.
    
        Raises:
            ValueError: If the conditions are not in a valid format.
    
        Examples:
            # Simple condition
            SQLUtils.parse_conditions("age > 30")
            # Output: "age > 30"
    
            # List of AND-ed conditions
            SQLUtils.parse_conditions(["age > 30", "status = 'active'"])
            # Output: "(age > 30 AND status = 'active')"
    
            # Nested logical conditions
            SQLUtils.parse_conditions([("age > 30", "AND", "status = 'active'")])
            # Output: "(age > 30 AND status = 'active')"
    
            # Complex nested conditions
            SQLUtils.parse_conditions([
                ("age > 30", "AND", "status = 'active'"),
                "salary > 50000"
            ])
            # Output: "((age > 30 AND status = 'active') AND salary > 50000)"
    
            # Auto-quoted identifiers
            SQLUtils.parse_conditions("users.id = o.user_id", auto_quote=True)
            # Output: "\"users\".\"id\" = \"o\".\"user_id\""
    
        Notes:
            - Logical operators like `AND` and `OR` are case-sensitive and must
              be provided in uppercase.
            - Auto-quoting is helpful for enforcing consistent quoting in
              generated SQL, but for complex expressions (e.g., UPPER(users.name))
              you may prefer raw conditions with auto_quote=False.
            - This method is used internally by `SelectQueryBuilder` for WHERE
              and JOIN clauses.
        """

        def is_number_literal(token: str) -> bool:
            """Return True if token is an integer or decimal number (e.g., 42, -1, 3.14)."""
            if token.startswith("-"):
                token = token[1:]
            return token.replace(".", "", 1).isdigit()
    
        def _quote_identifier(token: str) -> str:
            if "." in token and not token.strip().startswith(("'", '"')):
                parts = token.split(".")
                return ".".join(SQLUtils.quote_column(p, quote_style) for p in parts)
            return SQLUtils.quote_column(token, quote_style)
    
        def _process(expr: str) -> str:
            if not auto_quote:
                return expr
            tokens = expr.split()
            processed = []
            for tok in tokens:
                if tok.upper() in {"AND", "OR", "=", "<", ">", "<=", ">=", "<>", "!="}:
                    processed.append(tok)
                elif is_number_literal(tok):
                    processed.append(tok)  # leave numbers unquoted
                elif tok.startswith("'") and tok.endswith("'"):
                    processed.append(tok)  # string literal
                else:
                    processed.append(_quote_identifier(tok))
            return " ".join(processed)
    
        if isinstance(conditions, str):
            return _process(conditions)
    
        if isinstance(conditions, list):
            if all(isinstance(c, str) for c in conditions):
                return "(" + " AND ".join(_process(c) for c in conditions) + ")"
            elif all(isinstance(c, tuple) and len(c) == 3 for c in conditions):
                return "(" + " ".join(
                    _process(part) if i % 2 == 0 else part
                    for i, part in enumerate(conditions[0])
                ) + ")"
    
        raise ValueError("Invalid condition format")

    @staticmethod
    def format_columns(
        columns: List[Union[str, dict]],
        quote_style: str = '"'
    ) -> List[str]:
        """
    Format column names for SELECT or GROUP BY clauses, handling aliasing and
    quoting.

    This method processes a list of column specifications, supporting simple
    column names, aliasing, and quoting styles. It returns a list of formatted
    column names ready for inclusion in a SQL query.

    Args:
        columns (List[Union[str, dict]]): A list of columns to format.
            Supported formats:
            - `str`: A simple column name (e.g., "id").
            - `dict`: A dictionary specifying:
                - `column` (str): The column name.
                - `alias` (str, optional): An alias for the column.
        quote_style (str, optional): The quoting style for column names.
            Supported options:
            - `"`: Standard SQL double quotes (default).
            - `[`: Square brackets for databases like SQL Server.

    Returns:
        List[str]: A list of formatted column names.

    Raises:
        ValueError: If a column specification is invalid or unsupported.

    Examples:
        # Format simple column names
        formatted = SQLUtils.format_columns(["id", "name"])
        # Output: ['id', 'name']

        # Format columns with aliases using dictionaries
        formatted = SQLUtils.format_columns([
            {"column": "id", "alias": "employee_id"},
            {"column": "name", "alias": "full_name"}
        ])
        # Output: ['id AS employee_id', 'name AS full_name']

        # Mixing simple columns and aliasing
        formatted = SQLUtils.format_columns(
            ["id", {"column": "name", "alias": "full_name"}])
        # Output: ['id', 'name AS full_name']

        # Apply quoting
        formatted = SQLUtils.format_columns(
            ["user name", "select"], quote_style='"')
        # Output: ['"user name"', '"select"']

    Notes:
        - This method is typically used internally by the `SelectQueryBuilder`
        for SELECT and GROUP BY clause construction.
        - Ensure column names and aliases conform to SQL standards before
        passing them.
        - Invalid or unsupported column formats will raise a `ValueError`.
    """
        formatted = []
        for col in columns:
            if isinstance(col, dict):  # Aliased column
                column = SQLUtils.quote_column(
                    col["column"], quote_style)
                alias = SQLUtils.quote_column(
                    col.get("alias", ""), quote_style)
                formatted.append(f"{column} AS {alias}" if alias else column)
            elif isinstance(col, str):  # Simple column
                formatted.append(SQLUtils.quote_column(col, quote_style))
            else:
                raise ValueError(
                    f"Unsupported column type: {type(col)}. Use str or dict.")
        return formatted

    @staticmethod
    def build_clause(keyword: str, elements: List[str]) -> str:
        """
    Build a SQL clause with a keyword (e.g., GROUP BY, ORDER BY).

    This method constructs a SQL clause by combining a keyword with a list of
    elements, separated by commas. It ensures that the clause is properly
    formatted and only included if the `elements` list is not empty.

    Args:
        keyword (str): The SQL keyword for the clause
            (e.g., "GROUP BY", "ORDER BY").
        elements (List[str]): A list of elements to include in the clause.

    Returns:
        str: The formatted SQL clause or an empty string if `elements` is
        empty.

    Examples:
        # Build a GROUP BY clause
        clause = SQLUtils.build_clause(
            "GROUP BY", ["department_id", "job_title"])
        # Output: "GROUP BY department_id, job_title"

        # Build an empty clause
        clause = SQLUtils.build_clause("GROUP BY", [])
        # Output: ""

    Notes:
        - This method is typically used internally by the `SelectQueryBuilder`
        to construct optional SQL clauses.
        - Ensure that the `elements` list contains valid SQL identifiers or
        expressions before passing them to this method.
    """
        if elements:
            return f"{keyword} {', '.join(elements)}"
        return ""

    @staticmethod
    def validate_columns(
        selected_columns: List[str],
        available_columns: List[str]
    ):
        """
    Ensure selected columns exist in the available schema.

    This method validates that all columns specified in the `selected_columns`
    list are present in the `available_columns` list. It raises a `ValueError`
    if any column is invalid or missing from the available schema.

    Args:
        selected_columns (List[str]): The list of columns specified in the
            query.
        available_columns (List[str]): The list of columns available in the
            schema.

    Raises:
        ValueError: If any column in `selected_columns` is not present in
        `available_columns`.

    Examples:
        # Valid columns
        SQLUtils.validate_columns(
            ["id", "name"], ["id", "name", "age", "salary"])
        # Passes validation

        # Invalid columns
        SQLUtils.validate_columns(
            ["id", "invalid_column"], ["id", "name", "age", "salary"])
        # Raises ValueError: "Invalid columns selected: ['invalid_column']"

    Notes:
        - This method is used internally by the `SelectQueryBuilder` to
            validate column references in SELECT and GROUP BY clauses.
        - Ensure the schema (`available_columns`) is defined correctly before
        using this method.
        - This validation prevents runtime SQL errors caused by referencing
        non-existent columns.
    """
        invalid_columns = [
            col for col in selected_columns if col not in available_columns]
        if invalid_columns:
            raise ValueError(f"Invalid columns selected: {invalid_columns}")

    @staticmethod
    def validate_limit(limit: int):
        """
    Ensure the LIMIT value is a valid non-negative integer.

    This method validates the `LIMIT` clause to ensure that the specified value
    is a non-negative integer. If the value is invalid, it raises a
    `ValueError`.

    Args:
        limit (int): The LIMIT value to validate.

    Raises:
        ValueError: If the `limit` is not a non-negative integer.

    Examples:
        # Valid LIMIT value
        SQLUtils.validate_limit(10)  # Passes validation

        # Invalid LIMIT value (negative)
        SQLUtils.validate_limit(-1)  # Raises ValueError

        # Invalid LIMIT value (not an integer)
        SQLUtils.validate_limit("10")  # Raises ValueError

    Notes:
        - This method is typically used internally by the `SelectQueryBuilder`
        to validate LIMIT clauses before adding them to the query.
        - Ensure that the value passed is explicitly an integer to avoid
        errors.
    """
        if not isinstance(limit, int) or limit < 0:
            raise ValueError(
                f"Invalid LIMIT value: {limit}. "
                "Must be a non-negative integer.")

    @staticmethod
    def validate_table_name(table_name: str):
        """
    Validate the table name to ensure it is a non-empty string.

    This method checks that the `table_name` provided is a valid, non-empty
    string. It raises a `ValueError` if the table name is invalid, ensuring
    that queries reference proper table names.

    Args:
        table_name (str): The name of the table or subquery string to validate.

    Raises:
        ValueError: If the `table_name` is empty or not a string.

    Examples:
        # Valid table name
        SQLUtils.validate_table_name("employees")  # Passes validation

        # Invalid table name (empty string)
        SQLUtils.validate_table_name("")  # Raises ValueError

        # Invalid table name (None)
        SQLUtils.validate_table_name(None)  # Raises ValueError

    Notes:
        - This method is used internally by the `SelectQueryBuilder` to
        validate the `table_name` during initialization.
        - The table name should be a valid SQL identifier or a subquery wrapped
            in parentheses.
    """
        if not table_name or not isinstance(table_name, str):
            raise ValueError(
                f"Invalid table name: '{table_name}'. "
                "It must be a non-empty string.")

    @staticmethod
    def validate_column_names(columns: List[str], schema: List[str]):
        """
    Validate that all column names exist in the provided schema.

    This method ensures that each column in the `columns` list is present in
    the provided schema. It raises a `ValueError` if any column is invalid or
    missing from the schema.

    Args:
        columns (List[str]): A list of columns to validate.
        schema (List[str]): A list of valid column names (schema).

    Raises:
        ValueError: If any column in `columns` is not present in the `schema`.

    Examples:
        # Valid column names
        SQLUtils.validate_column_names(
            ["id", "name"], ["id", "name", "age", "salary"])

        # Invalid column name
        SQLUtils.validate_column_names(
            ["id", "invalid_column"], ["id", "name", "age", "salary"])

    Notes:
        - This method is typically used internally by the `SelectQueryBuilder`
        to validate columns in SELECT, WHERE, GROUP BY, and other clauses.
        - Ensure the schema is correctly defined before passing it to this
        method.
    """
        invalid_columns = [col for col in columns if col not in schema]
        if invalid_columns:
            raise ValueError(
                f"Invalid column(s): {invalid_columns}. "
                f"Must be in schema: {schema}")

    @staticmethod
    def validate_conditions(conditions: List[str]):
        """
    Ensure conditions in a WHERE or HAVING clause are valid.

    This method validates that each condition is a non-empty string and raises
    a `ValueError` if any condition is invalid. It is used to prevent syntax
    errors in SQL queries caused by empty or malformed conditions.

    Args:
        conditions (List[str]): A list of conditions to validate.

    Raises:
        ValueError: If any condition is empty or not a valid string.

    Examples:
        # Valid conditions
        SQLUtils.validate_conditions(["age > 30", "salary > 50000"])

        # Invalid condition (empty string)
        SQLUtils.validate_conditions(["age > 30", ""])  # Raises ValueError

        # Invalid condition (not a string)
        SQLUtils.validate_conditions(["age > 30", None])  # Raises ValueError

    Notes:
        - This method is typically used internally by the `SelectQueryBuilder`
        to validate WHERE and HAVING clauses before they are added to the
        query.
        - Each condition should already be a valid SQL expression before being
        passed.
    """
        if not conditions:
            raise ValueError("Conditions cannot be empty.")
        for condition in conditions:
            if not isinstance(condition, str) or not condition.strip():
                raise ValueError(f"Invalid condition: {condition}")

    @staticmethod
    def validate_group_by(
        selected_columns: List[str],
        group_by_columns: List[str],
        raw_expressions: List[str]
    ):
        """
    Ensure that GROUP BY columns are part of the SELECT clause or valid raw
    expressions.

    This method validates that all columns specified in the GROUP BY clause are
    included in the SELECT clause or are valid raw expressions (e.g., aggregate
    functions). It raises a `ValueError` if any GROUP BY column is invalid.

    Args:
        selected_columns (List[str]): Columns specified in the SELECT clause.
        group_by_columns (List[str]): Columns specified in the GROUP BY clause.
        raw_expressions (List[str]): Raw expressions used in the SELECT clause.

    Raises:
        ValueError: If a column in `group_by_columns` is not present in
        `selected_columns` or `raw_expressions`.

    Examples:
        # Valid GROUP BY columns
        SQLUtils.validate_group_by(
            selected_columns=["department_id", "SUM(salary)"],
            group_by_columns=["department_id"],
            raw_expressions=["SUM(salary)"]
        )  # Passes validation

        # Invalid GROUP BY column
        SQLUtils.validate_group_by(
            selected_columns=["department_id", "SUM(salary)"],
            group_by_columns=["job_title"],
            raw_expressions=["SUM(salary)"]
        )  # Raises ValueError

    Notes:
        - This method is used internally by the `SelectQueryBuilder` to ensure
        that GROUP BY clauses are valid and aligned with the SELECT clause.
        - Ensure that raw expressions are formatted consistently before
        validation.
    """

        aliases = [
            col.split(" AS ")[-1].strip()
            for col in selected_columns if " AS " in col
        ]

        missing_columns = [
            col for col in group_by_columns
            if col not in selected_columns and col not in raw_expressions and col not in aliases
        ]
        if missing_columns:
            raise ValueError(
                f"Invalid GROUP BY column(s) {missing_columns}. "
                "Must exist in SELECT clause, valid raw expressions, or aliases."
            )

    @staticmethod
    def validate_having(
        having_conditions: List[str],
        group_by_columns: List[str],
        selected_aggregates: List[str]
    ):
        """
    Ensure that HAVING conditions reference valid columns or aggregates.

    This method validates that all columns or expressions used in the HAVING
    clause are either included in the GROUP BY clause or are valid aggregate
    expressions. It raises a `ValueError` if any condition references invalid
    columns or expressions.

    Args:
        having_conditions (List[str]): List of conditions in the HAVING clause.
        group_by_columns (List[str]): List of columns specified in the GROUP
        BY clause.

    Raises:
        ValueError: If any HAVING condition references columns or expressions
        not included in the GROUP BY clause.

    Examples:
        # Valid HAVING conditions
        SQLUtils.validate_having(
            having_conditions=["SUM(salary) > 50000"],
            group_by_columns=["department_id"]
        )  # Passes validation

        # Invalid HAVING condition
        SQLUtils.validate_having(
            having_conditions=["job_title = 'Manager'"],
            group_by_columns=["department_id"]
        )  # Raises ValueError

    Notes:
        - This method is typically used internally by the `SelectQueryBuilder`
            to ensure HAVING clauses are valid and aligned with the GROUP BY
            clause.
        - Ensure aggregate functions and column names are correctly formatted
        before validation.
    """
        valid_references = set(group_by_columns + selected_aggregates)

        for condition in having_conditions:
            if not any(ref in condition for ref in valid_references):
                raise ValueError(
                    f"Invalid HAVING condition: '{condition}'. Must reference one of: {valid_references}."
                )

    @staticmethod
    def build_order_by_clause(
        columns: List[Union[str, dict]],
        quote_style: str = '"'
    ) -> str:
        """
    Build and format the ORDER BY clause for the query.

    This method constructs the ORDER BY clause by processing a list of column
    names or dictionaries that specify sorting order and additional attributes
    (e.g., null handling).

    Args:
        columns (List[Union[str, dict]]): A list of columns or dictionaries
        specifying the order.
        Supported formats:
            - `str`: A column name (default order: ASC).
            - `dict`: A dictionary specifying:
                - `column` (str): The column name.
                - `order` (str, optional): Sorting order ("ASC" or "DESC").
                    Defaults to "ASC".
                - `nulls` (str, optional): Null handling ("FIRST" or "LAST").
                    Defaults to None.
        quote_style (str, optional): The quoting style for column names
            (e.g., `"`, `[`). Defaults to `"`.

    Returns:
        str: The formatted ORDER BY clause or an empty string if `columns` is
        empty.

    Raises:
        ValueError: If any column in the `columns` list has an invalid format.

    Examples:
        # Simple ORDER BY
        clause = SQLUtils.build_order_by_clause(["name", "age"])
        # Output: "ORDER BY name ASC, age ASC"

        # ORDER BY with custom sorting
        clause = SQLUtils.build_order_by_clause([
            {"column": "name", "order": "DESC"},
            {"column": "age", "order": "ASC"}
        ])
        # Output: "ORDER BY name DESC, age ASC"

        # ORDER BY with null handling
        clause = SQLUtils.build_order_by_clause([
            {"column": "hire_date", "order": "ASC", "nulls": "LAST"}
        ])
        # Output: "ORDER BY hire_date ASC NULLS LAST"

    Notes:
        - If `columns` is empty, the method returns an empty string.
        - Ensure that all column names and attributes conform to SQL standards.
        - This method is typically used internally by the `SelectQueryBuilder`
        to construct the ORDER BY clause.
    """
        if not columns:
            return ""

        formatted = []
        for col in columns:
            if isinstance(col, dict):
                column = SQLUtils.quote_column(col["column"], quote_style)
                order = col.get("order", "ASC").upper()
                if order not in {"ASC", "DESC"}:
                    raise ValueError(
                        f"Invalid order '{order}'. Use 'ASC' or 'DESC'.")
                formatted.append(f"{column} {order}")
            elif isinstance(col, str):
                formatted.append(
                    f"{SQLUtils.quote_column(col, quote_style)} ASC")
            else:
                raise ValueError(
                    f"Unsupported column type: {type(col)}. Use str or dict.")

        return f"ORDER BY {', '.join(formatted)}"
