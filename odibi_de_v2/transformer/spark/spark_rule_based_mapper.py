from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, lit,when
from typing import List, Dict, Optional, Callable, Union, Any

from odibi_de_v2.core import IDataTransformer
from odibi_de_v2.utils import (
    enforce_types, validate_non_empty, ensure_output_type,
    log_call, benchmark
)
from odibi_de_v2.logger import log_exceptions, log_and_optionally_raise
from odibi_de_v2.core.enums import ErrorType

import pyspark.sql.functions as F
import re


class SparkRuleBasedMapper(IDataTransformer):
    """
    Initializes a SparkRuleBasedMapper object to apply rule-based transformations on a Spark DataFrame.

    This class allows for dynamic and conditional transformations based on a set of user-defined rules.
    Each rule can specify conditions and corresponding values to apply to a specified column of a DataFrame.
    The conditions support a variety of operators and can evaluate to dynamic values using lambda functions.

    Args:
        column_name (str): The name of the column to which the rules will be applied.
        rules (List[Dict[str, object]]): A list of dictionaries where each dictionary represents a rule.
            Each rule should have at least a 'condition' key with a string value representing the condition,
            and a 'value' key that can be a static value, a column name, or a lambda function
            (either as a callable or a string).
        default_value (Optional[object], optional): A default value to apply if none of the rules match.
            Defaults to None.

    Raises:
        ValueError: If any lambda string in the rules cannot be safely evaluated or if any rule dictionary
        is missing required keys.

    Example:
        >>> mapper = SparkRuleBasedMapper(
                column_name="status",
                rules=[
                    {"condition": "age > 18", "value": "adult"},
                    {"condition": "age <= 18", "value": "minor"}
                ],
                default_value="unknown"
            )
        >>> transformed_df = mapper.transform(spark_df)

    This example creates a SparkRuleBasedMapper instance that applies different labels to the 'status' column
    based on the 'age' column of the input DataFrame. If no conditions are met, 'unknown' is assigned.
    """

    @enforce_types(strict=False)
    @validate_non_empty(["column_name", "rules"])
    @benchmark(module="TRANSFORMER", component="SparkRuleBasedMapper")
    @log_call(module="TRANSFORMER", component="SparkRuleBasedMapper")
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkRuleBasedMapper",
        error_type=ErrorType.INIT_ERROR,
        raise_type=ValueError,
    )
    def __init__(
        self,
        column_name: str,
        rules: List[Dict[str, object]],
        default_value: Optional[object] = None,
    ):
        """
        Initializes a new instance of the SparkRuleBasedMapper class.

        This constructor initializes the SparkRuleBasedMapper with a specific column name, a set of
        transformation rules, and an optional default value for the column. It also logs the initialization details.

        Args:
            column_name (str): The name of the column to which the rules will be applied.
            rules (List[Dict[str, object]]): A list of dictionaries where each dictionary represents a rule.
                These rules are used to transform the values of the column. The method `_prepare_rules` is
                used to process these rules, which includes handling string representations of lambda functions.
            default_value (Optional[object], optional): The default value to be used for the column if no rule applies.
                Defaults to None.

        Raises:
            ErrorType.NO_ERROR: Indicates that no error occurred during initialization. This is logged rather than
            raised as an exception.

        Example:
            >>> mapper = SparkRuleBasedMapper(
                    column_name="age",
                    rules=[{"rule": lambda x: x > 18, "value": "adult"}, {"rule": lambda x: x <= 18, "value": "minor"}],
                    default_value="unknown"
                )
        """
        self.column_name = column_name
        self.rules = self._prepare_rules(rules)  # <- now handles string lambdas
        self.default_value = default_value

        log_and_optionally_raise(
            module="TRANSFORMER",
            component="SparkRuleBasedMapper",
            method="__init__",
            error_type=ErrorType.NO_ERROR,
            message=f"Initialized with column_name={column_name}, rules={rules}, default_value={default_value}",
            level="INFO",
        )
    def _safe_eval_lambda(self,expression: str) -> Callable:
        """
        Evaluates a lambda expression string and converts it into a callable function, ensuring that only
        specific, approved Spark functions are accessible during evaluation.

        Args:
            expression (str): A string representing the lambda expression to be evaluated. This should typically
            involve operations or functions related to Spark data manipulation.

        Returns:
            Callable: A callable object derived from the evaluated lambda expression.

        Raises:
            SyntaxError: If the lambda expression contains syntax errors.
            NameError: If the lambda expression tries to access unapproved attributes or functions.

        Example:
            # Example usage within a class that has _safe_eval_lambda method
            expression = "col('age') + 1"
            func = self._safe_eval_lambda(expression)
            # `func` can now be used as a function within Spark transformations

        Note:
            This method is intended for internal use where expressions are controlled or sanitized. It should not
            be exposed directly to user input to avoid security risks.
        """
        allowed_builtins = {
            "col": col,
            "F": F,
            "lit": lit,
        }
        return eval(expression.strip(), {"__builtins__": {}}, allowed_builtins)

    def _prepare_rules(self, rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Prepares a list of rules by converting string representations of lambda functions into actual
        callable lambda functions.

        This method processes each rule in the input list. If a rule's 'value' field is a string that
        starts with 'lambda', it attempts to convert this string into a lambda function using a safe evaluation method.
        If the conversion fails, it raises a ValueError.

        Args:
            rules (List[Dict[str, Any]]): A list of dictionaries representing rules. Each dictionary may contain a
            'value' key with a lambda function as a string.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries with the same structure as the input, where string lambdas
            have been converted to callable lambda functions.

        Raises:
            ValueError: If the lambda string cannot be safely evaluated to a lambda function.

        Example:
            >>> rules = [{'value': "lambda x: x + 1"}, {'value': 5}]
            >>> _prepare_rules(rules)
            [{'value': <function <lambda> at 0x7f2d5c1e8d30>}, {'value': 5}]
        """
        prepared = []
        for rule in rules:
            new_rule = rule.copy()
            value = new_rule.get("value")
            if isinstance(value, str) and value.strip().startswith("lambda"):
                try:
                    new_rule["value"] = self._safe_eval_lambda(value)
                except Exception as e:
                    raise ValueError(f"Failed to evaluate lambda in rule: {value}") from e
            prepared.append(new_rule)
        return prepared

    @enforce_types(strict=True)
    @ensure_output_type(DataFrame)
    @log_exceptions(
        module="TRANSFORMER",
        component="SparkRuleBasedMapper",
        error_type=ErrorType.TRANSFORM_ERROR,
        raise_type=RuntimeError,
    )
    def transform(self, data: DataFrame, **kwargs) -> DataFrame:
        """
        Transforms a DataFrame based on specified rules and updates a column with the transformation results.

        This method applies a series of transformation rules to the input DataFrame. Each rule specifies a
        condition and a corresponding value to apply when the condition is met. The method updates a specified
        column in the DataFrame with the results of these transformations. If no rules match, a default value
        can be applied.

        Args:
            data (DataFrame): The input DataFrame to be transformed.
            **kwargs: Additional keyword arguments that are not directly used but might be passed due to the
            method's design.

        Returns:
            DataFrame: A DataFrame with the specified column updated based on the transformation rules.

        Raises:
            ValueError: If any rule in the `rules` list is missing a 'condition' or 'value' key.

        Example:
            Assuming `self.rules` contains rules like [
                {'condition': 'col("age") > 18',
                'value': 'adult'}, {'condition': 'col("age") <= 18', 'value': 'minor'}],
                and `self.column_name` is 'status', the method can be used as follows:

            >>> df = spark.createDataFrame([(21,), (17,)], ['age'])
            >>> transformer = RuleBasedTransformer(rules, column_name='status', default_value='unknown')
            >>> transformed_df = transformer.transform(df)
            >>> transformed_df.show()
            +---+------+
            |age|status|
            +---+------+
            | 21| adult|
            | 17| minor|
            +---+------+
        """
        expr = None

        for rule in self.rules:
            condition = rule.get("condition")
            value = rule.get("value")
            use_column = rule.get("use_column", False)

            if condition is None or value is None:
                raise ValueError(f"Invalid rule: {rule}")

            parsed_condition = self._parse_condition(condition)

            if callable(value):
                target_value = value(data)
            elif use_column:
                target_value = col(value)
            else:
                target_value = lit(value)

            if expr is None:
                expr = when(parsed_condition, target_value)
            else:
                expr = expr.when(parsed_condition, target_value)

        if self.default_value is not None:
            expr = expr.otherwise(lit(self.default_value))
        else:
            expr = expr.otherwise(lit(None))

        return data.withColumn(self.column_name, expr)

    def _parse_condition(self, condition: str) -> Column:
        """
        Parses a logical condition string into a combined Column object representing the condition.

        This method interprets a string that describes a logical condition, where parts of the condition
        can be combined using 'and' or 'or'. The method supports complex conditions by recursively parsing
        each part of the condition and combining them using logical AND (&) or OR (|) operations.

        Args:
            condition (str): A string representing the logical condition to parse. The condition can include
                            logical operators 'and' and 'or' to combine different conditions.

        Returns:
            Column: A Column object that represents the parsed condition. This object can be used in database
                    operations to filter data according to the specified logical condition.

        Raises:
            ValueError: If the condition string is malformed or if an unsupported logical operator is used.

        Example:
            Assuming a setup where `_parse_single_condition` can parse individual conditions like "age > 30":
            >>> condition_str = "age > 30 and salary < 50000"
            >>> result_column = self._parse_condition(condition_str)
            This would parse the string into a Column object that represents the condition (age > 30)
            AND (salary < 50000).
        """
        condition = condition.strip()

        if " and " in condition.lower():
            parts = re.split(r"\s+and\s+", condition, flags=re.IGNORECASE)
            parsed_parts = [self._parse_single_condition(p.strip()) for p in parts]
            combined = parsed_parts[0]
            for part in parsed_parts[1:]:
                combined = combined & part
            return combined

        if " or " in condition.lower():
            parts = re.split(r"\s+or\s+", condition, flags=re.IGNORECASE)
            parsed_parts = [self._parse_single_condition(p.strip()) for p in parts]
            combined = parsed_parts[0]
            for part in parsed_parts[1:]:
                combined = combined | part
            return combined

        return self._parse_single_condition(condition)

    def _parse_single_condition(self, condition: str) -> Column:
        """
        Parses a single string condition into a PySpark Column object representing the condition.

        This method interprets a string that defines a condition on a DataFrame column and converts
        it into a PySpark Column object that can be used for DataFrame filtering operations. Supported conditions
        include checks for nullity, pattern matching with 'like', membership checks with 'in' and 'not in', and
        comparisons using operators like '==', '!=', '>', '<', '>=', '<='.

        Args:
            condition (str): A string representing a condition on a DataFrame column. The condition should be in a
            format understandable by SQL-like query operations.

        Returns:
            Column: A PySpark Column object that represents the condition expressed in the input string.

        Raises:
            ValueError: If the condition string is unsupported or malformed.

        Examples:
            - `_parse_single_condition("age is null")` would return a condition checking if the 'age' column is null.
            - `_parse_single_condition("salary > 3000")` would return a condition that checks if the 'salary' column
                values are greater than 3000.
            - `_parse_single_condition("name like '%John%'")` would return a condition that checks if the 'name'
                column contains the substring 'John'.
        """
        condition = condition.strip()

        if re.search(r"\bis\s+null\b", condition, re.IGNORECASE):
            field = condition.split()[0]
            return col(field).isNull()

        if re.search(r"\bis\s+not\s+null\b", condition, re.IGNORECASE):
            field = condition.split()[0]
            return col(field).isNotNull()

        if " like " in condition.lower():
            field, pattern = map(str.strip, condition.split("like"))
            return col(field).like(self._strip_quotes(pattern))

        if " not in " in condition.lower():
            field, rest = condition.lower().split("not in", 1)
            values = self._parse_list(rest)
            return ~col(field.strip()).isin(values)

        if " in " in condition.lower():
            field, rest = condition.lower().split("in", 1)
            values = self._parse_list(rest)
            return col(field.strip()).isin(values)

        simple_ops = ["==", "!=", ">=", "<=", ">", "<"]
        for op in simple_ops:
            if op in condition:
                left, right = map(str.strip, condition.split(op))
                right = self._try_cast(right)
                if op == "==":
                    return col(left) == right
                elif op == "!=":
                    return col(left) != right
                elif op == ">":
                    return col(left) > right
                elif op == ">=":
                    return col(left) >= right
                elif op == "<":
                    return col(left) < right
                elif op == "<=":
                    return col(left) <= right

        raise ValueError(f"Unsupported or invalid condition: '{condition}'")

    def _try_cast(self, value: str) -> Any:
        """
        Attempts to convert a string value to an int, float, or None, returning the original string if conversion fails.

        This method first strips any surrounding quotes from the input string using `_strip_quotes`. It then checks
        if the processed string is a case-insensitive match for "null", in which case it returns `None`. If the string
        is not "null", the method attempts to convert it to an `int`. If this conversion fails, it tries to convert the
        string to a `float`. If all conversions fail, the original string is returned.

        Args:
            value (str): The string value to attempt to cast.

        Returns:
            Any: The converted value as an `int`, `float`, or `None`, or the original `str` if no conversions
            are possible.

        Example:
            >>> _try_cast('"123"')
            123
            >>> _try_cast('null')
            None
            >>> _try_cast('123.456')
            123.456
            >>> _try_cast('hello')
            'hello'
        """
        value = self._strip_quotes(value)
        if value.lower() == "null":
            return None
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value

    def _strip_quotes(self, text: str) -> str:
        """
        Removes leading and trailing whitespace and quotes from the input string.

        This method first trims any leading or trailing whitespace from the input string using `str.strip()`.
        It then removes any leading or trailing single (`'`) or double (`"`) quotes.

        Args:
            text (str): The string from which whitespace and quotes are to be removed.

        Returns:
            str: The modified string with leading/trailing whitespace and quotes removed.

        Example:
            >>> _strip_quotes("  'Hello World!'  ")
            'Hello World!'
        """
        return text.strip().strip("'").strip('"')

    def _parse_list(self, text: str) -> List[Any]:
        """
        Parses a string representation of a list, attempting to cast each item to an appropriate type.

        This method processes a string that starts and ends with parentheses, strips them, and splits
        the string by commas. Each item is then stripped of leading and trailing whitespace and cast to
        a more specific type using the `_try_cast` method.

        Args:
            text (str): The string representation of the list to parse. It should start with '(' and end with ')'.

        Returns:
            List[Any]: A list of elements with types inferred and cast by `_try_cast`.

        Example:
            Given the input '(1, 2, "three", 4.0)', the method might return [1, 2, "three", 4.0] assuming `_try_cast`
            correctly identifies types.

        Note:
            This method is intended for internal use and assumes well-formed input. Malformed input may lead to
            unexpected results or errors.
        """
        text = text.strip().lstrip("(").rstrip(")")
        items = [self._try_cast(item.strip()) for item in text.split(",")]
        return items
