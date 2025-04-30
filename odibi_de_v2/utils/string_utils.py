"""
string_utils.py

Reusable string utilities for text normalization, column name formatting,
and general-purpose transformations for data engineering workflows.
"""

import re
from typing import Optional, List


def normalize_string(value: Optional[str], case: str = "lower") -> str:
    """
    Strips leading/trailing whitespace and converts to a specified case.

    Args:
        value (Optional[str]): The string to normalize.
        case (str): Desired case format: "lower", "upper", or "title".

    Returns:
        str: Normalized string.

    Raises:
        ValueError: If an unsupported case is provided.

    Example:
        >>> normalize_string("  Hello ", case="upper")
        'HELLO'
    """
    if not value:
        return ""
    value = value.strip()
    match case.lower():
        case "lower":
            return value.lower()
        case "upper":
            return value.upper()
        case "title":
            return value.title()
        case _:
            raise ValueError(f"Unsupported case option: {case}")


def clean_column_name(name: str) -> str:
    """
    Cleans a column name by replacing non-alphanumeric characters with
    underscores and converting to lowercase snake_case.

    Args:
        name (str): The column name to clean.

    Returns:
        str: Cleaned column name.

    Example:
        >>> clean_column_name("Total Sales ($)")
        'total_sales'
    """
    name = re.sub(r"[^\w]+", "_", name)
    return name.strip("_").lower()


def standardize_column_names(columns: List[str]) -> List[str]:
    """
    Applies `clean_column_name` to a list of column names.

    Args:
        columns (List[str]): List of column names.

    Returns:
        List[str]: List of cleaned column names.

    Example:
        >>> standardize_column_names(["First Name", "Age (Years)"])
        ['first_name', 'age_years']
    """
    return [clean_column_name(col) for col in columns]


def to_snake_case(text: str) -> str:
    """
    Converts CamelCase or PascalCase to snake_case.

    Args:
        text (str): Input string.

    Returns:
        str: snake_case version of string.

    Example:
        >>> to_snake_case("CamelCaseString")
        'camel_case_string'
    """
    text = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", text)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    return text.lower()


def to_kebab_case(text: str) -> str:
    """
    Converts text to kebab-case (lowercase words separated by hyphens).

    Args:
        text (str): Input string.

    Returns:
        str: kebab-case version.

    Example:
        >>> to_kebab_case("My Variable Name")
        'my-variable-name'
    """
    return re.sub(r"[^\w]+", "-", text.strip()).lower()


def to_camel_case(text: str) -> str:
    """
    Converts a string to camelCase.

    Args:
        text (str): Input string.

    Returns:
        str: camelCase version.

    Example:
        >>> to_camel_case("my variable name")
        'myVariableName'
    """
    words = re.split(r"[\W_]+", text.strip())
    return words[0].lower() + "".join(w.title() for w in words[1:])


def to_pascal_case(text: str) -> str:
    """
    Converts a string to PascalCase.

    Args:
        text (str): Input string.

    Returns:
        str: PascalCase version.

    Example:
        >>> to_pascal_case("my variable name")
        'MyVariableName'
    """
    words = re.split(r"[\W_]+", text.strip())
    return "".join(w.title() for w in words)


def remove_extra_whitespace(text: str) -> str:
    """
    Removes extra internal whitespace from a string.

    Args:
        text (str): Input string.

    Returns:
        str: String with multiple spaces reduced to single spaces.

    Example:
        >>> remove_extra_whitespace("  Hello   World  ")
        'Hello World'
    """
    return re.sub(r"\s+", " ", text.strip())


def is_null_or_blank(text: Optional[str]) -> bool:
    """
    Checks if a string is None, empty, or whitespace only.

    Args:
        text (Optional[str]): Input string.

    Returns:
        bool: True if null or blank, False otherwise.

    Example:
        >>> is_null_or_blank("   ")
        True
    """
    return not text or text.strip() == ""


def slugify(text: str) -> str:
    """
    Converts a string into a safe format for file names or identifiers.

    Args:
        text (str): Input string.

    Returns:
        str: Slugified string.

    Example:
        >>> slugify("Energy Report - 2025!")
        'energy_report_2025'
    """
    return re.sub(r"[^\w]+", "_", text.strip()).strip("_").lower()


def transform_column_name(
    column_name: str,
    case_style: str,
    validate: bool = True
) -> str:
    """
    Transform a column name to the specified case style.

    Args:
        column_name (str): The column name to transform.
        case_style (str): The desired case style. Supported styles are:
                        - "snake_case"
                        - "camelCase"
                        - "PascalCase"
                        - "lowercase"
                        - "uppercase"
        validate (bool): Whether to validate the inputs (default is True).

    Returns:
        str: The transformed column name.

    Example:
        ```python
        from my_module import (
            PandasValidationUtils
        )

        # Transform to snake_case
        result = PandasValidationUtils.transform_column_name(
            column_name="My Column Name", case_style="snake_case"
        )
        print(result)  # Output: my_column_name

        # Transform to camelCase
        result = PandasValidationUtils.transform_column_name(
            column_name="My Column Name", case_style="camelCase"
        )
        print(result)  # Output: myColumnName

        # Transform to PascalCase
        result = PandasValidationUtils.transform_column_name(
            column_name="My Column Name", case_style="PascalCase"
        )
        print(result)  # Output: MyColumnName

        # Transform to lowercase
        result = PandasValidationUtils.transform_column_name(
            column_name="My Column Name", case_style="lowercase"
        )
        print(result)  # Output: my column name

        # Transform to uppercase
        result = PandasValidationUtils.transform_column_name(
            column_name="My Column Name", case_style="uppercase"
        )
        print(result)  # Output: MY COLUMN NAME
        ```
    """
    if case_style == "snake_case":
        return re.sub(
            r"(_|-|\s)+", " ", column_name).lower().replace(" ", "_")
    elif case_style == "camelCase":
        temp = re.sub(
            r"(_|-|\s)+", " ", column_name).title().replace(" ", "")
        return temp[0].lower() + temp[1:]
    elif case_style == "PascalCase":
        return re.sub(
            r"(_|-|\s)+", " ", column_name).title().replace(" ", "")
    elif case_style == "lowercase":
        return column_name.lower()
    elif case_style == "uppercase":
        return column_name.upper()
    return column_name
