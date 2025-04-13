from typing import Any


def is_empty_dict(d: Any) -> bool:
    """
    Checks if the given object is an empty dictionary.

    Args:
        d (Any): The object to check.

    Returns:
        bool: True if the object is a dict and is empty, False otherwise.

    Example:
        >>> is_empty_dict({})
            True
        >>> is_empty_dict({'a': 1})
            False
    """
    return isinstance(d, dict) and not d


def is_valid_type(obj: Any, expected_types: tuple) -> bool:
    """
    Checks whether an object is an instance of the expected type(s).

    Args:
        obj (Any): The object to check.
        expected_types (tuple): Tuple of expected types (e.g., (str, int)).

    Returns:
        bool: True if obj is one of the expected types, False otherwise.

    Example:
            >>> is_valid_type(5, (int, float))
                True
            >>> is_valid_type("5", (int, float))
                False
    """
    return isinstance(obj, expected_types)


def is_non_empty_string(value: Any) -> bool:
    """
    Checks if a value is a non-empty string.

    Args:
        value (Any): The value to check.

    Returns:
        bool: True if the value is a non-empty string, False otherwise.

    Example:
        >>> is_non_empty_string("hello")
            True
        >>> is_non_empty_string("")
            False
    """
    return isinstance(value, str) and value.strip() != ""


def is_boolean(value: Any) -> bool:
    """
    Checks if a value is a boolean.

    Args:
        value (Any): The value to check.

    Returns:
        bool: True if the value is a bool, False otherwise.

    Example:
        >>> is_boolean(True)
            True
        >>> is_boolean("True")
            False
    """
    return isinstance(value, bool)
