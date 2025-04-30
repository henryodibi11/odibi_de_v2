"""
Decorator for validating that specified function parameters are not empty.

This covers:
- None
- Empty strings
- Empty lists, dicts, sets, tuples

Ideal for ensuring that required inputs are present and usable after
    type checks.
"""

from functools import wraps
from inspect import signature
from odibi_de_v2.logger.decorator import log_exceptions
from odibi_de_v2.core.enums import ErrorType


def validate_non_empty(param_names: list[str]):
    """
    Decorator to ensure that the specified parameters are not None or empty.

    Args:
        param_names (list[str]): List of parameter names to validate.

    Raises:
        ValueError: If any of the specified parameters are empty.

    Example:
        >>> @validate_non_empty(["path", "columns"])
        ... def load_data(path: str, columns: list[str]): ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            sig = signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()

            for param in param_names:
                value = bound.arguments.get(param)
                if _is_empty(value):
                    _log_empty_violation(
                        method=func.__name__,
                        message=(
                            f"Parameter '{param}' "
                            f"must not be empty. Got {value!r}")
                    )

            return func(*args, **kwargs)
        return wrapper
    return decorator


@log_exceptions(
    module="CORE",
    component="value_validation",
    level="error",
    error_type=ErrorType.VALUE_ERROR,
    raise_exception=True
)
def _log_empty_violation(method: str, message: str):
    raise ValueError(message)


def _is_empty(value):
    return (
        value is None or value == "" or value == []
        or value == {} or value == set() or value == ())
