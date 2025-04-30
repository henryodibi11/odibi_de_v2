"""
Decorator to enforce the return type of a function.
"""

from functools import wraps
from odibi_de_v2.logger.decorator import log_exceptions
from odibi_de_v2.core.enums import ErrorType


def ensure_output_type(expected_type: type):
    """
    Ensures that the return value of the decorated function matches the
        expected type.

    Args:
        expected_type (type): The type the function must return
            (e.g., pd.DataFrame).

    Raises:
        TypeError: If the return type does not match.

    Example:
        >>> @ensure_output_type(pd.DataFrame)
        ... def load(): ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if not isinstance(result, expected_type):
                _log_output_type_error(
                    method=func.__name__,
                    message=f"Expected return type {expected_type.__name__}, "
                            f"but got {type(result).__name__}"
                )
            return result
        return wrapper
    return decorator


@log_exceptions(
    module="CORE",
    component="output_validation",
    level="error",
    error_type=ErrorType.TYPE_ERROR,
    raise_exception=True
)
def _log_output_type_error(method: str, message: str):
    raise TypeError(message)
