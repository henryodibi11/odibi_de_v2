"""
Decorator to validate that a DataFrame contains required columns.
"""

from functools import wraps
from odibi_de_v2.logger.decorator import log_exceptions
from odibi_de_v2.core.enums import ErrorType


def validate_schema(required_columns: list[str], param_name: str = "df"):
    """
    Validates that the input DataFrame has the specified required columns.

    Args:
        required_columns (list[str]): List of required column names.
        param_name (str): Name of the parameter containing the DataFrame
            (default: "df").

    Raises:
        ValueError: If any required columns are missing.

    Example:
        >>> @validate_schema(["id", "timestamp"], param_name="df")
        ... def clean_data(df): ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            from inspect import signature
            bound = signature(func).bind(*args, **kwargs)
            bound.apply_defaults()

            df = bound.arguments.get(param_name)
            if df is None or not hasattr(df, "columns"):
                _log_schema_violation(
                    method=func.__name__,
                    message=(
                        f"Parameter '{param_name}' is not a "
                        "valid DataFrame-like object.")
                )

            missing = [
                col for col in required_columns if col not in df.columns]
            if missing:
                _log_schema_violation(
                    method=func.__name__,
                    message=f"Missing required columns: {missing}"
                )

            return func(*args, **kwargs)
        return wrapper
    return decorator


@log_exceptions(
    module="CORE",
    component="schema_validation",
    level="error",
    error_type=ErrorType.SCHEMA_ERROR,
    raise_exception=True
)
def _log_schema_violation(method: str, message: str):
    raise ValueError(message)
