from functools import wraps
import inspect
from odibi_de_v2.logger import log_exceptions
from odibi_de_v2.core import ErrorType


def validate_input_types(expected_types: dict):
    """
    Validates that specified input arguments match their expected types.

    Args:
        expected_types (dict): Mapping of argument names to expected types.

    Raises:
        TypeError: If any input argument has an incorrect type.

    Example:
        >>> @validate_input_types({"df": pd.DataFrame, "file_path": str})
        ... def save_data(df, file_path): ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            bound = inspect.signature(func).bind(*args, **kwargs)
            bound.apply_defaults()
            for arg_name, expected_type in expected_types.items():
                if arg_name in bound.arguments and not isinstance(bound.arguments[arg_name], expected_type):
                    _log_input_type_error(
                        method=func.__name__,
                        message=f"Argument '{arg_name}' must be of type {expected_type.__name__}, "
                                f"but got {type(bound.arguments[arg_name]).__name__}"
                    )
            return func(*args, **kwargs)
        return wrapper
    return decorator


@log_exceptions(
    module="CORE",
    component="input_validation",
    level="error",
    error_type=ErrorType.TYPE_ERROR,
    raise_exception=True
)
def _log_input_type_error(method: str, message: str):
    raise TypeError(message)
