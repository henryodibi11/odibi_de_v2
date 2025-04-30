"""
Decorator to log function calls with arguments for traceability.
"""

from functools import wraps
from inspect import signature
from odibi_de_v2.logger.log_helpers import log_and_optionally_raise


def log_call(module: str = "UNKNOWN", component: str = "unknown"):
    """
    Logs the function name and arguments when the decorated function is called.

    Args:
        module (str): Top-level module name (e.g., "INGESTION").
        component (str): Submodule/component name (e.g., "reader").

    Returns:
        Callable: The wrapped function with logging.

    Example:
        >>> @log_call(module="INGESTION", component="reader")
        ... def read_data(path: str): ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            sig = signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()

            arg_str = ",\n".join(
                f"{k}={v!r}" for k, v in bound.arguments.items() if k != "self"
            )

            log_and_optionally_raise(
                module=module,
                component=component,
                method=func.__name__,
                message=f"Called with:\n{arg_str}",
                level="INFO",
            )

            return func(*args, **kwargs)
        return wrapper
    return decorator
