from functools import wraps
from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.logger.log_helpers import log_and_optionally_raise

def log_exceptions(
    module: str,
    component: str,
    *,
    level: str = "error",
    error_type: ErrorType = ErrorType.VALUE_ERROR,
    raise_exception: bool = True,
    raise_type: Exception = ValueError
    ):
    """
    Decorator that wraps a function with standardized exception logging
    and optional re-raising of the exception.

    Args:
        module (str): Top-level module name (e.g., "INGESTION", "UTILS").
        component (str): Component or submodule name
            (e.g., "reader", "validator").
        level (str): Logging level — "error", "warning", or "info".
        error_type (ErrorType): Structured error category for tracking/logging.
        raise_exception (bool): Whether to raise the caught exception.
        raise_type (Exception): Type of exception to raise
            (if `raise_exception` is True).

    Returns:
        Callable: The wrapped function with logging behavior applied.

    Example:
        >>> @log_exceptions(
        ...    module="UTILS",
        ...    component="testing",
        ...    raise_exception=True)
        ... def risky_fn():
        ...     raise ValueError("bad input")
        ...
        >>> risky_fn()
        ValueError: [UTILS.testing.risky_fn] VALUE_ERROR: bad input
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                log_and_optionally_raise(
                    module=module,
                    component=component,
                    method=fn.__name__,
                    message=str(e),
                    error_type=error_type,
                    level=level,
                    raise_exception=raise_exception,
                    raise_type=raise_type
                )
        return wrapper
    return decorator
