"""
Decorator to measure and log function execution time.
"""

import time
from functools import wraps
from odibi_de_v2.logger.log_helpers import log_and_optionally_raise


def benchmark(module: str = "UNKNOWN", component: str = "unknown"):
    """
    Logs the runtime of a decorated function.

    Args:
        module (str): Top-level module name (e.g., "TRANSFORM").
        component (str): Submodule/component name (e.g., "stage_1").

    Returns:
        Callable: The wrapped function with benchmarking.

    Example:
        >>> @benchmark(module="TRANSFORM", component="stage_1")
        ... def process(df): ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            end = time.time()

            duration = round(end - start, 4)
            log_and_optionally_raise(
                module=module,
                component=component,
                method=func.__name__,
                message=f"Execution time: {duration} seconds",
                level="INFO"
            )
            return result
        return wrapper
    return decorator
