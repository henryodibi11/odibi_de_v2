"""
Decorator for runtime type enforcement using Python type hints.

This module provides the `@enforce_types` decorator to ensure that function
arguments match the type annotations at runtime. Useful for catching bugs and
improving reliability in dynamic systems or plugin architectures.
"""

from functools import wraps
from typing import get_type_hints, Union, get_origin, get_args
from inspect import signature
from odibi_de_v2.logger.decorator import log_exceptions
from odibi_de_v2.core.enums import ErrorType


def enforce_types(strict: bool = True):
    """
    Decorator that enforces type hints at runtime.

    Args:
        strict (bool): If True, raises TypeError on mismatch.
                    If False, logs the mismatch using `log_exceptions`.

    Returns:
        Callable: The wrapped function with validation behavior.

    Example:
        >>> @enforce_types(strict=True)
        ... def do_something(name: str, retries: int): ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            sig = signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            type_hints = get_type_hints(func)

            for name, value in bound.arguments.items():
                if name not in type_hints:
                    continue

                expected = type_hints[name]
                if not _matches_type(value, expected):
                    expected_str = _type_to_str(expected)
                    msg = (
                        f"Argument '{name}' must be of type {expected_str}, "
                        f"but got {type(value).__name__} with value {value!r}"
                    )

                    if strict:
                        raise TypeError(msg)
                    else:
                        _log_type_warning(
                            method=func.__name__,
                            message=msg
                        )
            return func(*args, **kwargs)
        return wrapper
    return decorator


@log_exceptions(
    module="CORE",
    component="type_validation",
    level="warning",
    error_type=ErrorType.TYPE_ERROR,
    raise_exception=False
)
def _log_type_warning(method: str, message: str):
    raise TypeError(message)  # logged, not raised


def _matches_type(value, expected_type):
    origin = get_origin(expected_type)
    args = get_args(expected_type)

    if origin is Union:
        return any(_matches_type(value, arg) for arg in args)

    if origin is list:
        return isinstance(value, list) and all(
            _matches_type(item, args[0]) for item in value
        )

    if origin is dict:
        return isinstance(value, dict) and all(
            _matches_type(k, args[0]) and _matches_type(v, args[1])
            for k, v in value.items()
        )

    if expected_type is type(None):
        return value is None

    return isinstance(value, expected_type)


def _type_to_str(tp):
    try:
        return str(tp).replace("typing.", "")
    except Exception:
        return str(tp)




