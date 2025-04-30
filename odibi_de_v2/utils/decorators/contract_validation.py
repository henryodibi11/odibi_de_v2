"""
Decorator to validate that function parameters conform to expected
    base classes.

This is used to enforce that critical framework components inherit
    from expected
abstract base classes (e.g., ReaderFactory, CloudConnector).
"""

from functools import wraps
from typing import Callable, Dict, Type
import inspect
from odibi_de_v2.logger.decorator import log_exceptions
from odibi_de_v2.core.enums import ErrorType


def validate_core_contracts(
    param_contracts: Dict[str, Type],
    *,
    allow_none: bool = True
        ):
    """
    Decorator to validate that parameters match expected base classes or
        interfaces.

    Args:
        param_contracts (Dict[str, Type]): Mapping of parameter names to
            base classes.
        allow_none (bool): If True, skips validation for parameters with
            value None.

    Raises:
        TypeError: If parameter fails validation (unless `allow_none=True`
            and param is None).

    Example:
        >>> @validate_core_contracts({
        ...     "reader": ReaderFactory,
        ...     "connector": CloudConnector
        ... })
        ... def __init__(self, reader, connector): ...
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            for param, expected_class in param_contracts.items():
                value = bound_args.arguments.get(param)

                if value is None:
                    if allow_none:
                        continue
                    else:
                        _log_contract_violation(
                            method=func.__name__,
                            message=f"Missing required parameter '{param}'"
                        )

                if inspect.isclass(value):
                    if not issubclass(value, expected_class):
                        _log_contract_violation(
                            method=func.__name__,
                            message=(
                                f"Parameter '{param}' must inherit from "
                                f"{expected_class.__name__}, got "
                                f"{value.__name__}")
                        )
                else:
                    if not isinstance(value, expected_class):
                        _log_contract_violation(
                            method=func.__name__,
                            message=(
                                f"Parameter '{param}' must be an instance of "
                                f"{expected_class.__name__}, got "
                                f"{type(value).__name__}")
                        )
            return func(*args, **kwargs)
        return wrapper
    return decorator


@log_exceptions(
    module="CORE",
    component="contract_validation",
    level="error",
    error_type=ErrorType.VALUE_ERROR,
    raise_exception=True
)
def _log_contract_violation(method: str, message: str):
    raise TypeError(message)
