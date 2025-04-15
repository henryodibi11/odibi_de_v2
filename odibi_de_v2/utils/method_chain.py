import inspect
from typing import Any, Callable, Union

from odibi_de_v2.logger.log_helpers import (
    log_and_optionally_raise)
from odibi_de_v2.utils.type_checks import(
    is_empty_dict)

from odibi_de_v2.core.enums import ErrorType
from odibi_de_v2.utils.validation_utils import (
    validate_non_empty_dict, validate_method_exists,
    validate_kwargs_match_signature)


def apply_dynamic_method(
    obj: Any,
    method: Callable,
    args: Union[dict, list, tuple, Any]
        ) -> Any:
    """
    Dynamically applies a method to an object using various types of arguments.
    Args:
        obj (Any): The object being transformed (can be passed through or unused).
        method (Callable): The method to invoke dynamically.
        args (Any): The arguments to pass:
            - If dict: passed as keyword arguments (**args)
            - If list/tuple: passed as positional arguments (*args)
            - Otherwise: passed directly as a single argument
    Returns:
        Any: The result of the method call.
    Example:
        >>> def add(a, b): return a + b
        >>> apply_dynamic_method(None, add, {"a": 2, "b": 3})
            5
        >>> def echo(x): return x
        >>> apply_dynamic_method(None, echo, "hello")
            'hello'
        >>> def combine(a, b, c): return a + b + c
        >>> apply_dynamic_method(None, combine, [1, 2, 3])
        6
   """
    if isinstance(args, dict):
        return method(**args)
    elif isinstance(args, (list, tuple)):
        return method(*args)
    else:
        return method(args)


def run_method_chain(obj: Any, methods_with_args: dict, validate: bool = False) -> Any:
    """
    Applies a chain of method calls to a given object based on a dictionary of
    method names and corresponding arguments.
    Args:
        obj (Any): The object to apply the methods to
            (e.g., a pandas DataFrame).
        methods_with_args (dict): A dictionary where each key is a method name
            (as a string) and each value is a dict, list/tuple, or single
            argument.
        validate (bool): Whether to validate that the arguments match each
            method's signature.
    Returns:
        Any: The resulting object after all method calls have been applied.
    Raises:
        ValueError: If invalid kwargs are passed or a method call fails.
    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({"name": ["Alice", None], "score": [90, None]})
        >>> method_chain = {
            ...     "fillna": {"value": {"name": "unknown", "score": 0}},
            ...     "rename": {"columns": {"score": "final_score"}}
            ... }
        >>> run_method_chain(df, method_chain)
                name  final_score
            0  Alice          90.0
            1  unknown         0.0
    """
    validate_non_empty_dict(
        methods_with_args,
        "run_method_chain - methods_with_args")

    for method_name, args in methods_with_args.items():
        validate_method_exists(
            obj,
            method_name,
            f"run_method_chain - {method_name}")
        if not hasattr(obj, method_name):
            continue

        method = getattr(obj, method_name)

        if validate:
            validate_kwargs_match_signature(
                method=method,
                kwargs=args,
                method_name=method_name,
                context="run_method_chain - {method_name}",
                raise_error=True)

        # Apply the method
        try:
            obj = apply_dynamic_method(obj, method, args)
        except Exception as e:
            error_msg = f"Error while calling '{method_name}': {e}"
            log_and_optionally_raise(
                    module="UTILS",
                    component="method_chain",
                    method="run_method_chain",
                    error_type=ErrorType.VALUE_ERROR,
                    message=error_msg,
                    level="error",
                    raise_exception=True,
                    raise_type=ValueError)

    return obj
