"""
safe_eval.py

Provides safe evaluation utilities for odibi_de, using the SAFE_GLOBALS
environment defined in utils/safe_globals.py.
"""

from typing import Any, Dict


def safe_eval_lambda(obj: Any, extra_globals: Dict[str, Any] = None) -> Any:
    """
    Safely evaluate string-based lambda functions in a restricted environment.

    This utility converts strings that begin with ``"lambda"`` into actual
    Python lambda functions, evaluated inside a sandboxed global environment.
    The sandbox removes all dangerous builtins (e.g., ``open``, ``__import__``)
    and whitelists only a curated set of safe modules and functions
    (see :mod:`odibi_de_v2.utils.safe_globals`).

    If the input is not a string or does not start with ``"lambda"``,
    the object is returned unchanged.

    Args:
        obj (Any):
            The object to evaluate. If this is a string starting with
            ``"lambda"``, it will be compiled into a function.
        extra_globals (Dict[str, Any], optional):
            Additional modules or functions to expose inside the lambda's
            evaluation environment (e.g., ``{"Decimal": Decimal}``).

    Returns:
        Any: 
            - A callable lambda function if ``obj`` was a string lambda.
            - The original object unchanged otherwise.

    Raises:
        TypeError: If evaluation fails due to a syntax or runtime error.

    Examples
    --------
    Basic numeric transform:
        >>> fn = safe_eval_lambda("lambda row: row['x'] * 1.05")
        >>> fn({"x": 100})
        105.0

    Using math functions (whitelisted by default):
        >>> fn = safe_eval_lambda("lambda row: math.sqrt(row['value'])")
        >>> fn({"value": 16})
        4.0

    Regex-based text cleaning:
        >>> fn = safe_eval_lambda("lambda row: re.sub(r'\\d+', '', row['col'])")
        >>> fn({"col": "temp123"})
        'temp'

    Date parsing with datetime:
        >>> fn = safe_eval_lambda(
        ...     "lambda row: datetime.datetime.strptime(row['ts'], '%Y-%m-%d')"
        ... )
        >>> fn({"ts": "2025-09-19"})
        datetime.datetime(2025, 9, 19, 0, 0)

    Extending with extra globals:
        >>> from decimal import Decimal
        >>> fn = safe_eval_lambda(
        ...     "lambda row: Decimal(row['amount']) * 1.10",
        ...     extra_globals={"Decimal": Decimal}
        ... )
        >>> fn({"amount": "100"})
        Decimal('110.0')
    """
    from odibi_de_v2.utils import  SAFE_GLOBALS

    safe_globals = dict(SAFE_GLOBALS)  # copy defaults
    if extra_globals:
        safe_globals.update(extra_globals)

    if isinstance(obj, str) and obj.strip().startswith("lambda"):
        try:
            return eval(obj, safe_globals, {})
        except Exception as e:
            raise TypeError(f"Failed to evaluate lambda: {obj!r}. Error: {e}")
    return obj
