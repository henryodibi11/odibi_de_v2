from typing import Any


def safe_eval_lambda(obj: Any) -> Any:
    """
    Evaluates a string that represents a lambda function and returns the resulting lambda function, otherwise returns
    the input object unchanged.

    This function is designed to safely evaluate strings that start with 'lambda', converting them into actual lambda
    functions. If the input is not a string or does not start with 'lambda', the input is returned as is.

    Args:
        obj (Any): The object to evaluate. This can be any type, but if it is a string that starts with 'lambda',
        it will be evaluated as a lambda function.

    Returns:
        Any: If `obj` is a string representing a lambda function, returns the evaluated lambda function. Otherwise,
        returns `obj` unchanged.

    Raises:
        SyntaxError: If the string is malformed and cannot be evaluated as a lambda.
        TypeError: If the evaluation fails due to incorrect usage of types within the lambda expression.

    Example:
        >>> safe_eval_lambda("lambda x: x + 1")
        <function <lambda> at 0x7f3d2c0e1ee0>
        >>> safe_eval_lambda("lambda x, y: x + y")
        <function <lambda> at 0x7f3d2c0e1f70>
        >>> safe_eval_lambda(10)
        10
    """

    if isinstance(obj, str) and obj.strip().startswith("lambda"):
        return eval(obj)
    return obj