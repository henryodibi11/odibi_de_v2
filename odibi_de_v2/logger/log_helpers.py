from .error_utils import format_error
from odibi_de_v2.core.enums import ErrorType
from .log_singleton import get_logger


def log_info(message: str):
    """
    Log an info-level message using the global logger.
    Args:
        message (str): The message to log.
    """
    get_logger().log("info", message)


def log_debug(message: str):
    """
    Log a debug-level message using the global logger.
    Args:
        message (str): The message to log.
    """
    get_logger().log("debug", message)


def log_warning(message: str):
    """
    Log a warning-level message using the global logger.
    Args:
        message (str): The message to log.
    """
    get_logger().log("warning", message)


def log_error(message: str):
    """
    Log an error-level message using the global logger.
    Args:
        message (str): The message to log.
    """
    get_logger().log("error", message)


def log_and_optionally_raise (
    *,
    module: str,
    component: str,
    method: str = None,
    error_type: ErrorType = ErrorType.NO_ERROR,
    message: str,
    level: str = "error",
    raise_exception: bool = False,
    raise_type: Exception = ValueError
        ) -> None:
    """
    Formats and logs a structured error/warning/info message and optionally raises an exception.

    Args:
        module (str): High-level module name (e.g., "INGESTION", "UTILS").
        component (str): Component or submodule name (e.g., "validation_utils").
        method (str, optional): The specific method name associated with the log.
        message (str): The message content to log and/or raise.
        error_type (ErrorType): Structured error type for logging metadata.
        level (str): Logging level — "error", "warning", or "info".
        raise_exception (bool): Whether to raise an exception.
        raise_type (Exception): Exception type to raise if `raise_exception=True`.

    Raises:
        Exception: The specified `raise_type`, only if `raise_exception` is True.

    Example:
    >>> log_and_optionally_raise(
        ...     module="UTILS",
        ...     component="validator",
        ...     method="check_config",
        ...     message="Missing required key",
        ...     error_type=ErrorType.VALUE_ERROR,
        ...     level="error",
        ...     raise_exception=True
        ... )
        ValueError: [UTILS.validator.check_config] VALUE_ERROR: Missing required key
    """
    formatted_msg = format_error(
        module=module,
        component=component,
        method=method,
        error_type=error_type,
        message=message
    )

    level = level.lower()
    if level == "error":
        log_error(formatted_msg)
    elif level == "warning":
        log_warning(formatted_msg)
    else:
        log_info(formatted_msg)

    if raise_exception:
        raise raise_type(formatted_msg)

