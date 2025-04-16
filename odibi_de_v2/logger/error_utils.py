from ..core.enums import ErrorType


def format_error(
    module: str,
    component: str,
    method: str,
    error_type: ErrorType,
    message: str
        ) -> str:
    """
    Format a standardized error message for logging and exception handling.

    Args:
        module (str): High-level module name (e.g. 'CONNECTOR').
        component (str): Class or component name (e.g. 'AzureBlobConnector').
        method (str): Method or function name (e.g. 'load_file').
        error_type (ErrorType): Enum representing the type of error.
        message (str): Descriptive error message.

    Returns:
        str: Structured error string in the format:
                '[MODULE].[COMPONENT].[METHOD] - [ERROR_TYPE]: [MESSAGE]'

    Example:
    >>> from core.enums import ErrorType
    >>> format_error(
        ...     module="VALIDATOR",
        ...     component="SchemaValidator",
        ...     method="run",
        ...     error_type=ErrorType.VALUE_ERROR,
        ...     message="Missing field 'site_id'"
        ... )
        'VALIDATOR.SchemaValidator.run - VALUE_ERROR: Missing field \'site_id\'
        '
    """
    return f"{module}.{component}.{method} - ERROR TYPE: {error_type.value} - {message}\n"
