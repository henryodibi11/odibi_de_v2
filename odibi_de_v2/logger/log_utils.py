from logger.log_singleton import get_logger


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
