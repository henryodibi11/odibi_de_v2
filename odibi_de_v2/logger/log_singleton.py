import threading
from logger.metadata_manager import MetadataManager
from logger.dynamic_logger import DynamicLogger


_logger_instance = None
_logger_lock = threading.Lock()


def get_logger(metadata_manager: MetadataManager = None) -> DynamicLogger:
    """
    Get the singleton instance of DynamicLogger.
    Initializes the logger on first call, using the provided metadata manager.
    Subsequent calls return the same logger instance.
    Args:
        metadata_manager (MetadataManager, optional): Optional metadata manager
            to use when initializing the logger. If None, a default
            manager is used with basic project/table values.
    Returns:
        DynamicLogger: The singleton logger instance.
    Example:
    >>> logger = get_logger()
    >>> logger.log("info", "Pipeline started")
    """
    global _logger_instance
    with _logger_lock:
        if _logger_instance is None:
            if metadata_manager is None:
                metadata_manager = MetadataManager()
                metadata_manager.update_metadata(
                    project="DefaultProject",
                    table="DefaultTable"
                )
            _logger_instance = DynamicLogger(metadata_manager)
    return _logger_instance


def refresh_logger(metadata_manager: MetadataManager = None) -> DynamicLogger:
    """
    Reset and reinitialize the logger instance.
    This is useful if you need to change the metadata manager or
    reset the logger during runtime (e.g., in tests or isolated runs).
    Args:
        metadata_manager (MetadataManager, optional): New metadata manager
            to use. If None, a default one is created.
    Returns:
        DynamicLogger: The refreshed logger instance.
    Example:
    >>> new_manager = MetadataManager()
    >>> new_manager.update_metadata(project="NewProject")
    >>> logger = refresh_logger(new_manager)
    """
    global _logger_instance
    with _logger_lock:
        _logger_instance = None
    return get_logger(metadata_manager)
