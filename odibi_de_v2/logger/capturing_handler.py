import logging


class CapturingHandler(logging.Handler):
    """
    A custom logging handler that captures log messages for programmatic
    access.This handler is useful for testing, debugging, or collecting logs
    that need to be processed or inspected within the application, rather than
    output to a file or console.
    Attributes:
        records (list): A list of formatted log messages captured during
            runtime.
    Example:
    >>> handler = CapturingHandler()
    >>> logger = logging.getLogger("test_logger")
    >>> logger.setLevel(logging.INFO)
    >>> logger.addHandler(handler)
    >>> logger.info("Hello, logs!")
    >>> handler.get_logs()
        ['Hello, logs!']
    """
    def __init__(self):
        """
        Initialize the CapturingHandler with an empty list of log records.
        """
        super().__init__()
        self.records = []

    def emit(self, record):
        """
        Capture and format the log message when a logging event occurs.
        Args:
            record (LogRecord): A log record containing the event metadata.
        """
        self.records.append(self.format(record))

    def get_logs(self) -> list:
        """
        Retrieve all captured log messages.
        Returns:
            list: A list of formatted log strings.
        """
        return self.records

    def clear_logs(self):
        """
        Clear all captured log messages from memory.
        """
        self.records.clear()
