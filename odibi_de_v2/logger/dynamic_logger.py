import logging
from logger.metadata_manager import MetadataManager
from logger.capturing_handler import CapturingHandler


class DynamicLogger:
    """
    A logger that supports dynamic metadata injection, console output,
    and in-memory log capturing for testing or inspection.
    Attributes:
        metadata_manager (MetadataManager): Metadata manager instance.
        logger (logging.Logger): Underlying logger object.
        capturing_handler (CapturingHandler): Captures logs in memory.
    Example:
    >>> manager = MetadataManager()
    >>> manager.update_metadata(project="OEE", step="save")
    >>> dynamic_logger = DynamicLogger(manager)
    >>> dynamic_logger.log("info", "Starting process")
    >>> dynamic_logger.get_logs()
        ['project=OEE - step=save - Starting process']
        """
    def __init__(
        self,
        metadata_manager: MetadataManager,
        logger_name: str = "DynamicLogger"
            ):
        """
        Initialize the DynamicLogger and attach handlers and filters.
        Args:
            metadata_manager (MetadataManager): Provides dynamic metadata.
            logger_name (str): Name of the logger (default: 'DynamicLogger').
        """
        self.metadata_manager = metadata_manager
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False
        self.capturing_handler = self._ensure_handlers()
        # Clear duplicate filters
        self.logger.filters.clear()
        self.logger.addFilter(self._create_metadata_filter())

    def _ensure_handlers(self) -> CapturingHandler:
        """
        Attach a StreamHandler (console) and CapturingHandler (memory),
        if not already present.
        Returns:
            CapturingHandler: The capturing handler instance.
        """
        dynamic_formatter = self._dynamic_formatter()
        # Add StreamHandler if missing
        if not any(
            isinstance(
                h, logging.StreamHandler) for h in self.logger.handlers):
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(dynamic_formatter)
            self.logger.addHandler(stream_handler)
        # Add or retrieve CapturingHandler
        capturing_handler = next(
            (h for h in self.logger.handlers if isinstance(
                h, CapturingHandler)), None
        )
        if not capturing_handler:
            capturing_handler = CapturingHandler()
            capturing_handler.setFormatter(dynamic_formatter)
            self.logger.addHandler(capturing_handler)
        return capturing_handler

    def _dynamic_formatter(self) -> logging.Formatter:
        """
        Create a formatter that injects metadata into log output.
        Returns:
            logging.Formatter: Custom formatter with metadata support.
        """
        class DynamicFormatter(logging.Formatter):
            def format(inner_self, record):
                metadata = getattr(record, "metadata", {})
                metadata_str = " - ".join(
                    f"{k}={v}" for k, v in sorted(metadata.items()))
                base_message = super(
                    DynamicFormatter, inner_self).format(record)
                return (
                    f"{metadata_str} - {base_message}"
                    ) if metadata else base_message
        return DynamicFormatter("%(asctime)s - %(levelname)s - %(message)s")

    def _create_metadata_filter(self) -> logging.Filter:
        """
        Create a filter that injects metadata into each log record.
        Returns:
            logging.Filter: Metadata-injecting filter.
        """
        class MetadataFilter(logging.Filter):
            def __init__(self, manager: MetadataManager):
                super().__init__()
                self.manager = manager

            def filter(self, record):
                record.metadata = self.manager.get_metadata()
                return True
        return MetadataFilter(self.metadata_manager)

    def log(self, level: str, message: str):
        """
        Log a message at the specified level.
        Args:
            level (str): Log level (e.g., 'info', 'error', 'debug').
            message (str): Message to log.
        Raises:
            ValueError: If the level is not a valid logging level.
        """
        log_method = getattr(self.logger, level.lower(), None)
        if callable(log_method):
            log_method(message)
        else:
            self.logger.error(f"Invalid log level: {level}")
            raise ValueError(f"Invalid log level: {level}")

    def get_logs(self) -> list:
        """
        Return captured log messages.
        Returns:
            list: Captured logs.
        """
        return self.capturing_handler.get_logs()

    def clear_logs(self):
        """
        Clear all captured logs.
        """
        self.capturing_handler.clear_logs()

    def set_log_level(self, level: str):
        """
        Change the logging level dynamically.
        Args:
            level (str): New log level (e.g., 'DEBUG', 'INFO').
        Raises:
            ValueError: If the level is not valid.
        """
        level = level.upper()
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if level not in valid:
            self.logger.error(f"Invalid logging level: {level}")
            raise ValueError(f"Invalid logging level: {level}")
        self.logger.setLevel(level)
        for handler in self.logger.handlers:
            handler.setLevel(level)
        self.logger.info(f"Logging level set to {level}")
