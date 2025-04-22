import logging
from odibi_de_v2.logger.capturing_handler import CapturingHandler


def test_captures_single_log_message():
    logger = logging.getLogger("test_logger_1")
    logger.setLevel(logging.INFO)
    handler = CapturingHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.propagate = False  # Avoid printing to console
    logger.info("Hello, world!")
    logs = handler.get_logs()
    assert logs == ["Hello, world!"]


def test_clear_logs():
    handler = CapturingHandler()
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname="", lineno=0,
        msg="Temporary log", args=(), exc_info=None
    )
    handler.emit(record)
    assert handler.get_logs() == ["Temporary log"]
    handler.clear_logs()
    assert handler.get_logs() == []
