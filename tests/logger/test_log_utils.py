from odibi_de_v2.logger.log_utils import log_info, log_error
from odibi_de_v2.logger.log_singleton import refresh_logger


def test_log_info_is_captured():
    logger = refresh_logger()
    log_info("Test info message")
    logs = logger.get_logs()
    assert any("INFO" in log and "Test info message" in log for log in logs)


def test_log_error_is_captured():
    logger = refresh_logger()
    log_error("This is an error")
    logs = logger.get_logs()
    assert any("ERROR" in log and "This is an error" in log for log in logs)
