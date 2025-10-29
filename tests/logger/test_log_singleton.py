from odibi_de_v2.logger.log_singleton import get_logger, refresh_logger
from odibi_de_v2.logger.metadata_manager import MetadataManager


def test_get_logger_returns_same_instance():
    logger1 = get_logger()
    logger2 = get_logger()
    assert logger1 is logger2


def test_refresh_logger_returns_new_instance():
    old_logger = get_logger()
    new_logger = refresh_logger()
    assert old_logger is not new_logger


def test_get_logger_with_custom_metadata():
    manager = MetadataManager()
    manager.update_metadata(project="Custom", step="transform")
    logger = refresh_logger(manager)
    logger.log("info", "Custom metadata test")
    captured = logger.get_logs()[-1]
    assert "project=Custom" in captured
    assert "step=transform" in captured


def test_refresh_logger_with_new_metadata():
    manager = MetadataManager()
    manager.update_metadata(task="refresh_test")
    logger = refresh_logger(manager)
    logger.log("info", "Fresh start")
    captured = logger.get_logs()[-1]
    assert "task=refresh_test" in captured
