import pytest


def test_log_info_with_metadata(logger_with_metadata):

    logger_with_metadata.log("info", "Data ingestion started")
    logs = logger_with_metadata.get_logs()
    print(logs)
    assert any("project=OEE" in log for log in logs)
    assert any("step=ingest" in log for log in logs)
    assert any("Data ingestion started" in log for log in logs)


def test_set_log_level_and_log_debug(logger_with_metadata):
    logger_with_metadata.set_log_level("DEBUG")
    logger_with_metadata.log("debug", "Debug mode active")
    logs = logger_with_metadata.get_logs()
    assert any("Debug mode active" in log for log in logs)


def test_clear_logs(logger_with_metadata):
    logger_with_metadata.log("info", "First message")
    assert len(logger_with_metadata.get_logs()) > 0

    logger_with_metadata.clear_logs()
    assert logger_with_metadata.get_logs() == []


def test_invalid_log_level_raises(logger_with_metadata):
    with pytest.raises(ValueError):
        logger_with_metadata.log("nonsense", "This shouldn't work")


def test_set_invalid_log_level_raises(logger_with_metadata):
    with pytest.raises(ValueError):
        logger_with_metadata.set_log_level("BOGUS")
