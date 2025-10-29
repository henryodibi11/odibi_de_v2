import pytest
from odibi_de_v2.logger.metadata_manager import MetadataManager


def test_initial_metadata_is_empty():
    manager = MetadataManager()
    assert manager.get_metadata() == {}


def test_update_metadata_adds_keys():
    manager = MetadataManager()
    manager.update_metadata(project="OEE", step="extract")
    assert manager.get_metadata() == {"project": "OEE", "step": "extract"}


def test_update_metadata_overwrites_when_cleared():
    manager = MetadataManager()
    manager.update_metadata(project="OEE", step="extract")
    manager.update_metadata(clear_existing=True, phase="load")
    assert manager.get_metadata() == {"phase": "load"}
    assert "project" not in manager.get_metadata()


def test_update_metadata_appends_when_not_cleared():
    manager = MetadataManager()
    manager.update_metadata(project="OEE")
    manager.update_metadata(step="transform")
    assert manager.get_metadata() == {"project": "OEE", "step": "transform"}


def test_update_metadata_raises_error_on_invalid_clear_flag():
    manager = MetadataManager()
    with pytest.raises(ValueError) as excinfo:
        manager.update_metadata(clear_existing="yes", foo="bar")
    assert "'clear_existing' must be a boolean." in str(excinfo.value)
