import pytest
from unittest.mock import MagicMock
from odibi_de_v2.logger.metadata_manager import MetadataManager
from odibi_de_v2.logger.dynamic_logger import DynamicLogger
from odibi_de_v2.connector import AzureBlobConnection


@pytest.fixture
def mock_factory():
    factory = MagicMock()
    factory.csv_reader.return_value = MagicMock(name="MockCSVReader")
    factory.csv_saver.return_value = MagicMock(name="MockCSVSaver")
    return factory


@pytest.fixture
def mock_connector():
    connector = MagicMock()
    connector.get_file_path.return_value = "mock/path.csv"
    return connector


@pytest.fixture
def connector():
    return AzureBlobConnection(account_name="myaccount", account_key="fakekey")


@pytest.fixture
def logger_with_metadata():
    manager = MetadataManager()
    manager.update_metadata(project="OEE", step="ingest")
    return DynamicLogger(metadata_manager=manager)
