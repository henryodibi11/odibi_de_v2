from core.reader_provider import ReaderProvider
from core.enums import DataType


class FakeReaderProvider(ReaderProvider):
    def create_reader(self, storage_unit, object_name):
        path = self.connector.get_file_path(storage_unit, object_name)
        return self.factory.csv_reader(path)


def test_create_reader_calls_factory_and_connector(
    mock_factory,
    mock_connector
        ):
    provider = FakeReaderProvider(mock_factory, DataType.CSV, mock_connector)
    result = provider.create_reader("my_bucket", "data.csv")

    mock_connector.get_file_path.assert_called_once_with(
        "my_bucket",
        "data.csv")
    mock_factory.csv_reader.assert_called_once_with("mock/path.csv")

    assert result is mock_factory.csv_reader.return_value
