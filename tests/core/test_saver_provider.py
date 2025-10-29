from core.saver_provider import SaverProvider
from core.enums import DataType


class FakeSaverProvider(SaverProvider):
    def create_saver(self, storage_unit, object_name):
        path = self.connector.get_file_path(storage_unit, object_name)
        return self.factory.csv_saver(path)


def test_create_saver_calls_factory_and_connector(
    mock_factory,
    mock_connector
        ):
    provider = FakeSaverProvider(mock_factory, DataType.CSV, mock_connector)
    result = provider.create_saver("my_bucket", "data.csv")

    mock_connector.get_file_path.assert_called_once_with(
        "my_bucket",
        "data.csv")
    mock_factory.csv_saver.assert_called_once_with("mock/path.csv")

    assert result is mock_factory.csv_saver.return_value
