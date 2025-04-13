from core.reader import DataReader
from unittest.mock import MagicMock


class FakeDataReader(DataReader):
    def read_data(self):
        mock_df = MagicMock()
        mock_df.head.return_value = "yes it worked"
        return mock_df


def test_read_sample_data_calls_head():
    reader = FakeDataReader("filepath")
    result = reader.read_sample_data(3)
    assert result == "yes it worked"
