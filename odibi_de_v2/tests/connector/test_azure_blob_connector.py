# import pytest
from unittest.mock import patch, MagicMock
from odibi_de_v2.core.enums import Framework


@patch("odibi_de_v2.connector.azure.azure_blob_connector.BlobServiceClient")
def test_get_connection(mock_blob_client, connector):
    mock_instance = MagicMock()
    mock_blob_client.return_value = mock_instance

    client = connector.get_connection()

    mock_blob_client.assert_called_once_with(
        account_url="https://myaccount.blob.core.windows.net",
        credential="fakekey"
    )
    assert client == mock_instance


def test_get_file_path_spark(connector):
    path = connector.get_file_path(
        "mycontainer", "myfile.csv", Framework.SPARK)
    expected = (
        "abfss://mycontainer@myaccount"
        ".dfs.core.windows.net/myfile.csv"
    )
    assert path == expected


def test_get_file_path_pandas(connector):
    path = connector.get_file_path(
        "mycontainer", "myfile.csv", Framework.PANDAS)
    expected = "az://mycontainer/myfile.csv"
    assert path == expected


# def test_get_file_path_invalid_enum(connector):
#     with pytest.raises(ValueError):
#         connector.get_file_path("container", "file", "INVALID")


def test_get_framework_config_spark(connector):
    config = connector.get_framework_config(Framework.SPARK)
    expected_key = "fs.azure.account.key.myaccount.dfs.core.windows.net"
    assert config == {expected_key: "fakekey"}


def test_get_framework_config_pandas(connector):
    config = connector.get_framework_config(Framework.PANDAS)
    assert config == {
        "account_name": "myaccount",
        "account_key": "fakekey"
    }


# def test_get_framework_config_invalid_enum(connector):
#     with pytest.raises(ValueError):
#         connector.get_framework_config("INVALID")
