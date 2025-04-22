from utils.file_utils import (
    get_file_extension,
    get_stem_name,
    extract_file_name,
    extract_folder_name,
    is_valid_file_path,
    is_supported_format

)


def test_get_file_extension():
    assert get_file_extension("data/file.csv") == "csv"
    assert get_file_extension("dir/file.JSON") == "json"
    assert get_file_extension("a/b/c/file.parquet") == "parquet"
    assert get_file_extension("no_extension") == ""


def test_get_stem_name():
    assert get_stem_name("data/folder/sample.csv") == "sample"
    assert get_stem_name("a/b/c/file.JSON") == "file"
    assert get_stem_name("/mnt/data/archive.gz") == "archive"
    assert get_stem_name("file_without_ext") == "file_without_ext"


def test_extract_file_name():
    assert extract_file_name("folder/abc.json") == "abc.json"
    assert extract_file_name("a/b/c/d/e/report.avro") == "report.avro"
    assert extract_file_name("/root/project/file.csv") == "file.csv"


def test_extract_folder_name():
    assert extract_folder_name("folder/abc.json") == "folder"
    assert extract_folder_name("a/b/c/d/e/report.avro") == "e"
    assert extract_folder_name("/mnt/data/year_2025/file.csv") == "year_2025"
    assert extract_folder_name("file.csv") == ""


def test_is_valid_file_path():
    assert is_valid_file_path("data/file.csv") is True
    assert is_valid_file_path("a/b/c/file.json") is True
    assert is_valid_file_path("file.csv") is True
    assert is_valid_file_path("") is False
    assert is_valid_file_path("folder/") is False


def test_is_supported_format():
    supported = ["csv", "json", "parquet", "avro"]
    assert is_supported_format("file.csv", supported) is True
    assert is_supported_format("a/b/c/file.avro", supported) is True
    assert is_supported_format("file.xlsx", supported) is False
    assert is_supported_format("no_extension", supported) is False
    assert is_supported_format("FOLDER/FILE.PARQUET", supported) is True
