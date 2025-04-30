from utils.type_checks import (
    is_empty_dict,
    is_valid_type,
    is_non_empty_string,
    is_boolean)


def test_is_empty_dict():
    assert is_empty_dict({}) is True
    assert is_empty_dict({"a": 1}) is False
    assert is_empty_dict([]) is False
    assert is_empty_dict(None) is False


def test_is_valid_type():
    assert is_valid_type(5, (int, float)) is True
    assert is_valid_type("5", (int, float)) is False
    assert is_valid_type(True, (bool,)) is True


def test_is_non_empty_string():
    assert is_non_empty_string("hello") is True
    assert is_non_empty_string("") is False
    assert is_non_empty_string("   ") is False
    assert is_non_empty_string(None) is False


def test_is_boolean():
    assert is_boolean(True) is True
    assert is_boolean(False) is True
    assert is_boolean("False") is False
    assert is_boolean(0) is False
