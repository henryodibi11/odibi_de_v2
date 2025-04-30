import pytest
from odibi_de_v2.utils.decorators.validate_non_empty import validate_non_empty


@validate_non_empty(["text", "items"])
def process(text: str, items: list):
    return f"{text} ({len(items)})"


def test_validate_non_empty_pass():
    result = process("Valid Input", [1, 2, 3])
    assert result == "Valid Input (3)"


def test_validate_non_empty_empty_string():
    with pytest.raises(ValueError):
        process("", [1, 2])


def test_validate_non_empty_empty_list():
    with pytest.raises(ValueError):
        process("Something", [])


def test_validate_non_empty_none_value():
    with pytest.raises(ValueError):
        process(None, [1])
