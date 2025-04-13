import pytest
from utils.string_utils import (
    normalize_string,
    clean_column_name,
    standardize_column_names,
    to_snake_case,
    to_kebab_case,
    to_camel_case,
    to_pascal_case,
    remove_extra_whitespace,
    is_null_or_blank,
    slugify
)


def test_normalize_string():
    assert normalize_string("  Hello  ") == "hello"
    assert normalize_string("  Hello  ", case="upper") == "HELLO"
    assert normalize_string("hello world", case="title") == "Hello World"
    assert normalize_string(None) == ""
    with pytest.raises(ValueError):
        normalize_string("test", case="weird")


def test_clean_column_name():
    assert clean_column_name("Total Sales ($)") == "total_sales"
    assert clean_column_name("  Name#1@!") == "name_1"


def test_standardize_column_names():
    input_cols = ["First Name", "Last Name", "Total Sales ($)"]
    expected = ["first_name", "last_name", "total_sales"]
    assert standardize_column_names(input_cols) == expected


def test_to_snake_case():
    assert to_snake_case("CamelCaseString") == "camel_case_string"
    assert to_snake_case("simpleTest") == "simple_test"


def test_to_kebab_case():
    assert to_kebab_case("Hello World") == "hello-world"
    assert to_kebab_case("Multiple   Spaces") == "multiple-spaces"


def test_to_camel_case():
    assert to_camel_case("My Variable Name") == "myVariableName"
    assert to_camel_case("   multiple_words_here ") == "multipleWordsHere"


def test_to_pascal_case():
    assert to_pascal_case("my variable name") == "MyVariableName"
    assert to_pascal_case("  multiple_words_here ") == "MultipleWordsHere"


def test_remove_extra_whitespace():
    assert remove_extra_whitespace("  Hello   World  ") == "Hello World"
    assert remove_extra_whitespace("Line   with  gaps") == "Line with gaps"


def test_is_null_or_blank():
    assert is_null_or_blank("") is True
    assert is_null_or_blank("    ") is True
    assert is_null_or_blank(None) is True
    assert is_null_or_blank("valid") is False


def test_slugify():
    assert slugify("Energy Report - 2025!") == "energy_report_2025"
    assert slugify(" Slug/Name.test ") == "slug_name_test"
