import pytest
from odibi_de_v2.utils.decorators.type_enforcement import enforce_types


@enforce_types(strict=True)
def strict_func(name: str, age: int):
    return f"{name} is {age} years old"


@enforce_types(strict=False)
def lenient_func(tags: list[str]):
    return ", ".join(tags) if tags else "No tags"


def test_enforce_types_strict_pass():
    result = strict_func("Henry", 30)
    assert result == "Henry is 30 years old"


def test_enforce_types_strict_fail():
    with pytest.raises(TypeError):
        strict_func("Henry", "thirty")  # age must be int


# def test_enforce_types_lenient_logs(caplog):
#     lenient_func("not_a_list")
#     assert "Argument 'tags' must be of type list[str]" in caplog.text
