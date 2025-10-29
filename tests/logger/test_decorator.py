import pytest
from odibi_de_v2.logger.decorator import log_exceptions
from odibi_de_v2.core.enums import ErrorType
# A dummy function that will raise an error


@log_exceptions(module="TEST", component="test_decorator")
def always_fail():
    raise ValueError("Expected failure")


def test_log_and_raise_decorator():
    with pytest.raises(ValueError) as exc_info:
        always_fail()
    assert "Expected failure" in str(exc_info.value)
