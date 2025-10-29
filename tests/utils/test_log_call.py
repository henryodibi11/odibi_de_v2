import pytest
from odibi_de_v2.utils.decorators.log_call import log_call


@log_call(module="TEST", component="logging")
def add(x, y):
    return x + y


def test_log_call_logs_arguments(caplog):
    with caplog.at_level("INFO"):
        result = add(2, 3)
        assert result == 5