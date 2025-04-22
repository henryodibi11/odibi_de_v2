import pytest
from  odibi_de_v2.utils.decorators.contract_validation import validate_core_contracts


class BaseConnector:
    def connect(self): ...


class CustomConnector(BaseConnector):
    def connect(self): return True


class InvalidConnector:
    pass


@validate_core_contracts({"connector": BaseConnector})
def use_connector(connector):
    return connector.connect()


def test_validate_core_contracts_pass():
    assert use_connector(CustomConnector()) is True


# def test_validate_core_contracts_fail():
#     with pytest.raises(TypeError):
#         use_connector(InvalidConnector())
