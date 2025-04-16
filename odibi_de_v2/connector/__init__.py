from .azure import AzureBlobConnection
from .local import LocalConnection


__all__ = [
    "AzureBlobConnection",
    "LocalConnection"
]
