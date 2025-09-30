from abc import ABC
from ..core.process import IProcess


# solo per definizione di cos'è una source o butto tutto qua dentro e bona?
class ISource(IProcess, ABC):
    """
    Defines the contract for any message source (e.g., MQTT, OPC UA)
    that the bridge can connect to. Each source wil be run in
    its own independent process.
    """

    pass
