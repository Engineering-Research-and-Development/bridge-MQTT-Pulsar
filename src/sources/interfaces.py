from abc import ABC, abstractmethod
from typing import Callable

MessageCallback = Callable[["ISource", dict], None]


class IConnectable(ABC):
    """Defines a contract for components that have a connect/stop lifecycle."""

    @abstractmethod
    def connect(self) -> bool:
        """Establishes the connection to the endpoint."""
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        """Stops the component and cleans up resources."""
        raise NotImplementedError


class ISource(IConnectable):
    """
    Defines the contract for any message source (e.g., MQTT, OPC UA)
    that the bridge can connect to.
    """

    @abstractmethod
    def start(self, on_message_callback: MessageCallback) -> None:
        """
        Starts the process of listening for data.

        Args:
            on_message_callback: A function to be called when a new message
                                 is received. The source is responsible for
                                 invoking this callback with standardized data.
        """
        raise NotImplementedError
