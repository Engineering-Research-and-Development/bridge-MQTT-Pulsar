from abc import ABC, abstractmethod
from typing import Callable

MessageCallback = Callable[[dict], None]


class IMessageSource(ABC):
    """
    Defines the contract for any message source (e.g., MQTT, OPC UA)
    that the bridge can connect to.
    """

    @abstractmethod
    def connect(self) -> bool:
        """Establishes the connection to the data source."""
        raise NotImplementedError

    @abstractmethod
    def start(self, on_message_callback: MessageCallback):
        """
        Starts the process of listening or subscribing for data.

        This method should be non-blocking or run in its own thread,
        allowing the orchestrator to manage multiple sources concurrently.

        Args:
            on_message_callback: A function to be called when a new message
                                 is received. The source is responsible for
                                 invoking this callback with standardized data.
        """
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        """Stops listening and disconnects from the data source."""
        raise NotImplementedError
