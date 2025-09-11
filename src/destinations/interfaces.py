from abc import abstractmethod
from ..sources.interfaces import IConnectable
from ..core.message import Message


class IDestination(IConnectable):
    """Defines a contract for any message destination (Pulsar, Kafka...)."""

    @abstractmethod
    def publish(self, message: Message, destination_topic: str) -> None:
        """Publishes a standardized Message object to a specific destination topic."""
        raise NotImplementedError
