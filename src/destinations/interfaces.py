from abc import abstractmethod
from ..core.message import Message


class IDestination:
    """Defines a contract for any message destination (Pulsar, Kafka...)."""

    @abstractmethod
    def publish(self, message: Message, destination_topic: str) -> None:
        """Publishes a standardized Message object to a specific destination topic."""
        raise NotImplementedError
