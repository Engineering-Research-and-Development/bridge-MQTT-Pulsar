from abc import abstractmethod
from ..sources.interfaces import IConnectable


class IDestination(IConnectable):
    """Defines a contract for any message destination (Pulsar, Kafka...)."""

    @abstractmethod
    def publish(self, message: dict) -> None:
        """Publishes a message to the destination."""
        raise NotImplementedError
