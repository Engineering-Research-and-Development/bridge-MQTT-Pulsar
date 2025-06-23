from abc import ABC, abstractmethod

class Publisher(ABC):
    @abstractmethod
    def connect(self) -> bool:
        """Connects to the publishing backend."""
        raise NotImplementedError

    @abstractmethod
    def publish(self, client, userdata, msg):
        """Publishes a message received from MQTT."""
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        """Stops the publisher and cleans up resources."""
        raise NotImplementedError