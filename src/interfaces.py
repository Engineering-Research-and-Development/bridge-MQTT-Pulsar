from abc import ABC, abstractmethod

class TopicRouter(ABC):
    @abstractmethod
    def get_pulsar_topic(self, mqtt_topic: str) -> str | None:
        """
        Determines Pulsar's destination topic given an MQTT topic.
        Returns the Pulsar topic as str or None if the message is to be ignored.
        """
        raise NotImplementedError

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