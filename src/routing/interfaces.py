from abc import ABC, abstractmethod


class TopicRouter(ABC):
    @abstractmethod
    def get_pulsar_topic(self, message: dict) -> str | None:
        """
        Determines Pulsar's destination topic given a standardized message.

        Args:
            message (dict): The standardized message dictionary, containing
                            at least 'source', 'topic', and 'payload'.

        Returns:
            The Pulsar topic as a string, or None if the message should be ignored.
        """
        raise NotImplementedError
