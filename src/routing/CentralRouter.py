from loguru import logger
from .interfaces import TopicRouter


class CentralRouter(TopicRouter):
    """
    Delegates message routing based on the source specified in the message.
    """

    def __init__(self):
        self._strategies: dict[str, TopicRouter] = {}
        logger.debug(
            "Delegation router initialized. Ready to register routing strategies."
        )

    def register_strategy(self, source_name: str, strategy: TopicRouter):
        """Registers the routing strategy for a given source name."""
        self._strategies[source_name] = strategy
        logger.debug(f"Registered routing strategy for source: '{source_name}'")

    def get_pulsar_topic(self, message: dict) -> str | None:
        source = message.get("source")
        if not source:
            logger.warning(
                f"Message is missing 'source' field. Cannot route. Message: {message}"
            )
            return None

        strategy = self._strategies.get(source)
        if strategy:
            return strategy.get_pulsar_topic(message)

        logger.warning(
            f"No routing strategy registered for source '{source}'. Ignoring message."
        )
        return None
