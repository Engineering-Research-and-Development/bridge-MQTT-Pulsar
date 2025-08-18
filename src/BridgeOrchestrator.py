from loguru import logger
from .pulsar.interfaces import Publisher
from .sources.interfaces import IMessageSource


class BridgeOrchestrator:
    def __init__(self, sources: list[IMessageSource], publisher: Publisher):
        self.sources = sources
        self.publisher = publisher

    def _handle_message(self, message: dict):
        """Callback that gets the standardized message from the source"""
        # TODO: add router logic
        self.publisher.publish(message)

    def run(self):
        logger.info("Starting the Bridge...")

        if not self.publisher.connect():
            logger.critical("Critical Pulsar initialization error. Exiting.")
            return

        for source in self.sources:
            if source.connect():
                source.start(self._handle_message)
            else:
                logger.error(
                    f"Failed to connect to source: {source.__class__.__name__}. It will be ignored."
                )

        logger.success("Bridge is running.")

        try:
            # TODO: change this logic
            while True:
                pass
        except KeyboardInterrupt:
            logger.info("Keyboard interruption detected. Shutting down...")
        finally:
            self.stop()

    def stop(self):
        logger.info("Shutting down the bridge...")
        for source in self.sources:
            source.stop()
        self.publisher.stop()
        logger.success("Bridge shut down successfully.")
