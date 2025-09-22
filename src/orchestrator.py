import threading
import re
from loguru import logger
from concurrent.futures import ThreadPoolExecutor
from .destinations.interfaces import IDestination
from .sources.interfaces import ISource
from .core.message import Message


class Orchestrator:
    def __init__(self, pipelines: dict[ISource, IDestination]):
        self._pipelines = pipelines
        self._sources = list(pipelines.keys())
        self._destinations = list(set(pipelines.values()))
        self._shutdown_event = threading.Event()

        self._executor = ThreadPoolExecutor(max_workers=10)

        self._invalid_chars_re = re.compile(r"[^a-zA-Z0-9_-]")

    def run(self):
        logger.info("Starting the Bridge...")

        for component in self._destinations + self._sources:
            if not component.connect():
                logger.critical(
                    f"Critical error connecting {component.__class__.__name__}. Exiting."
                )
                return

        for source in self._sources:
            source.start(self._handle_message)

        logger.success("Bridge is running.")

        try:
            self._shutdown_event.wait()
        except KeyboardInterrupt:
            logger.info("Keyboard interruption detected. Shutting down...")
        finally:
            self.stop()

    def _handle_message(self, source: ISource, message: Message):
        """
        Receives a message from a source and dispatches it to the correct destination.
        The forwarding logic is submitted to a thread pool to avoid blocking the source.
        """
        destination = self._pipelines.get(source)
        if not destination:
            logger.error(
                f"Configuration error: No destination found for source {source.__class__.__name__}. Message dropped."
            )
            return

        self._executor.submit(self._forward_message, destination, message)

    def _determine_destination_topic(self, message: Message):
        """Returns the corrected destination topic"""
        normalized_topic = self._invalid_chars_re.sub("-", message.topic)
        # eg: persistent://public/default/test-data
        return f"persistent://public/default/{normalized_topic}"

    def _forward_message(self, destination: IDestination, message: Message):
        """Calls the destination's publish method."""
        try:
            destination_topic = self._determine_destination_topic(message)

            destination.publish(message, destination_topic)
        except Exception:
            logger.exception(
                f"An unhandled error occurred in a forwarding worker thread for destination {destination.__class__.__name__}."
            )

    def stop(self):
        logger.info("Shutting down the bridge...")

        logger.debug(
            "Shutting down the forwarding thread pool (waiting for tasks to complete)..."
        )
        self._executor.shutdown(wait=True)

        for component in self._sources + self._destinations:
            component.stop()

        logger.success("Bridge shut down successfully.")
