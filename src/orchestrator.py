import threading
from loguru import logger
from concurrent.futures import ThreadPoolExecutor
from .destinations.interfaces import IDestination
from .sources.interfaces import ISource


class Orchestrator:
    def __init__(self, pipelines: dict[ISource, IDestination]):
        self._pipelines = pipelines
        self._sources = list(pipelines.keys())
        self._destinations = list(set(pipelines.values()))

        self._shutdown_event = threading.Event()

        self._executor = ThreadPoolExecutor(max_workers=10)

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

    def _handle_message(self, source: ISource, message: dict):
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

    def _forward_message(self, destination: IDestination, message: dict):
        """
        This method prepares the message and calls the destination's publish method.
        """
        try:
            # TODO: encapsulate routing logic
            source_topic = message.get("topic")
            # source_type = message.get("source")

            device_name = "".join(
                c if c.isalnum() or c in "-_" else "-" for c in source_topic
            )

            # eg: persistent://public/default/test-data
            destination_topic = f"persistent://public/default/{device_name}"

            message_to_publish = message.copy()
            message_to_publish["destination_topic"] = destination_topic

            destination.publish(message_to_publish)
        except Exception:
            logger.exception(
                f"An unhandled error occurred in a forwarding worker thread for destination {destination.__class__.__name__}."
            )

    def stop(self):
        logger.info("Shutting down the bridge...")

        logger.debug("Shutting down the forwarding thread pool...")
        self._executor.shutdown(wait=True)

        for component in self._sources + self._destinations:
            component.stop()

        logger.success("Bridge shut down successfully.")
