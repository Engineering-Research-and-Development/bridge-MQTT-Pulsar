from multiprocessing import Queue
from multiprocessing.synchronize import Event
from loguru import logger

from .interfaces import ISource
from ..core.message import Message
from ..core.heartbeat import HeartbeatMixin
from ..core.sourceconnection import MqttSourceConnection


class MqttSource(ISource, HeartbeatMixin):
    """
    Message source for collecting data from an MQTT broker by subscribing to topics.
    """

    def __init__(self, config: dict):
        self.config = config
        self._message_queue: Queue | None = None
        self._connector = MqttSourceConnection(config)

    def run(self, message_queue: Queue, stop_event: Event) -> None:
        """Main process loop for the MQTT source."""
        self._message_queue = message_queue

        client = self._connector.connect()

        if not client:
            logger.critical("MQTT source could not connect. Process will exit.")
            return

        self.client.on_message = self._internal_on_message

        logger.info("MQTT source is running.")
        stop_event.wait()

        self._connector.disconnect()
        logger.info("MQTT source has stopped.")

    def _internal_on_message(self, client, userdata, msg):
        """Internal callback that works as an adapter and translator between Source and Publisher."""
        if self._on_message_callback:
            standardized_message = Message(
                source_id=self.config["id"],
                topic=msg.topic,
                payload=msg.payload,
            )
            self._message_queue.put(standardized_message)
