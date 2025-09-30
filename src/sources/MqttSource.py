import socket
import threading
from multiprocessing import Queue
from multiprocessing.synchronize import Event
import paho.mqtt.client as mqtt
from loguru import logger

from .interfaces import ISource
from ..core.message import Message
from ..core.heartbeat import HeartbeatMixin


class MqttSource(ISource, HeartbeatMixin):
    """
    Message source for collecting data from an MQTT broker by subscribing to topics.
    """

    def __init__(self, config: dict):
        self.config = config
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, client_id=self.config["client_id"]
        )
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self._message_queue: Queue | None = None

        self.heartbeat_interval = config.get("heartbeat", {}).get("interval_seconds")
        self._stop_event = threading.Event()

    def run(self, message_queue: Queue, stop_event: Event) -> None:
        """Main process loop for the MQTT source."""
        self._message_queue = message_queue
        self.client.on_message = self._internal_on_message

        if not self._connect():
            logger.critical("MQTT source could not connect. Process will exit.")
            return

        logger.info("MQTT source is running.")
        stop_event.wait()

        self._stop()
        logger.info("MQTT source has stopped.")

    def _connect(self) -> bool:
        try:
            logger.info(
                f"Attempting to connect to MQTT broker at {self.config['broker_host']}..."
            )
            self.client.connect(
                self.config["broker_host"],
                self.config["broker_port"],
                self.config["keepalive"],
            )
            self.client.loop_start()

            threading.Timer(
                2.0, lambda: self._start_heartbeat(self.heartbeat_interval)
            ).start()
            self._stop_event.clear()

            return True
        except (socket.gaierror, ConnectionRefusedError, TimeoutError) as e:
            logger.critical(
                f"MQTT connection failed: Could not reach broker at {self.config['broker_host']}:{self.config['broker_port']}. "
                f"Error: {e}. Check configuration or broker status."
            )
            return False
        except Exception:
            logger.exception(
                "An unexpected, non-connection error occurred during MQTT setup"
            )
            return False

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.success(
                f"Connected to MQTT broker: {self.config['broker_host']}:{self.config['broker_port']}"
            )
            client.subscribe(self.config["topic_subscribe"])
            logger.info(f"Subscribed to topic: {self.config['topic_subscribe']}")
        else:
            logger.warning(
                f"MQTT connection failed with code: {reason_code}. The client will try again automatically."
            )

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info("MQTT client disconnected successfully.")
        else:
            logger.warning(f"Unexpected MQTT disconnection. Reason code: {reason_code}")

    def _internal_on_message(self, client, userdata, msg):
        """Internal callback that works as an adapter and translator between Source and Publisher."""
        if self._on_message_callback:
            standardized_message = Message(
                source_id=self.config["id"],
                topic=msg.topic,
                payload=msg.payload,
            )
            self._message_queue.put(standardized_message)

    def _stop(self):
        self._stop_event.set()
        self._stop_heartbeat()
        try:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("MQTT: Stopped.")
        except Exception as e:
            logger.warning(f"Exception during MQTT source disconnetion: {e}")

    @property
    def _is_healthy(self) -> bool:
        return self.client.is_connected()

    def _perform_reconnect(self) -> bool:
        logger.info("MQTT: Heartbeat failed, attempting to perform reconnection...")
        try:
            self.client.reconnect()
            return self.client.is_connected()
        except Exception:
            # If paho's reconnect() fails we raise an exception to handle it to tenacity
            raise ConnectionError("MQTT reconnect attempt failed")
