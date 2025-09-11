import socket
import threading
import paho.mqtt.client as mqtt
from loguru import logger

from ..core.message import Message
from .interfaces import ISource, MessageCallback


class MqttSource(ISource):
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
        self._on_message_callback: MessageCallback | None = None
        self._thread: threading.Thread | None = None

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
                source_id="mqtt",
                topic=msg.topic,
                payload=msg.payload,
            )
            self._on_message_callback(self, standardized_message)

    def connect(self) -> bool:
        try:
            self.client.on_message = self._internal_on_message
            logger.info(
                f"Attempting to connect to MQTT broker at {self.config['broker_host']}..."
            )
            self.client.connect(
                self.config["broker_host"],
                self.config["broker_port"],
                self.config["keepalive"],
            )
            self.client.loop_start()
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

    def start(self, on_message_callback: MessageCallback):
        if not callable(on_message_callback):
            raise TypeError("on_message_callback must be a callable function")
        logger.info("MQTT source is starting and setting up message callback.")
        self._on_message_callback = on_message_callback

    def stop(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("MQTT source closed.")
        except Exception as e:
            logger.warning(f"Exception during MQTT source disconnetion: {e}")
