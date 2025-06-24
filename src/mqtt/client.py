import socket
import asyncio
import paho.mqtt.client as mqtt
from loguru import logger
from .interfaces import MessageSource

class MqttClientManager(MessageSource):
    def __init__(self, config: dict):
        self.config = config
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.config['client_id'])
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.on_message_callback = None
    
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.success(f"Connected to MQTT broker: {self.config['broker_host']}:{self.config['broker_port']}")
            client.subscribe(self.config['topic_subscribe'])
            logger.info(f"Subscribed to topic: {self.config['topic_subscribe']}")
        else:
            logger.warning(f"MQTT connection failed with code: {reason_code}. The client will try again automatically.")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info("MQTT client disconnected successfully.")
        else:
            logger.warning(f"Unexpected MQTT disconnection. Reason code: {reason_code}")  

    async def connect(self):
        if not self.on_message_callback:
            logger.error("on_message_callback is not set. Messages can't be processed.")
            return False
        try:
            self.client.on_message = self.on_message_callback
            logger.info(f"Attempting to connect to MQTT broker at {self.config['broker_host']}...")
            self.client.connect(
                self.config['broker_host'],
                self.config['broker_port'],
                self.config['keepalive']
            )
            return True
        except (socket.gaierror, ConnectionRefusedError, TimeoutError) as e:
            logger.critical(
                f"MQTT connection failed: Could not reach broker at {self.config['broker_host']}:{self.config['broker_port']}. "
                f"Error: {e}. Check configuration or broker status."
            )
            return False 
        except Exception:
            logger.exception("An unexpected, non-connection error occurred during MQTT setup")
            return False

    async def start_listening(self):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.client.loop_forever)

    async def stop(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("MQTT client closed.")
        except Exception as e:
            logger.warning(f"Exception during MQTT client disconnetion: {e}")