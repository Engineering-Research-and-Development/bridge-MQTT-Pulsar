import paho.mqtt.client as mqtt
from loguru import logger

class MqttClientManager:
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

    def connect(self):
        try:
            if self.on_message_callback:
                self.client.on_message = self.on_message_callback
            else:
                logger.error("MQTT on_message_callback is not set. Messages can't be processed.")
                return False
            self.client.connect(self.config['broker_host'], self.config['broker_port'], self.config['keepalive'])
            return True
        except (OSError, Exception):
            logger.exception("Failed to initiate connection to MQTT broker.")
            return False

    def start_listening(self):
        self.client.loop_forever()

    def stop(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("MQTT client closed.")
        except Exception as e:
            logger.warning(f"Exception during MQTT client disconnetion: {e}")