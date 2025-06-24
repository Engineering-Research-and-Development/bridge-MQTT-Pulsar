from loguru import logger
from .pulsar.interfaces import Publisher
from .mqtt.client import MqttClientManager


class MqttPulsarOrchestrator:
    def __init__(self, mqtt_manager: MqttClientManager, publisher: Publisher):
        self.mqtt_manager = mqtt_manager
        self.publisher = publisher

    def run(self):
        logger.info("Starting MQTT -> Pulsar bridge...")

        if not self.publisher.connect():
            logger.critical("Critical Pulsar initialization error. Exiting.")
            return

        self.mqtt_manager.on_message_callback = self.publisher.publish

        if not self.mqtt_manager.connect():
            logger.critical(
                "Couldn't connect to the MQTT Broker on startup. Check the address and restart."
            )
            self.stop()
            return

        try:
            self.mqtt_manager.start_listening()
        except KeyboardInterrupt:
            logger.info("Keyboard interruption detected. Shutting down...")
        finally:
            self.stop()

    def stop(self):
        logger.info("Shutting down the bridge...")
        self.mqtt_manager.stop()
        self.publisher.stop()
        logger.success("Bridge shut down successfully")
