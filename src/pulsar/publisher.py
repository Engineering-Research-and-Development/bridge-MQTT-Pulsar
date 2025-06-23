import pulsar
from loguru import logger
from .interfaces import Publisher
from ..routing.interfaces import TopicRouter

class PulsarPublisher(Publisher):
    def __init__(self, config: dict, router: TopicRouter):
        self.config = config
        self.client: pulsar.Client | None = None
        self.producers: dict[str, pulsar.Producer] = {}
        self.router = router

    def connect(self) -> bool:
        try:
            self.client = pulsar.Client(self.config['service_url'])
            # If the broker is not available, this call will timeout and raise an exception.
            self.client.get_topic_partitions('persistent://public/default/non-existent-topic-for-health-check')
            
            logger.success(f"Successfully connected to Pulsar service at {self.config['service_url']}")
            return True
        except (pulsar.ConnectError, pulsar.Timeout) as e:
            logger.critical(
                f"Pulsar connection failed: Could not reach broker at {self.config['service_url']}. "
                f"Error: {e.__class__.__name__}. Check configuration or broker status."
            )
            if self.client:
                self.client.close()
            return False
        except Exception:
            logger.exception("An unexpected error occurred while connecting to Pulsar")
            if self.client:
                self.client.close()
            return False
    
    def _get_producer(self, topic: str) -> pulsar.Producer | None:
        if topic in self.producers:
            return self.producers[topic]
        
        if not self.client:
            logger.error("Pulsar client is not connected. Cannot create new producer.")
            return None
        
        try:
            producer = self.client.create_producer(topic)
            self.producers[topic] = producer
            logger.info(f"Created new Pulsar producer for topic: {topic}")
            return producer
        except Exception:
            logger.exception(f"Failed to create producer for topic: {topic}")
            return None

    def publish(self, client, userdata, msg):
        pulsar_topic = self.router.get_pulsar_topic(msg.topic)

        if not pulsar_topic:
            return
        
        producer = self._get_producer(pulsar_topic)

        if not producer:
            logger.error(f"Could not get a producer for topic '{pulsar_topic}'. Message dropped.")
            return

        try:
            payload_str = msg.payload.decode('utf-8', errors='replace')
            logger.debug(f"MQTT < Topic: {msg.topic} | Forwarding {payload_str} to Pulsar > Topic: {pulsar_topic}")
            logger.info(f"Forwarding message from MQTT topic '{msg.topic}' to Pulsar topic '{pulsar_topic}'")
            producer.send(msg.payload)
        except Exception as e:
            logger.exception(f"Error while sending message to Pulsar: {e}")

    def stop(self):
        logger.info("Closing all Pulsar producers...")
        logger.debug(f"Closing {len(self.producers)} Pulsar producers.")
        for topic, producer in self.producers.items():
            try:
                producer.close()
            except Exception:
                logger.warning(f"Error closing producer for topic {topic}")
        if self.client:
            try:
                self.client.close()
                logger.info("Pulsar client closed.")
            except Exception as e:
                logger.warning(f"Exception during Pulsar client closing: {e}")