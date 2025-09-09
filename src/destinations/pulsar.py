import pulsar
from loguru import logger
from tenacity import Retrying, stop_after_attempt, wait_exponential, RetryError
from .interfaces import IDestination


class PulsarDestination(IDestination):
    def __init__(self, config: dict):
        self.config = config
        self.client: pulsar.Client | None = None
        self.producers: dict[str, pulsar.Producer] = {}

        publishing_config = self.config.get("publishing", {})
        self.retrier = Retrying(
            stop=stop_after_attempt(publishing_config.get("retry_attempts", 5)),
            wait=wait_exponential(multiplier=1, min=2, max=10),
        )
        self.dlq_topic = publishing_config.get("dlq_topic")
        self.dlq_producer: pulsar.Producer | None = None

    def connect(self) -> bool:
        try:
            self.client = pulsar.Client(self.config["service_url"])
            # If the broker is not available, this call will timeout and raise an exception.
            self.client.get_topic_partitions(
                "persistent://public/default/non-existent-topic-for-health-check"
            )

            if self.dlq_topic:
                self.dlq_producer = self.client.create_producer(self.dlq_topic)
                logger.info(
                    f"Successfully created DLQ producer for topic: {self.dlq_topic}"
                )

            logger.success(
                f"Successfully connected to Pulsar service at {self.config['service_url']}"
            )
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

    def _create_producer(self, topic: str) -> pulsar.Producer:
        logger.debug(f"Attempting to create producer for Pulsar topic '{topic}'...")
        try:
            producer = self.client.create_producer(topic)
            logger.info(f"Created new Pulsar producer for topic: {topic}")
            return producer
        except Exception as e:
            logger.warning(
                f"Failed attempt to create producer for Pulsar topic '{topic}'. "
                f"Error: {e.__class__.__name__}. Retrying...."
            )
            raise

    def _get_producer(self, topic: str) -> pulsar.Producer | None:
        if topic in self.producers:
            return self.producers[topic]

        if not self.client:
            logger.error("Pulsar client is not connected. Cannot create new producer.")
            return None

        try:
            producer = self.retrier(self._create_producer, topic)
            self.producers[topic] = producer
            return producer
        except RetryError as e:
            logger.critical(
                f"Could not create a producer for topic '{topic}' after max attempts. "
                f"Final error: {e}. This may indicate a persistent Pulsar issue."
            )
            return None

    def _send_message(self, producer: pulsar.Producer, payload: bytes):
        try:
            logger.debug(
                f"Attempting to send message to Pulsar topic '{producer.topic()}'..."
            )
            producer.send(payload)
            logger.success(
                f"Message successfully sent to Pulsar topic '{producer.topic()}'."
            )
        except Exception as e:
            logger.warning(
                f"Failed to send message to Pulsar topic '{producer.topic()}'. "
                f"Error: {e.__class__.__name__}. Retrying..."
            )
            raise

    def _send_to_dlq(self, msg: dict):
        if not self.dlq_producer:
            logger.error(
                f"DLQ producer is not available. Message from topic '{msg.get('source')}' will be lost."
            )
            return

        try:
            properties = {
                "original_source": msg.get("source"),
                "original_topic": msg.get("topic", "N/A"),
                "failure_reason": "Max retries exceeded or producer creation failed",
            }
            payload = msg.get("payload", b"")
            self.dlq_producer.send(payload, properties=properties)
            logger.warning(
                f"Message from topic '{msg.get('topic')}' successfully sent to DLQ '{self.dlq_topic}'."
            )
        except Exception:
            logger.critical(
                f"CRITICAL: Failed to send message from topic '{msg.get('topic')}' to DLQ '{self.dlq_topic}'. DATA LOSS OCCURRED."
            )

    def publish(self, message: dict):
        destination_topic = message.get("destination_topic")
        payload = message.get("payload")
        source_topic = message.get("topic")

        if not destination_topic or payload is None:
            logger.warning(
                f"Message is missing 'destination_topic' or 'payload'. Cannot publish. Message: {message}"
            )
            return

        producer = self._get_producer(destination_topic)

        if not producer:
            logger.error(
                f"Could not get a producer for topic '{destination_topic}'. Forwarding to Dead Letter Queue."
            )
            self._send_to_dlq(message)
            return

        try:
            logger.info(
                f"Forwarding message from '{message.get('source')}' topic '{source_topic}' to Pulsar topic '{destination_topic}'"
            )
            self.retrier(self._send_message, producer, payload)
        except RetryError as e:
            logger.critical(
                f"Message from topic '{source_topic}' could not be delivered to Pulsar after all attempts. "
                f"Final error: {e}. Forwarding to Dead Letter Queue."
            )
            self._send_to_dlq(message)
        except Exception:
            logger.exception(
                f"An unexpected, non-retryable error occurred during message publishing for topic '{source_topic}'"
            )

    def stop(self):
        logger.info("Closing all Pulsar producers...")
        logger.debug(f"Closing {len(self.producers)} Pulsar producers.")
        for topic, producer in self.producers.items():
            try:
                producer.close()
            except Exception:
                logger.warning(f"Error closing producer for topic {topic}")

        if self.dlq_producer:
            try:
                self.dlq_producer.close()
            except Exception:
                logger.warning(f"Error closing DLQ producer for topic {self.dlq_topic}")

        if self.client:
            try:
                self.client.close()
                logger.info("Pulsar client closed.")
            except Exception as e:
                logger.warning(f"Exception during Pulsar client closing: {e}")
