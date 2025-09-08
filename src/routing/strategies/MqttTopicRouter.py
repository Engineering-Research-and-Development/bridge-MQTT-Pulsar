from loguru import logger
from ..interfaces import TopicRouter


class MqttTopicRouter(TopicRouter):
    """
    Routing strategy for messages originating from MQTT.
    It extracts a device name from the MQTT topic based on a configured prefix and index.
    """

    def __init__(self, config: dict):
        self.tenant = config.get("pulsar_tenant", "public")
        self.namespace = config.get("pulsar_namespace", "default")
        self.match_prefix_parts = config.get("match_prefix", "").split("/")
        self.device_index = config.get("device_index", 2)

        if not self.match_prefix_parts or self.match_prefix_parts == [""]:
            raise ValueError("MQTT routing config 'match_prefix' cannot be empty.")

    def get_pulsar_topic(self, message: dict) -> str | None:
        # eg: if mqtt_topic "test/data/camera", extract "camera".
        mqtt_topic = message.get("topic")
        if not mqtt_topic:
            logger.warning(
                f"MQTT message is missing 'topic' field. Ignoring. Message: {message}"
            )
            return None

        parts = mqtt_topic.split("/")
        prefix_len = len(self.match_prefix_parts)

        if (
            len(parts) > self.device_index
            and parts[:prefix_len] == self.match_prefix_parts
        ):
            device_name = parts[self.device_index]
            return f"persistent://{self.tenant}/{self.namespace}/{device_name}"

        logger.warning(
            f"No MQTT routing rule found for topic '{mqtt_topic}'. Ignoring message."
        )
        return None
