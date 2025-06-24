from loguru import logger
from .interfaces import TopicRouter


class DeviceTopicRouter(TopicRouter):
    def __init__(self, config: dict):
        # eg: persistent://<tenant>/<namespace>/<device_name>
        self.tenant = config.get("pulsar_tenant", "public")
        self.namespace = config.get("pulsar_namespace", "default")
        self.match_prefix_parts = config.get("match_prefix", "").split("/")
        self.device_index = config.get("device_index", 2)

        if not self.match_prefix_parts or self.match_prefix_parts == [""]:
            raise ValueError("Routing config 'match_prefix' cannot be empty.")

    def get_pulsar_topic(self, mqtt_topic: str) -> str | None:
        parts = mqtt_topic.split("/")
        # eg: if mqtt_topic "test/data/camera", extract "camera".
        prefix_len = len(self.match_prefix_parts)
        if (
            len(parts) > self.device_index
            and parts[:prefix_len] == self.match_prefix_parts
        ):
            device_name = parts[self.device_index]
            return f"persistent://{self.tenant}/{self.namespace}/{device_name}"

        logger.warning(
            f"No routing rule found for MQTT topic '{mqtt_topic}', ignoring message."
        )
        return None
