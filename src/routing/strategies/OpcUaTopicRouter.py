from loguru import logger
import re
from ..interfaces import TopicRouter


class OpcUaTopicRouter(TopicRouter):
    """
    Routing strategy for messages originating from OPC UA.
    It normalizes the OPC UA Node ID to use it as a device name in the Pulsar topic.
    """

    def __init__(self, config: dict):
        self.tenant = config.get("pulsar_tenant", "public")
        self.namespace = config.get("pulsar_namespace", "default")
        # Replace chars that are invalid in Pulsar topic names
        self._invalid_chars_re = re.compile(r"[^a-zA-Z0-9_-]")

    def get_pulsar_topic(self, message: dict) -> str | None:
        node_id = message.get("topic")
        if not node_id:
            logger.warning(
                f"OPC UA message is missing 'topic' (Node ID) field. Ignoring. Message: {message}"
            )
            return None

        # Normalize the Node ID to be a valid Pulsar topic component.
        # eg: "ns=3;i=1001" becomes "ns-3-i-1001"
        device_name = self._invalid_chars_re.sub("-", node_id)

        return f"persistent://{self.tenant}/{self.namespace}/{device_name}"
