import asyncio
from multiprocessing import Queue
from multiprocessing.synchronize import Event
from loguru import logger
from asyncua import Client, Node, ua
from asyncua.common.subscription import DataChangeNotif

from ..core.message import Message
from ..core.heartbeat import HeartbeatMixin
from .interfaces import ISource


class OpcUaSource(ISource, HeartbeatMixin):
    """
    Message source for collecting data from an OPC UA server by subscribing to node changes.
    """

    def __init__(self, config: dict):
        self.config = config
        self.server_url = config["server_url"]
        self.nodes_to_subscribe = config.get("nodes_to_subscribe", [])
        self.publishing_interval = config.get("publishing_interval_ms", 500)
        self._message_queue: Queue | None = None

        self.heartbeat_interval = config.get("heartbeat", {}).get(
            "interval_seconds", 30
        )

    def run(self, message_queue: Queue, stop_event: Event) -> None:
        """The entry point for the dedicated process."""
        if not self.nodes_to_subscribe:
            logger.warning(
                "OPC-UA: No nodes configured for subscription. Process will not start."
            )
            return

        self._message_queue = message_queue
        try:
            asyncio.run(self._main_async_task(stop_event))
        except KeyboardInterrupt:
            logger.info("OPC-UA: Process interrupted.")
        except Exception:
            logger.exception(
                "OPC-UA: An unhandled exception occurred in the async worker."
            )
        finally:
            logger.info("OPC-UA source has stopped.")

    async def _main_async_task(self, stop_event: Event):
        """The main async method that orchestrates the client's lifecycle."""
        client = Client(url=self.server_url)
        try:
            async with client:
                logger.success(f"OPC-UA: Successfully connected to {self.server_url}.")
                subscription = await client.create_subscription(
                    self.publishing_interval, self
                )
                nodes = [
                    client.get_node(node_id) for node_id in self.nodes_to_subscribe
                ]
                await subscription.subscribe_data_change(nodes)
                logger.success(
                    f"OPC-UA: Subscription successful for {len(nodes)} nodes."
                )

                logger.info("OPC-UA: Listening for data changes...")
                while not stop_event.is_set():
                    await asyncio.sleep(1)
        except (OSError, asyncio.TimeoutError) as e:
            logger.critical(f"OPC-UA: Failed to connect to server. Error: {e}")
        except ua.UaError as e:
            logger.error(
                f"OPC-UA: Failed to subscribe to nodes. Error: {e}. Check Node IDs."
            )
        except Exception:
            logger.exception("OPC-UA: A critical error occurred in the client task.")
        finally:
            logger.warning("OPC-UA: Client is disconnecting.")

    # --- Data Callback ---

    def datachange_notification(self, node: Node, val: any, data: DataChangeNotif):
        """Callback to handle data change and put message on the queue."""
        if not self._message_queue:
            return

        try:
            data_value = data.monitored_item.Value
            standardized_message = Message(
                source_id=self.config["id"],
                topic=node.nodeid.to_string(),
                payload=str(val).encode("utf-8"),
                timestamp=data_value.ServerTimestamp,
                metadata={"quality": data_value.StatusCode.name},
            )
            self._message_queue.put(standardized_message)
        except Exception:
            node_id_str = node.nodeid.to_string() if node else "Unknown"
            logger.exception(
                f"Error processing OPC-UA data change notification for node {node_id_str}."
            )

    # --- Heartbeat ---

    @property
    def _is_healthy(self) -> bool:
        if (
            not self.client
            or not self._loop
            or not self._loop.is_running()
            or not self._thread
            or not self._thread.is_alive()
        ):
            return False
        try:
            health_check_node = self.client.get_node("i=2256")

            future = asyncio.run_coroutine_threadsafe(
                health_check_node.read_value(), self._loop
            )

            future.result(timeout=5)
            return True
        except Exception:
            return False

    def _perform_reconnect(self) -> bool:
        logger.info("OPC-UA: Heartbeat failed, attempting to perform reconnection...")
        self.stop()
        return self.connect()
