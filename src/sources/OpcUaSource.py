import asyncio
import threading
from loguru import logger
from asyncua import Client, Node, ua
from asyncua.common.subscription import Subscription, DataChangeNotif
from typing import Optional

from ..core.message import Message
from .interfaces import ISource, MessageCallback


class OpcUaSource(ISource):
    """
    Message source for collecting data from an OPC UA server by subscribing to node changes.
    """

    def __init__(self, config: dict):
        self.server_url = config["server_url"]
        self.nodes_to_subscribe = config.get("nodes_to_subscribe", [])
        self.publishing_interval = config.get("publishing_interval_ms", 500)

        self.client: Optional[Client] = None
        self._on_message_callback: Optional[MessageCallback] = None
        self._subscription: Optional[Subscription] = None

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._connection_status = threading.Event()

    def connect(self) -> bool:
        """
        Starts the async connection process in a separated thread and waits for the result.
        Returns True if the connection is successfull, False otherwise.
        """
        if not self.nodes_to_subscribe:
            logger.warning(
                "OPC-UA service is enabled, but no nodes are configured for subscription. Service will not start."
            )
            return False

        if self._thread is not None:
            logger.warning("OPC-UA connect called more than once. Ignoring.")
            return self._connection_status.is_set()

        logger.info("Starting OPC-UA service thread...")
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_worker, daemon=True)
        self._thread.start()

        if not self._connection_status.wait(timeout=10):
            logger.critical(f"OPC-UA: Connection to {self.server_url} timed out.")
            self.stop()
            return False

        logger.success(f"OPC-UA: Successfully connected to {self.server_url}.")
        return True

    def start(self, on_message_callback: MessageCallback):
        if not self._thread or not self._thread.is_alive():
            logger.error("OPC-UA: Cannot start source as it is not connected.")
            return
        if not callable(on_message_callback):
            raise TypeError("on_message_callback must be a callable function.")

        logger.info("OPC-UA: Setting message callback and enabling subscriptions.")
        self._on_message_callback = on_message_callback

    def stop(self):
        if not self._thread:
            return
        logger.info("OPC-UA: Stopping source...")
        self._stop_event.set()
        self._thread.join(timeout=10)
        if self._thread.is_alive():
            logger.warning("OPC-UA: Worker thread did not terminate gracefully.")
        self._thread = None
        logger.info("OPC-UA: Source stopped.")

    # --- Threading and Async Bridging ---

    def _run_worker(self):
        """The entry point for the dedicated worker thread."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._main_async_task())
        except Exception:
            logger.exception(
                "OPC-UA: An unhandled exception occurred in the async worker."
            )
        finally:
            if self._loop.is_running():
                self._loop.close()
            logger.info("OPC-UA: Async loop has been closed.")

    async def _main_async_task(self):
        """The main async method that orchestrates the client's lifecycle."""
        self.client = Client(url=self.server_url)
        try:
            async with self.client:
                # 1. Report successful connection
                self._connection_status.set()

                # 2. Wait for the start() method to provide the callback
                await self._wait_for_callback()

                # 3. If we have not been stopped, set up the subscription
                if not self._stop_event.is_set():
                    await self._setup_subscription()

                # 4. Keep alive until stop request
                await self._keep_alive()
        except (OSError, asyncio.TimeoutError) as e:
            logger.critical(f"OPC-UA: Failed to connect to server. Error: {e}")
        except Exception:
            logger.exception("OPC-UA: A critical error occurred in the client task.")
        finally:
            # Make sure connect() unblocks even if it fails
            self._connection_status.set()
            logger.warning("OPC-UA: Client is disconnecting.")

    # --- Async Helper Methods ---

    async def _wait_for_callback(self):
        """Waits until the on_message_callback is provided by the start() method."""
        logger.debug("OPC-UA: Async worker is waiting for the message callback...")
        while not self._on_message_callback and not self._stop_event.is_set():
            await asyncio.sleep(0.1)
        if self._on_message_callback:
            logger.debug("OPC-UA: Message callback received.")

    async def _setup_subscription(self):
        """Creates the subscription and subscribes to the configured nodes."""
        try:
            self._subscription = await self.client.create_subscription(
                self.publishing_interval, self
            )
            nodes = [
                self.client.get_node(node_id) for node_id in self.nodes_to_subscribe
            ]
            await self._subscription.subscribe_data_change(nodes)
            logger.success(f"OPC-UA: Subscription successful for {len(nodes)} nodes.")
        except ua.UaError as e:
            logger.error(
                f"OPC-UA: Failed to subscribe to nodes. Error: {e}. Check Node IDs."
            )
        except Exception:
            logger.exception(
                "OPC-UA: An unexpected error occurred during subscription setup."
            )

    async def _keep_alive(self):
        """Keeps the async task alive, periodically checking for the stop signal."""
        logger.info("OPC-UA: Listening for data changes...")
        while not self._stop_event.is_set():
            await asyncio.sleep(1)

    # --- Data Callback ---

    def datachange_notification(self, node: Node, val: any, data: DataChangeNotif):
        """
        Callback method to handle a single data change notification.
        """
        if not self._on_message_callback:
            return

        try:
            data_value = data.monitored_item.Value

            standardized_message = Message(
                source_id="opcua",
                topic=node.nodeid.to_string(),
                payload=str(val).encode("utf-8"),
                timestamp=data_value.ServerTimestamp,
                metadata={"quality": data_value.StatusCode.name},
            )
            self._on_message_callback(self, standardized_message)

        except Exception:
            node_id_str = node.nodeid.to_string() if node else "Unknown"
            logger.exception(
                f"Error processing OPC-UA data change notification for node {node_id_str}."
            )
