import asyncio
import threading
from loguru import logger
from asyncua import Client, Node, ua
from typing import Optional

from .interfaces import IMessageSource, MessageCallback


class OpcUaSource(IMessageSource):
    """
    Message source for collecting data from an OPC UA server by subscribing to node changes.
    """

    def __init__(self, config: dict):
        self.config = config
        self.server_url = config["server_url"]
        self.nodes_to_subscribe = config.get("nodes_to_subscribe", [])
        self.client: Optional[Client] = None
        self._on_message_callback: Optional[MessageCallback] = None

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
        self._thread = threading.Thread(
            target=self._run_async_loop,
            daemon=True,
        )
        self._thread.start()

        connected = self._connection_status.wait(timeout=10)
        if not connected:
            logger.critical(f"OPC-UA connection to {self.server_url} timed out.")
            self.stop()
            return False

        logger.success(f"OPC-UA service connected to {self.server_url}.")
        return True

    def start(self, on_message_callback: MessageCallback):
        if not self._thread or not self._thread.is_alive():
            logger.error("OPC-UA source cannot be started as it is not connected.")
            return

        if not callable(on_message_callback):
            raise TypeError("on_message_callback must be a callable function.")

        logger.info("Setting up OPC-UA message callback and subscriptions.")
        self._on_message_callback = on_message_callback

    def stop(self):
        if not self._thread:
            return
        logger.info("Stopping OPC-UA source...")
        self._stop_event.set()

        if self._loop and self._loop.is_running() and self.client:
            future = asyncio.run_coroutine_threadsafe(
                self.client.disconnect(), self._loop
            )
            try:
                future.result(timeout=5)
            except Exception:
                logger.warning("Timeout or error during OPC-UA client disconnection.")

        self._thread.join(timeout=10)
        if self._thread.is_alive():
            logger.warning("OPC-UA thread did not terminate gracefully.")

        self._thread = None
        logger.info("OPC-UA source stopped.")

    def _run_async_loop(self):
        """Entry point for the thread to start managing the asyncio loop."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        try:
            self._loop.run_until_complete(self._manage_connection_and_subscription())
        except Exception:
            logger.exception(
                "An unhandled exception occurred in the OPC-UA async loop."
            )
        finally:
            if self._loop.is_running():
                self._loop.close()
            logger.info("OPC-UA async loop has been closed.")

    async def _manage_connection_and_subscription(self):
        """
        Core async method to handle connection, subscription, and listening.
        """
        self.client = Client(url=self.server_url)
        try:
            async with self.client:
                # Signal that the connection was successful
                self._connection_status.set()

                # Wait until the start() method provides the callback
                while not self._on_message_callback and not self._stop_event.is_set():
                    await asyncio.sleep(0.1)

                if self._on_message_callback:
                    await self._setup_subscription()

                # Keep the connection alive until a stop is requested
                while not self._stop_event.is_set():
                    await asyncio.sleep(1)

        except (OSError, asyncio.TimeoutError) as e:
            logger.critical(
                f"Failed to connect to OPC-UA server at {self.server_url}. Error: {e}"
            )
            self._connection_status.set()  # Unblock the connect() method to report failure
        except Exception:
            logger.exception("A critical error occurred in the OPC-UA client.")
            self._connection_status.set()
        finally:
            logger.warning("OPC-UA client is disconnecting.")

    async def _setup_subscription(self):
        try:
            handler = self.SubscriptionHandler(self._on_message_callback)
            subscription = await self.client.create_subscription(500, handler)
            nodes = [
                self.client.get_node(node_id) for node_id in self.nodes_to_subscribe
            ]
            await subscription.subscribe_data_change(nodes)
            logger.success(
                f"OPC-UA subscription successful for {len(nodes)} nodes. Listening for data changes..."
            )
        except ua.UaError as e:
            logger.error(
                f"Failed to subscribe to OPC-UA nodes. Error: {e}. Ensure node IDs are correct and accessible."
            )
        except Exception:
            logger.exception(
                "An unexpected error occurred during OPC-UA subscription setup."
            )

    class SubscriptionHandler:
        """Internal class to mangage asyncua's data change callback."""

        def __init__(self, on_message_callback: MessageCallback):
            self._on_message_callback = on_message_callback

        def datachange_notification(self, node: Node, val, data):
            try:
                node_id = node.nodeid.to_string()
                logger.debug(f"OPC-UA DataChange: Node={node_id}, Value={val}")

                standardized_message = {
                    "source": "opcua",
                    "topic": node_id,  # NodeId as "topic" for the routing
                    "payload": str(val).encode("utf-8"),
                    "timestamp": data.ServerTimestamp,
                    "quality": data.Value.StatusCode.name,
                }
                self._on_message_callback(standardized_message)
            except Exception:
                logger.exception("Error processing OPC-UA data change notification.")
