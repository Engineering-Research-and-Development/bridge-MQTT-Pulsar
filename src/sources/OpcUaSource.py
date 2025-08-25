# src/sources/opcua.py

import asyncio
import threading
from loguru import logger
from asyncua import Client, Node
from concurrent.futures import Future

from .interfaces import IMessageSource, MessageCallback


class OpcUaSource(IMessageSource):
    """
    Message source for collecting data from an OPC UA server by subscribing to node changes.
    """

    def __init__(self, config: dict):
        self.config = config
        self.server_url = config["server_url"]
        self.nodes_to_subscribe = config["nodes_to_subscribe"]
        self.client = Client(url=self.server_url)
        self._on_message_callback: MessageCallback | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._connection_future: Future | None = None

    def connect(self) -> bool:
        """
        Starts the async connection process in a separated thread and waits for the result.
        Returns True if the connection is successfull, False otherwise.
        """
        if self._thread is not None:
            logger.warning("OPC-UA connect called more than once.")
            return False

        self._connection_future = Future()
        self._thread = threading.Thread(target=self._run_async_loop, daemon=True)
        self._thread.start()

        return self._connection_future.result(timeout=10)

    def start(self, on_message_callback: MessageCallback):
        if not callable(on_message_callback):
            raise TypeError("on_message_callback must be a callable function")
        self._on_message_callback = on_message_callback

        self._thread = threading.Thread(target=self._run_async_loop, daemon=True)
        self._thread.start()
        logger.info("OPC-UA source thread started.")

    def _run_async_loop(self):
        """Entry point for the thread to start managing the asyncio loop."""
        try:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._connect_and_listen())
        except Exception:
            logger.exception(
                "An unhandled exception occurred in the OPC-UA async loop."
            )
        finally:
            if self._loop and self._loop.is_running():
                self._loop.close()
            logger.info("OPC-UA async loop has been closed.")

    async def _connect_and_listen(self):
        """Async logic for connection and listening."""
        try:
            async with self.client:
                logger.info(f"OPC-UA client connecting to {self.server_url}...")

                self._connection_future.set_result(True)
                logger.success(
                    f"Successfully connected to OPC-UA server: {self.server_url}"
                )

                while (
                    self._on_message_callback is None and not self._stop_event.is_set()
                ):
                    await asyncio.sleep(0.1)

                if self._on_message_callback:
                    handler = self.SubscriptionHandler(self._on_message_callback)
                    subscription = await self.client.create_subscription(500, handler)
                    nodes = [
                        self.client.get_node(node_id)
                        for node_id in self.nodes_to_subscribe
                    ]
                    await subscription.subscribe_data_change(nodes)
                    logger.success(
                        "OPC-UA subscription successful. Listening for data changes..."
                    )

                while not self._stop_event.is_set():
                    await asyncio.sleep(1)

        except Exception as e:
            logger.critical(f"Failed to connect or subscribe to OPC-UA server: {e}")
            if not self._connection_future.done():
                self._connection_future.set_result(False)
        finally:
            logger.warning("OPC-UA client is disconnecting.")

    def stop(self):
        logger.info("Stopping OPC-UA source...")
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                logger.warning("OPC-UA thread did not terminate gracefully.")
        logger.info("OPC-UA source stopped.")

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
                    "timestamp": data.monitored_item.ServerTimestamp,
                    "quality": data.monitored_item.Value.StatusCode.name,
                }
                self._on_message_callback(standardized_message)
            except Exception:
                logger.exception("Error processing OPC-UA data change notification.")
