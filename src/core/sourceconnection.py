import asyncio
import socket
import threading
import paho.mqtt.client as mqtt

from abc import ABC, abstractmethod
from asyncua import Client, ua
from loguru import logger
from multiprocessing.synchronize import Event


class ISourceConnection(ABC):
    """
    Manages the connection lifecycle of a data source.
    Its responsibility is to connect to an external service, handle raw data,
    and pass it to a callback provided by its source class.
    """

    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def connect(self) -> bool:
        """
        Establishes the connection to the endpoint.
        """
        raise NotImplementedError

    @abstractmethod
    def disconnect(self) -> None:
        """Stops the component and cleans up resources."""
        raise NotImplementedError


class MqttSourceConnection(ISourceConnection):
    """Handles the connection logic for an MQTT broker."""

    def __init__(self, config: dict):
        super().__init__(config)
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, client_id=self.config["client_id"]
        )
        self.connect()
        self.heartbeat_interval = config.get("heartbeat", {}).get("interval_seconds")
        self.heartbeat = None
        self._stop_event = threading.Event()

    def connect(self) -> mqtt.Client | None:
        try:
            logger.info(f"Attempting to connect to {self.config['broker_host']}...")
            self.client.connect(
                self.config["broker_host"],
                self.config["broker_port"],
                self.config["keepalive"],
            )
            self.client.subscribe(self.config["topic_subscribe"])
            self.client.loop_start()

            self.heartbeat = threading.Timer(
                2.0, lambda: self._start_heartbeat(self.heartbeat_interval)
            ).start()
            self._stop_event.clear()

            return self.client
        except (socket.gaierror, ConnectionRefusedError, TimeoutError) as e:
            logger.critical(
                f"MQTT connection failed: Could not reach broker at {self.config['broker_host']}:{self.config['broker_port']}. "
                f"Error: {e}. Check configuration or broker status."
            )
            return None
        except Exception:
            logger.exception(
                "An unexpected, non-connection error occurred during MQTT setup"
            )
            return None

    def disconnect(self) -> None:
        logger.info("Disconnecting from MQTT broker...")
        try:
            self.client.loop_stop()
            self.client.disconnect()
            self.client.on_disconnect()
        except Exception as e:
            logger.warning(f"Exception during MQTT broker disconnection: {e}")

    @property
    def _is_healthy(self) -> bool:
        return self.client.is_connected()

    def _perform_reconnect(self) -> bool:
        logger.info("MQTT: Heartbeat failed, attempting to perform reconnection...")
        try:
            self.client.reconnect()
            return self.client.is_connected()
        except Exception:
            # If paho's reconnect() fails we raise an exception to handle it to tenacity
            raise ConnectionError("MQTT reconnect attempt failed")


class OpcuaSourceConnection:
    """Handles the async connection and subscription logic for an OPC UA server."""

    def __init__(self, config: dict):
        self.server_url = config["server_url"]
        self.nodes_to_subscribe = config.get("nodes_to_subscribe", [])
        self.publishing_interval = config.get("publishing_interval_ms", 500)

    async def connect_and_run(self, data_change_handler: object, stop_event: Event):
        """
        The main async method that orchestrates the client's lifecycle.
        It connects, creates a subscription, and keeps running until stopped.
        """
        client = Client(url=self.server_url)
        try:
            async with client:
                logger.success(f"OPC-UA: Successfully connected to {self.server_url}.")
                subscription = await client.create_subscription(
                    self.publishing_interval, data_change_handler
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
