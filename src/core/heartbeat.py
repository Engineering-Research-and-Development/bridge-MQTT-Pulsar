import threading
from abc import ABC, abstractmethod
from loguru import logger
from tenacity import Retrying, stop_after_attempt, wait_exponential


class HeartbeatMixin(ABC):
    """
    Provides heartbeat functionality for each individual module.

    A component using this mixin must implement:
    - is_healthy(): A check to determine if the component is connected.
    - perform_reconnect(): The logic to attempt a reconnection.
    """

    _heartbeat_timer: threading.Timer | None = None

    @property
    @abstractmethod
    def _is_healthy(self) -> bool:
        """Must return True if the component is healthy."""
        raise NotImplementedError

    @abstractmethod
    def _perform_reconnect(self) -> bool:
        """Must contain the logic to reconnect."""
        raise NotImplementedError

    def _start_heartbeat(self, interval_seconds: int):
        """Starts the heartbeat timer."""
        if self._stop_event.is_set():
            return

        # Schedules next healthcheck
        self._heartbeat_timer = threading.Timer(
            interval_seconds, self._run_heartbeat_check, [interval_seconds]
        )
        self._heartbeat_timer.daemon = True
        self._heartbeat_timer.start()

    def _stop_heartbeat(self):
        """Stops the heartbeat timer."""
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()
            self._heartbeat_timer = None

    def _run_heartbeat_check(self, interval_seconds: int):
        """The core logic executed by the timer."""
        component_name = self.__class__.__name__

        if self._is_healthy:
            logger.debug(f"Heartbeat check PASSED for {component_name}.")
        else:
            logger.warning(
                f"Heartbeat check FAILED for {component_name}. Attempting to reconnect..."
            )

            reconnect_retrier = Retrying(
                stop=stop_after_attempt(5),
                wait=wait_exponential(multiplier=1, min=2, max=10),
                reraise=True,
            )

            try:
                reconnect_retrier(self._perform_reconnect)
                logger.success(
                    f"Successfully reconnected {component_name} after heartbeat failure."
                )
            except Exception:
                logger.critical(
                    f"Could not reconnect {component_name} after multiple attempts. "
                    f"The component remains disconnected."
                )

        # Schedules next check
        self._start_heartbeat(interval_seconds)
