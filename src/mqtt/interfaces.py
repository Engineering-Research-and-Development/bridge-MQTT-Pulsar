from abc import ABC, abstractmethod

class MessageSource(ABC):
    """Defines the contract for any source of messages (MQTT, OPC UA, etc.)."""
    @abstractmethod
    async def connect(self):
        raise NotImplementedError

    @abstractmethod
    async def start_listening(self):
        raise NotImplementedError

    @abstractmethod
    async def stop(self):
        raise NotImplementedError