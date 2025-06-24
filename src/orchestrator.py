from loguru import logger
import asyncio
from .pulsar.interfaces import Publisher
from .mqtt.client import MqttClientManager
from .mqtt.interfaces import MessageSource

class BridgeOrchestrator:
    def __init__(self, sources: list[MessageSource], publisher: Publisher):
        self.sources = sources
        self.publisher = publisher
    
    async def run(self):
        logger.info("Starting MQTT -> Pulsar bridge...")

        for source in self.sources:
            await source.connect()

        tasks = [asyncio.create_task(source.start_listening()) for source in self.sources]
        await asyncio.gather(*tasks)

    async def stop(self):
        logger.info("Shutting down the bridge...")
        for source in self.sources:
            await source.stop()
        self.publisher.stop()
        logger.success("Bridge shut down successfully")