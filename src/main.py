import sys
import os
import yaml
from loguru import logger

from .BridgeOrchestrator import BridgeOrchestrator
from .sources.MqttSource import MqttSource
from .sources.OpcUaSource import OpcUaSource
from .pulsar.publisher import PulsarPublisher
from .routing.CentralRouter import CentralRouter
from .routing.strategies.MqttTopicRouter import MqttTopicRouter
from .routing.strategies.OpcUaTopicRouter import OpcUaTopicRouter


def load_config(path: str = "config.yaml"):
    config_path = os.path.join(os.getcwd(), path)

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
            logger.info("Config loaded succesfully.")
            return config
    except FileNotFoundError:
        logger.critical(f"Config file not found in '{config_path}'")
        exit(1)
    except yaml.YAMLError as e:
        logger.critical(f"Syntax error in YAML file '{config_path}': {e}")
        exit(1)


def main():
    config = load_config("config.yaml")

    log_level = config.get("logging", {}).get("level", "INFO").upper()
    logger.remove()
    logger.add(sys.stderr, level=log_level, colorize=True)
    logger.info(f"Logger level set to: {log_level}")

    routing_config = config.get("routing", {})
    central_router = CentralRouter()
    sources = []
    # TODO: discovery dinamica?
    if config.get("mqtt", {}).get("enabled", False):
        sources.append(MqttSource(config["mqtt"]))
        central_router.register_strategy("mqtt", MqttTopicRouter(routing_config))

    if config.get("opcua", {}).get("enabled", False):
        sources.append(OpcUaSource(config["opcua"]))
        central_router.register_strategy("opcua", OpcUaTopicRouter(routing_config))

    if not sources:
        logger.critical("No message sources are enabled in the configuration. Exiting.")
        return

    publisher = PulsarPublisher(config["pulsar"], router=central_router)

    bridge = BridgeOrchestrator(sources=sources, publisher=publisher)
    bridge.run()


if __name__ == "__main__":
    main()
