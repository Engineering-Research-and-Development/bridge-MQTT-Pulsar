import sys
import os
import yaml
from loguru import logger

from .orchestrator import Orchestrator
from .sources.mqttsource import MqttSource
from .sources.opcuasource import OpcUaSource
from .destinations.pulsar import PulsarDestination


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

    pipelines = {}

    pulsar_dest = PulsarDestination(config["pulsar"])

    if not pulsar_dest.connect():
        logger.critical("Critical Pulsar initialization error. Exiting.")
        return

    # TODO: discovery dinamica da config?
    sources = {}
    if config.get("mqtt", {}).get("enabled", False):
        sources["mqtt"] = MqttSource(config["mqtt"])
    if config.get("opcua", {}).get("enabled", False):
        sources["opcua"] = OpcUaSource(config["opcua"])

    if "mqtt" in sources:
        pipelines[sources["mqtt"]] = pulsar_dest
        logger.info("Pipeline configured: MQTT -> Pulsar")

    if "opcua" in sources:
        pipelines[sources["opcua"]] = pulsar_dest
        logger.info("Pipeline configured: OPC UA -> Pulsar")

    if not pipelines:
        logger.critical("No pipelines configured or sources enabled. Exiting.")
        return

    bridge = Orchestrator(pipelines)
    bridge.run()


if __name__ == "__main__":
    main()
