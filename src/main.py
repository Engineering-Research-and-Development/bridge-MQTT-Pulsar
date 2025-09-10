import sys
import os
import yaml
from loguru import logger

from .orchestrator import Orchestrator
from .sources.mqttsource import MqttSource
from .sources.opcuasource import OpcUaSource
from .destinations.pulsar import PulsarDestination
from .sources.interfaces import ISource
from .destinations.interfaces import IDestination


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

    components: dict[str, ISource | IDestination | None] = {
        "mqtt": MqttSource(config["mqtt"])
        if config.get("mqtt", {}).get("enabled")
        else None,
        "opcua": OpcUaSource(config["opcua"])
        if config.get("opcua", {}).get("enabled")
        else None,
        "pulsar": PulsarDestination(config["pulsar"])
        if config.get("pulsar", {}).get("enabled")
        else None,
    }

    pipelines_map = {}
    pipeline_configs = config.get("pipelines", [])

    if not pipeline_configs:
        logger.warning("No pipelines are defined in the configuration. Exiting.")
        return

    for p_config in pipeline_configs:
        source_name = p_config.get("source")
        dest_name = p_config.get("destination")

        source_instance = components.get(source_name)
        dest_instance = components.get(dest_name)

        if source_instance and dest_instance:
            pipelines_map[source_instance] = dest_instance
            logger.success(f"Pipeline configured: {source_name} -> {dest_name}")
        else:
            logger.error(
                f"Skipping pipeline '{p_config.get('name', 'unnamed')}': "
                f"Source '{source_name}' or Destination '{dest_name}' is not enabled in the config."
            )

    if not pipelines_map:
        logger.critical("No valid pipelines could be constructed. Exiting.")
        return

    bridge = Orchestrator(pipelines_map)
    bridge.run()


if __name__ == "__main__":
    main()
