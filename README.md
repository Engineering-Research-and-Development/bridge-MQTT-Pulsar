# MQTT to Apache Pulsar Bridge

A configuration-driven bridge to forward messages from an MQTT broker to specific Apache Pulsar topics based on routing rules.

All settings (brokers, topics, routing rules) are managed via the `config.yaml` file.

## Prerequisites

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) (for local development)
- Docker & Docker Compose (for running the full environment)

## Getting Started (Local Development)

Follow these steps to run the bridge on your local machine for development.

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/Engineering-Research-and-Development/bridge-MQTT-Pulsar.git
    cd bridge-MQTT-Pulsar
    ```

2.  **Set up the virtual environment:**
    Create and activate a local virtual environment using `uv`.
    ```bash
    uv venv
    ```

3.  **Install dependencies in editable mode:**
    Install the project using `pyproject.toml`.
    ```bash
    uv pip install -e .
    ```

4.  **Configure the bridge:**
    Edit the `config.yaml` file to match your local setup.

5.  **Run the application:**
    Now run the script using `uv`.
    ```bash
    uv run bridge
    ```

## Running with Docker

The provided `Dockerfile` builds a production-ready image. This is the recommended way to run the bridge in a deployed environment.

1.  **Build the Docker image:**
    From the project root directory, run:
    ```bash
    docker build -t bridge-mqtt-pulsar:latest .
    ```

2.  **Prepare the configuration for Docker:**
    Remember that your `config.yaml` will need to use Docker service names instead of `localhost` (e.g., `mosquitto`, `pulsar`).

3.  **Run the container:**
    Ensure the bridge container is on the same Docker network as your other services.
    ```bash
    docker run --rm --name my-bridge --network your-network-name \
      -v "$(pwd)/config.yaml:/app/config.yaml" \
      bridge-mqtt-pulsar:latest
    ```
    - `--network`: Connects the bridge to your services.
    - `-v`: Mounts your Docker-specific config file into the container.

## Configuration

All behavior is controlled via `config.yaml`.

-   **`mqtt`**: Connection details for the MQTT broker (`broker_host`, `broker_port`, etc.).
-   **`pulsar`**: Connection details for the Pulsar service (`service_url`).
-   **`routing`**: Defines the rules for the topic router.
    -   `match_prefix`: The MQTT topic prefix to match (e.g., `test/data`).
    -   `device_index`: The zero-based index of the topic segment that contains the dynamic device name.