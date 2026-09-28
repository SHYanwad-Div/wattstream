"""WattStream settings, loaded from a local .env file.

Real credentials live only in .env, which is git-ignored. The repo ships
.env.example with empty values, so API keys never reach GitHub.

Settings are read when the functions below are called, not at import time,
so the unit tests can import project code without any credentials.

Check your setup:  python config.py
"""
import os
from pathlib import Path

from dotenv import load_dotenv

# Load the .env that sits next to this file, whichever folder you run from.
load_dotenv(Path(__file__).resolve().parent / ".env")

# Topic names are not secrets, so they have defaults (override them in .env).
READINGS_TOPIC = os.getenv("READINGS_TOPIC", "meter-readings")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "overload-alerts")


def _require(name: str) -> str:
    """Return a setting from the environment, or fail with a clear message."""
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is not set. Copy .env.example to .env and fill it in.")
    return value


def kafka_config() -> dict:
    """Connection settings for a confluent_kafka Producer, Consumer or AdminClient."""
    return {
        "bootstrap.servers": _require("BOOTSTRAP_SERVERS"),
        # Confluent Cloud only accepts encrypted (TLS) connections. The API key and
        # secret are sent as a SASL/PLAIN username and password inside that connection.
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": _require("KAFKA_API_KEY"),
        "sasl.password": _require("KAFKA_API_SECRET"),
    }


def schema_registry_config() -> dict:
    """Settings for confluent_kafka.schema_registry.SchemaRegistryClient."""
    # Schema Registry is a separate service with its own API key (not the Kafka one).
    key = _require("SCHEMA_REGISTRY_API_KEY")
    secret = _require("SCHEMA_REGISTRY_API_SECRET")
    return {
        "url": _require("SCHEMA_REGISTRY_URL"),
        "basic.auth.user.info": f"{key}:{secret}",
    }


if __name__ == "__main__":
    # Self-check: confirms .env is filled in and Confluent Cloud accepts both keys.
    # Prints topic and subject names only, never the secrets.
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.schema_registry import SchemaRegistryClient

    topics = AdminClient(kafka_config()).list_topics(timeout=15).topics
    print(f"Kafka OK: {len(topics)} topic(s): {sorted(topics)}")
    if READINGS_TOPIC not in topics:
        print(f"  Topic '{READINGS_TOPIC}' does not exist yet. Create it in the Confluent Cloud UI.")

    subjects = SchemaRegistryClient(schema_registry_config()).get_subjects()
    print(f"Schema Registry OK: {len(subjects)} subject(s): {sorted(subjects)}")
