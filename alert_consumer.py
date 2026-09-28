"""Print overload alerts from Kafka as readable lines.

The alerts come from the Flink job in flink/02_overload_alerts.sql. Each message
is Avro; the deserializer fetches Flink's schema from Schema Registry using the
schema ID stored in the message.

Usage (PowerShell, with the virtual environment active):
    python alert_consumer.py                # consumer group wattstream-alert-consumer
    python alert_consumer.py --group demo   # a new group starts from the oldest alert
"""
import argparse

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import config


def format_alert(alert: dict, partition: int, offset: int) -> str:
    """One readable line per alert, with the time shown in local time."""
    when = alert["event_ts"].astimezone().strftime("%H:%M:%S")
    return (f"{when}  {alert['severity']:<4}  {alert['device_id']}  {alert['power_w']:>6.0f} W  "
            f"({alert['voltage_v']:.1f} V x {alert['current_a']:.2f} A)  "
            f"[partition {partition}, offset {offset}]")


def main() -> None:
    parser = argparse.ArgumentParser(description="Print overload alerts from Kafka.")
    parser.add_argument("--group", default="wattstream-alert-consumer",
                        help="consumer group ID (default: wattstream-alert-consumer)")
    args = parser.parse_args()
    topic = config.ALERTS_TOPIC

    consumer = Consumer({
        **config.kafka_config(),
        # Consumers with the same group.id share the work: Kafka gives each one some of the
        # partitions and remembers the group's position (offset) in every partition.
        "group.id": args.group,
        # Where a group starts when it has no saved offset yet: the oldest message.
        # Offsets are committed automatically every 5 seconds (enable.auto.commit),
        # so a restarted consumer continues where it left off.
        "auto.offset.reset": "earliest",
        "log_level": 3,  # librdkafka logs errors only
    })
    deserialize = AvroDeserializer(SchemaRegistryClient(config.schema_registry_config()))

    def on_assign(_consumer, partitions):
        print(f"Assigned partitions: {sorted(p.partition for p in partitions)}")

    consumer.subscribe([topic], on_assign=on_assign)
    print(f"Reading '{topic}' as consumer group '{args.group}'. Ctrl+C to stop.")
    try:
        while True:
            msg = consumer.poll(1.0)  # wait up to 1 s for the next message
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            alert = deserialize(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            print(format_alert(alert, msg.partition(), msg.offset()))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()  # commit the final offsets and leave the group cleanly


if __name__ == "__main__":
    main()
