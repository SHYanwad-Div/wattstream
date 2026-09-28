"""Simulate ESP32-style smart meters and stream their readings to Kafka.

Every meter sends one reading per second (by default) to the meter-readings
topic. Values are Avro-encoded with schemas/meter_reading.avsc, which the
serializer registers in Schema Registry on the first send.

Usage (PowerShell, with the virtual environment active):
    python producer.py                          # 5 meters, 1 reading/s each, until Ctrl+C
    python producer.py --duration 60            # stop after 60 seconds
    python producer.py --devices 10 --rate 2    # 10 meters, 2 readings/s each
"""
import argparse
import random
import time
from collections import Counter, defaultdict
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

import config

SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "meter_reading.avsc"

NOMINAL_VOLTAGE_V = 230.0
VOLTAGE_NOISE_V = 2.0         # standard deviation of the voltage noise
SPIKE_PROBABILITY = 0.02      # about 2% of readings are overload spikes
OVERLOAD_THRESHOLD_W = 3000   # same threshold as the Flink alert query

# Typical current draw in amps for each meter, like homes with different appliances.
# With more than five meters, the profiles repeat.
BASE_CURRENTS_A = [1.5, 3.0, 4.5, 6.0, 8.0]


def make_devices(count: int) -> dict[str, float]:
    """Return {device_id: typical current in amps} for meter-01, meter-02, ..."""
    return {
        f"meter-{i:02d}": BASE_CURRENTS_A[(i - 1) % len(BASE_CURRENTS_A)]
        for i in range(1, count + 1)
    }


def make_reading(device_id: str, base_current_a: float, rng: random.Random, event_ts_ms: int) -> dict:
    """Build one reading as a dict that matches schemas/meter_reading.avsc."""
    voltage_v = round(rng.gauss(NOMINAL_VOLTAGE_V, VOLTAGE_NOISE_V), 2)
    if rng.random() < SPIKE_PROBABILITY:
        # Overload spike: 15-25 A at ~230 V is about 3.4-5.9 kW, well above the threshold.
        current_a = rng.uniform(15.0, 25.0)
    else:
        # Normal load: this meter's typical current, give or take about 10%.
        current_a = rng.gauss(base_current_a, base_current_a * 0.1)
    current_a = round(current_a, 3)
    return {
        "device_id": device_id,
        "event_ts": event_ts_ms,
        "voltage_v": voltage_v,
        "current_a": current_a,
        "power_w": round(voltage_v * current_a, 2),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream simulated smart-meter readings to Kafka.")
    parser.add_argument("--devices", type=int, default=5, help="number of meters (default: 5)")
    parser.add_argument("--rate", type=float, default=1.0,
                        help="readings per second from each meter (default: 1)")
    parser.add_argument("--duration", type=float, default=0,
                        help="seconds to run; 0 means until Ctrl+C (default: 0)")
    args = parser.parse_args()
    if args.devices < 1 or args.rate <= 0 or args.duration < 0:
        parser.error("use --devices 1 or more, --rate above 0, and --duration 0 or more")
    return args


def main() -> None:
    args = parse_args()
    devices = make_devices(args.devices)
    topic = config.READINGS_TOPIC
    rng = random.Random()

    producer = Producer({
        **config.kafka_config(),
        "acks": "all",               # a write counts only when all in-sync replicas have it
        "enable.idempotence": True,  # retries can't duplicate or reorder messages
        "log_level": 3,              # librdkafka logs errors only (hides startup chatter)
    })

    # Values are Avro. On the first send, the serializer registers the schema under the
    # subject "meter-readings-value"; after that, each message carries just the schema ID.
    schema_registry = SchemaRegistryClient(config.schema_registry_config())
    serialize_value = AvroSerializer(schema_registry, SCHEMA_PATH.read_text(encoding="utf-8"))
    serialize_key = StringSerializer("utf_8")

    counts = Counter()
    partitions = defaultdict(set)  # device_id -> partitions its readings landed on

    def on_delivery(err, msg):
        """Called from poll()/flush() once Kafka has confirmed or rejected a message."""
        if err is not None:
            counts["failed"] += 1
            print(f"  delivery failed: {err}")
        else:
            counts["delivered"] += 1
            partitions[msg.key().decode("utf-8")].add(msg.partition())

    print(f"Producing to '{topic}': {len(devices)} meters, {args.rate:g} reading(s)/s each. "
          "Ctrl+C to stop.")
    interval = 1.0 / args.rate
    start = next_batch_at = time.monotonic()
    batch = 0
    try:
        while args.duration == 0 or time.monotonic() - start < args.duration:
            batch += 1
            now_ms = int(time.time() * 1000)  # wall-clock time, used as the event time
            overloads = []
            for device_id, base_current_a in devices.items():
                reading = make_reading(device_id, base_current_a, rng, now_ms)
                if reading["power_w"] > OVERLOAD_THRESHOLD_W:
                    overloads.append(f"{device_id} {reading['power_w']:.0f} W")
                producer.produce(
                    topic,
                    # Key = device_id. The partition is chosen by hashing the key, so every
                    # reading from one meter goes to the same partition and stays in order.
                    key=serialize_key(device_id),
                    value=serialize_value(reading, SerializationContext(topic, MessageField.VALUE)),
                    # Record timestamp = event time, so Flink's $rowtime is when the reading was taken.
                    timestamp=reading["event_ts"],
                    on_delivery=on_delivery,
                )
            counts["overloads"] += len(overloads)
            note = f" | OVERLOAD: {', '.join(overloads)}" if overloads else ""
            print(f"{time.strftime('%H:%M:%S')} batch {batch}: sent {len(devices)}, "
                  f"delivered so far {counts['delivered']}, failed {counts['failed']}{note}")

            # produce() only queues messages; they are sent in the background. poll() runs
            # the delivery callbacks, so we keep calling it while we wait for the next batch.
            next_batch_at += interval
            while time.monotonic() < next_batch_at:
                producer.poll(max(next_batch_at - time.monotonic(), 0))
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        unsent = producer.flush(10)  # wait up to 10 s for queued messages to be delivered
        print(f"Done: {counts['delivered']} delivered, {counts['failed']} failed, {unsent} unsent, "
              f"{counts['overloads']} overload spikes.")
        print("Partition per meter (same key -> same partition):")
        for device_id in sorted(partitions):
            print(f"  {device_id} -> partition {', '.join(map(str, sorted(partitions[device_id])))}")


if __name__ == "__main__":
    main()
