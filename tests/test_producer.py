"""Unit tests for the simulated meter readings in producer.py.

No Kafka or credentials needed: these only test the pure function that builds a
reading. A fixed random seed makes the "random" data identical on every run.
"""
import json
import random

import pytest

import producer


@pytest.fixture(scope="module")
def readings():
    """20,000 readings: 4,000 rounds from 5 meters, generated with a fixed seed."""
    rng = random.Random(42)
    devices = producer.make_devices(5)
    return [
        producer.make_reading(device_id, base_current_a, rng, event_ts_ms=1_700_000_000_000)
        for _ in range(4_000)
        for device_id, base_current_a in devices.items()
    ]


def test_device_ids_and_profiles():
    assert list(producer.make_devices(3)) == ["meter-01", "meter-02", "meter-03"]
    # With more than five meters, the current profiles repeat.
    devices = producer.make_devices(7)
    assert devices["meter-06"] == devices["meter-01"]


def test_reading_matches_the_avro_schema_fields(readings):
    schema = json.loads(producer.SCHEMA_PATH.read_text(encoding="utf-8"))
    assert set(readings[0]) == {field["name"] for field in schema["fields"]}
    assert readings[0]["event_ts"] == 1_700_000_000_000


def test_voltage_stays_near_230_volts(readings):
    assert all(215 < r["voltage_v"] < 245 for r in readings)


def test_power_is_voltage_times_current(readings):
    for r in readings:
        assert r["power_w"] == pytest.approx(r["voltage_v"] * r["current_a"], abs=0.01)


def test_about_two_percent_of_readings_are_overload_spikes(readings):
    spikes = sum(r["power_w"] > producer.OVERLOAD_THRESHOLD_W for r in readings)
    assert 0.015 <= spikes / len(readings) <= 0.025


def test_only_spikes_cross_the_alert_threshold(readings):
    # Normal load is each meter's typical current +/- 10% (at most 8 A); spikes draw 15-25 A.
    normal = [r for r in readings if r["current_a"] < 15]
    spikes = [r for r in readings if r["current_a"] >= 15]
    assert max(r["power_w"] for r in normal) < producer.OVERLOAD_THRESHOLD_W
    assert min(r["power_w"] for r in spikes) > producer.OVERLOAD_THRESHOLD_W
