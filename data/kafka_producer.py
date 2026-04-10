"""
Kafka Network Event Simulator
Project: Real-Time Network Event Streaming Pipeline
Cisco-style use case: simulates network device telemetry events

Produces events to Kafka topic: network-events
Run: python kafka_producer.py

Event types mirror real network monitoring:
  - device_health     CPU, memory, temperature readings
  - interface_stats   Packet loss, bandwidth utilization
  - security_alert    Port scans, auth failures, anomalies
  - link_state        Interface up/down events
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

# ── Config ────────────────────────────────────────────────────
KAFKA_BROKER  = "localhost:9092"
TOPIC         = "network-events"
EVENTS_PER_SEC = 10   # throttle for local dev

# ── Device registry (simulated network inventory) ─────────────
DEVICES = [
    {"device_id": f"SW-{i:03d}", "device_type": "Switch",
     "location": random.choice(["DC-East","DC-West","Branch-NY","Branch-LA","Branch-CHI"]),
     "vendor": random.choice(["Cisco","Juniper","Arista"]),
     "model": random.choice(["Catalyst 9300","EX4300","7050CX3"])}
    for i in range(1, 21)
] + [
    {"device_id": f"RTR-{i:03d}", "device_type": "Router",
     "location": random.choice(["DC-East","DC-West","Branch-NY"]),
     "vendor": "Cisco",
     "model": random.choice(["ASR 1001","ISR 4431","CSR 1000V"])}
    for i in range(1, 11)
] + [
    {"device_id": f"FW-{i:03d}", "device_type": "Firewall",
     "location": random.choice(["DC-East","DC-West"]),
     "vendor": random.choice(["Cisco","Palo Alto","Fortinet"]),
     "model": random.choice(["ASA 5506","PA-220","FortiGate 60F"])}
    for i in range(1, 6)
]

SEVERITY_WEIGHTS = {
    "INFO":     0.60,
    "WARNING":  0.25,
    "CRITICAL": 0.10,
    "ERROR":    0.05,
}

# ── Event generators ──────────────────────────────────────────

def device_health_event(device):
    cpu  = random.uniform(5, 98)
    mem  = random.uniform(20, 95)
    temp = random.uniform(30, 85)
    severity = (
        "CRITICAL" if cpu > 90 or mem > 90 or temp > 80 else
        "WARNING"  if cpu > 75 or mem > 75 or temp > 70 else
        "INFO"
    )
    return {
        "event_id":     str(uuid.uuid4()),
        "event_type":   "device_health",
        "device_id":    device["device_id"],
        "device_type":  device["device_type"],
        "location":     device["location"],
        "vendor":       device["vendor"],
        "model":        device["model"],
        "severity":     severity,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "payload": {
            "cpu_utilization_pct":    round(cpu, 2),
            "memory_utilization_pct": round(mem, 2),
            "temperature_celsius":    round(temp, 2),
            "uptime_seconds":         random.randint(3600, 31536000),
        }
    }

def interface_stats_event(device):
    packet_loss = random.uniform(0, 15)
    bandwidth   = random.uniform(0, 100)
    severity = (
        "CRITICAL" if packet_loss > 10 or bandwidth > 95 else
        "WARNING"  if packet_loss > 5  or bandwidth > 80 else
        "INFO"
    )
    return {
        "event_id":     str(uuid.uuid4()),
        "event_type":   "interface_stats",
        "device_id":    device["device_id"],
        "device_type":  device["device_type"],
        "location":     device["location"],
        "vendor":       device["vendor"],
        "model":        device["model"],
        "severity":     severity,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "payload": {
            "interface":              f"GigabitEthernet{random.randint(0,3)}/{random.randint(0,24)}",
            "packet_loss_pct":        round(packet_loss, 3),
            "bandwidth_utilization":  round(bandwidth, 2),
            "bytes_in":               random.randint(1000, 10_000_000),
            "bytes_out":              random.randint(1000, 10_000_000),
            "errors_in":              random.randint(0, 50),
            "errors_out":             random.randint(0, 50),
        }
    }

def security_alert_event(device):
    alert_types = [
        "port_scan_detected",
        "auth_failure_spike",
        "suspicious_traffic_pattern",
        "ddos_signature_matched",
        "unauthorized_access_attempt",
    ]
    alert = random.choice(alert_types)
    severity = (
        "CRITICAL" if alert in ("ddos_signature_matched", "unauthorized_access_attempt") else
        "ERROR"    if alert == "port_scan_detected" else
        "WARNING"
    )
    return {
        "event_id":     str(uuid.uuid4()),
        "event_type":   "security_alert",
        "device_id":    device["device_id"],
        "device_type":  device["device_type"],
        "location":     device["location"],
        "vendor":       device["vendor"],
        "model":        device["model"],
        "severity":     severity,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "payload": {
            "alert_type":     alert,
            "source_ip":      f"192.168.{random.randint(0,255)}.{random.randint(1,254)}",
            "destination_ip": f"10.0.{random.randint(0,10)}.{random.randint(1,254)}",
            "port":           random.randint(1, 65535),
            "protocol":       random.choice(["TCP","UDP","ICMP"]),
            "packet_count":   random.randint(100, 100_000),
        }
    }

def link_state_event(device):
    state = random.choices(["up","down","flapping"], weights=[0.85, 0.10, 0.05])[0]
    severity = "CRITICAL" if state == "down" else "WARNING" if state == "flapping" else "INFO"
    return {
        "event_id":     str(uuid.uuid4()),
        "event_type":   "link_state",
        "device_id":    device["device_id"],
        "device_type":  device["device_type"],
        "location":     device["location"],
        "vendor":       device["vendor"],
        "model":        device["model"],
        "severity":     severity,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "payload": {
            "interface":    f"GigabitEthernet{random.randint(0,3)}/{random.randint(0,24)}",
            "state":        state,
            "previous_state": "up" if state != "up" else "down",
            "reason":       random.choice(["admin_shutdown","cable_unplugged","negotiation_failure","auto"]),
        }
    }

EVENT_GENERATORS = [
    device_health_event,
    interface_stats_event,
    security_alert_event,
    link_state_event,
]

# ── Producer ──────────────────────────────────────────────────

def run_producer(max_events=None):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    print(f"Kafka producer started → topic: {TOPIC}")
    print(f"Broker: {KAFKA_BROKER}  |  Rate: {EVENTS_PER_SEC} events/sec")
    print("Press Ctrl+C to stop\n")

    count = 0
    try:
        while True:
            device = random.choice(DEVICES)
            generator = random.choice(EVENT_GENERATORS)
            event = generator(device)

            producer.send(
                topic=TOPIC,
                key=event["device_id"],
                value=event,
            )

            count += 1
            if count % 50 == 0:
                print(f"  Sent {count:,} events | Latest: {event['event_type']} from {event['device_id']} [{event['severity']}]")

            if max_events and count >= max_events:
                break

            time.sleep(1 / EVENTS_PER_SEC)

    except KeyboardInterrupt:
        print(f"\nProducer stopped. Total events sent: {count:,}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    run_producer()
