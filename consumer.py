# consumer.py  (Windows Safe - using avro-python3)

import io
import json
import time
import random
from collections import defaultdict
from typing import Dict
from confluent_kafka import Consumer, Producer

from avro.schema import Parse
from avro.io import DatumReader, BinaryDecoder

BOOTSTRAP = "localhost:29092"
CONSUMER_GROUP = "orders-consumer-group"
TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
SCHEMA_FILE = "order.avsc"

MAX_RETRIES = 4
INITIAL_BACKOFF = 0.5  # seconds

# Load Avro schema
with open(SCHEMA_FILE, "r") as f:
    schema = Parse(f.read())

datum_reader = DatumReader(schema)


def avro_deserialize(bytes_val):
    bio = io.BytesIO(bytes_val)
    decoder = BinaryDecoder(bio)
    return datum_reader.read(decoder)


# Kafka consumer config
c_conf = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "max.poll.interval.ms": 600000,
}

consumer = Consumer(c_conf)

# Producer for DLQ
p_conf = {"bootstrap.servers": BOOTSTRAP}
producer = Producer(p_conf)

# Running averages
overall_count = 0
overall_sum = 0.0
product_stats: Dict[str, Dict[str, float]] = defaultdict(lambda: {"count": 0, "sum": 0.0})


def compute_running_average(price):
    global overall_count, overall_sum
    overall_count += 1
    overall_sum += price
    return overall_sum / overall_count


def update_product_avg(product, price):
    s = product_stats[product]
    s["count"] += 1
    s["sum"] += price
    return s["sum"] / s["count"]


class TemporaryProcessingError(Exception):
    pass


class PermanentProcessingError(Exception):
    pass


def process_order(order):
    """Simulate order processing with failures for demo purposes."""
    if not order.get("orderId") or not order.get("product"):
        raise PermanentProcessingError("Invalid order data")

    # 10% transient failure
    if random.random() < 0.10:
        raise TemporaryProcessingError("Simulated transient failure")

    # 2% permanent failure
    if random.random() < 0.02:
        raise PermanentProcessingError("Simulated permanent failure")

    price = float(order["price"])

    overall_avg = compute_running_average(price)
    product_avg = update_product_avg(order["product"], price)

    return {
        "overall_avg": overall_avg,
        "product_avg": product_avg,
        "processed_count": overall_count,
    }


def send_to_dlq(key, value_bytes, reason="permanent_failure"):
    print(f"⚠ Sending {key} to DLQ (reason={reason})")
    producer.produce(
        DLQ_TOPIC,
        key=key,
        value=value_bytes,
        headers=[("dlq-reason", reason.encode())],
    )
    producer.flush()


def run_consumer():
    consumer.subscribe([TOPIC])
    print("📥 Consumer is running...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                print("❌ Consumer error:", msg.error())
                continue

            key = msg.key().decode() if msg.key() else None
            raw = msg.value()

            # Safe Avro deserialization
            try:
                order = avro_deserialize(raw)
            except Exception as e:
                print(f"❌ Deserialization failed → DLQ: {e}")
                send_to_dlq(msg.key(), raw, reason="deserialization_failure")
                consumer.commit(message=msg)
                continue

            # Processing with retry logic
            attempt = 0
            backoff = INITIAL_BACKOFF

            while attempt <= MAX_RETRIES:
                try:
                    result = process_order(order)

                    print(
                        f"✔ Processed {key}: "
                        f"overall_avg={result['overall_avg']:.2f}, "
                        f"product_avg={result['product_avg']:.2f}, "
                        f"count={result['processed_count']}"
                    )

                    consumer.commit(message=msg)
                    break

                except TemporaryProcessingError as te:
                    attempt += 1
                    print(
                        f"🔁 Temporary error for {key}, "
                        f"attempt {attempt}/{MAX_RETRIES}: {te}. Waiting {backoff}s..."
                    )
                    time.sleep(backoff)
                    backoff *= 2

                    if attempt > MAX_RETRIES:
                        print(f"❗ Max retries exceeded → DLQ: {key}")
                        send_to_dlq(msg.key(), raw, reason="max_retries_exceeded")
                        consumer.commit(message=msg)
                        break

                except PermanentProcessingError as pe:
                    print(f"❌ Permanent error for {key}: {pe} → DLQ")
                    send_to_dlq(msg.key(), raw, reason="permanent_error")
                    consumer.commit(message=msg)
                    break

                except Exception as e:
                    attempt += 1
                    print(f"⚠ Unexpected error for {key}: {e}")
                    time.sleep(backoff)
                    backoff *= 2

                    if attempt > MAX_RETRIES:
                        send_to_dlq(msg.key(), raw, reason="unexpected_max_retries")
                        consumer.commit(message=msg)
                        break

    except KeyboardInterrupt:
        print("🛑 Stopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()
