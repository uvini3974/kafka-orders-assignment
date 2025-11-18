# producer.py (Windows Safe - using avro-python3)

import uuid
import time
import random
import json
import io
from confluent_kafka import Producer
from avro.schema import Parse
from avro.io import DatumWriter, BinaryEncoder

BOOTSTRAP = "localhost:29092"
TOPIC = "orders"
SCHEMA_FILE = "order.avsc"


# Load and parse Avro schema
def load_schema(path):
    with open(path, "r") as f:
        return Parse(f.read())


schema = load_schema(SCHEMA_FILE)


# Serialize using avro-python3
def avro_serialize(schema, record):
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer = DatumWriter(schema)
    writer.write(record, encoder)
    return bytes_writer.getvalue()


# Kafka Producer
conf = {
    "bootstrap.servers": BOOTSTRAP
}

p = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for {msg.key()}: {err}")
    else:
        print(f"✔ Delivered {msg.key().decode()} to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def produce_one(order):
    key = order["orderId"].encode()
    val = avro_serialize(schema, order)

    p.produce(
        TOPIC,
        value=val,
        key=key,
        callback=delivery_report
    )

    p.poll(0)  # process callbacks


def random_order(product_list):
    return {
        "orderId": str(uuid.uuid4()),
        "product": random.choice(product_list),
        "price": round(random.uniform(5.0, 500.0), 2)
    }


if __name__ == "__main__":
    products = ["ItemA", "ItemB", "ItemC", "ItemD"]

    try:
        print("🚀 Producing orders... (Ctrl+C to stop)")
        while True:
            order = random_order(products)
            print(f"➡ Producing: {order}")
            produce_one(order)
            time.sleep(random.uniform(0.2, 1.0))
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        p.flush()
