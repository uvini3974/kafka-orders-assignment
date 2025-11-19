#  Kafka Orders Processing System (Assignment)

Applied Big Data Engineering  EC8203


## Project Overview

This project implements a **Kafka-based real-time processing system** for order messages, using:

- Apache Kafka for messaging  
- Avro serialization  
- Python Producers & Consumers  
- Real-time running average  
- Retry logic with exponential backoff  
- Dead Letter Queue (DLQ) support  

This demonstrates a complete end-to-end data pipeline.

---

## System Features

###  Avro Serialization
All messages follow this schema:

```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "price", "type": "float"}
  ]
}
```

## Real-Time Aggregat

-Global running average of order prices
-Per-product running averages
---
## Retry Logic

-4 retry attempts
-Exponential backoff (0.5s, 1s, 2s, 4s)
-If still failing → DLQ
---
## DLQ Support

-Failed messages are published to

## Technologies Used

Component	      Technology
Messaging	      Apache Kafka
Serialization	  Avro (avro-python3)
Producer	      Python + confluent-kafka
Consumer	      Python + confluent-kafka
DLQ	Kafka       secondary topic
Retry Logic	    Python + exponential backoff
Orchestration	  Docker Compose

## How to Run the Project

Step 1    Start Kafka
docker compose up -d

Step 2    Create Topics
docker exec -it kafka-orders-assignment-kafka-1 bash
kafka-topics --bootstrap-server localhost:29092 --create --topic orders --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:29092 --create --topic orders-dlq --partitions 3 --replication-factor 1

Step 3    Create Virtual Environment
python -m venv venv
venv\Scripts\Activate.ps1

Step 5     Run Consumer
python consumer.py

Step 6     Run Producer
python producer.py

step 7     Testing DLQ
kafka-console-consumer --bootstrap-server localhost:29092 --topic orders-dlq --from-beginning

## Project Structure

kafka-orders-assignment/
│
├── producer.py
├── consumer.py
├── order.avsc
├── README.md
└── venv/




