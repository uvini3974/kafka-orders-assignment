#  Kafka Orders Processing System (Assignment)
_Applied Big Data Engineering — EC8203_  
_University of Ruhuna_

##  1. Project Overview

This project implements a **Kafka-based real-time processing system** for order messages, using:

- Apache Kafka for messaging  
- Avro serialization  
- Python Producers & Consumers  
- Real-time running average  
- Retry logic with exponential backoff  
- Dead Letter Queue (DLQ) support  

This demonstrates a complete end-to-end data pipeline.

---

##  2. System Features

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
