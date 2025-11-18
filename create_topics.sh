#!/usr/bin/env bash
# run this from host: docker exec -it kafka bash -c "/path/in/container/create_topics.sh"
BOOTSTRAP=localhost:29092
# create orders topic
docker exec -it $(docker ps --filter "ancestor=confluentinc/cp-kafka:7.3.2" --format "{{.Names}}" | head -n1) bash -c "\
  kafka-topics --bootstrap-server $BOOTSTRAP --create --topic orders --partitions 3 --replication-factor 1 || true"

# create DLQ
docker exec -it $(docker ps --filter "ancestor=confluentinc/cp-kafka:7.3.2" --format "{{.Names}}" | head -n1) bash -c "\
  kafka-topics --bootstrap-server $BOOTSTRAP --create --topic orders-dlq --partitions 3 --replication-factor 1 || true"

echo "Created topics 'orders' and 'orders-dlq' (if they didn't already exist)."
