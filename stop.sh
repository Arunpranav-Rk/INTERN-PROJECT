#!/bin/bash

echo "🛑 Stopping Kafka Broker..."
"$KAFKA_HOME/bin/kafka-server-stop.sh"

sleep 2

echo "🛑 Stopping Zookeeper..."
"$KAFKA_HOME/bin/zookeeper-server-stop.sh"

sleep 2

echo "✅ Kafka and Zookeeper stopped cleanly."

jps
