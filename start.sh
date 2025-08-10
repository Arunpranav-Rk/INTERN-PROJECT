#!/bin/bash

echo "🔌 Starting Zookeeper..."
"$KAFKA_HOME/bin/zookeeper-server-start.sh" "$KAFKA_HOME/config/zookeeper.properties" > logs/system/zookeeper.log 2>&1 &

sleep 3

echo "📡 Starting Kafka Broker..."
"$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/server.properties" > logs/system/kafka.log 2>&1 &

sleep 3
echo "✅ Kafka and Zookeeper started successfully!"

jps
