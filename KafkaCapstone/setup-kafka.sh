#!/bin/bash

# setup_kafka.sh

# Exit immediately if a command exits with a non-zero status
set -e

# Function to generate a UUID
generate_uuid() {
  if command -v uuidgen >/dev/null 2>&1; then
    uuidgen
  else
    # Fallback to Python if uuidgen is not available
    python3 -c 'import uuid; print(uuid.uuid4())'
  fi
}

# Generate a unique CLUSTER_ID
CLUSTER_ID=$(generate_uuid)
echo "Generated CLUSTER_ID: $CLUSTER_ID"

# Replace the placeholder CLUSTER_ID in docker-compose.yml
sed -i.bak "s/CLUSTER_ID: \"your_cluster_id_here\"/CLUSTER_ID: \"$CLUSTER_ID\"/" docker-compose.yml
echo "Updated docker-compose.yml with CLUSTER_ID."

# Start Kafka broker using Docker Compose
echo "Starting Kafka broker..."
docker compose up -d

# Wait for Kafka to initialize
echo "Waiting for Kafka to initialize..."
sleep 15  # Adjust sleep time as necessary

# Create Kafka topic 'weather'
echo "Creating Kafka topic 'weather'..."
docker exec kafka_kraft kafka-topics --create --topic weather --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 || echo "Topic 'weather' may already exist."

# List all Kafka topics to verify
echo "Listing all Kafka topics:"
docker exec kafka_kraft kafka-topics --list --bootstrap-server localhost:9092

echo "Kafka setup completed successfully."