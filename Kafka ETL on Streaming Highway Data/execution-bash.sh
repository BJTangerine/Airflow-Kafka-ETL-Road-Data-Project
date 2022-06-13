#!/bin/bash

# creates 'toll' Kafka topic
/home/project/kafka_2.12-2.8.0/bin/kafka-topics.sh --create --topic toll --bootstrap-server localhost:9092

# runs streaming data generator/simulator, produces messages to topic
python3 /home/project/streaming-data-generator.py

# runs streaming data consumer, extracts data from topic's messages
python3 /home/project/kafka-consumer.py