#!/bin/sh
/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1  --driver-memory 5G /realtime/stream_consumer.py