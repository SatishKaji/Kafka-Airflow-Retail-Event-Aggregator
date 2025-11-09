# Kafka Retail Events Producer & Consumer

This project demonstrates a Kafka-based retail event aggregator system with a Python producer and consumer. It uses Docker for running Kafka and is automated using Apache Airflow.

## Overview

- **Producer**: Generates retail events, including item views, cart additions, and order creations, and sends them to a Kafka topic.
- **Consumer**: Consumes events from Kafka, aggregates data by products and users, and outputs summaries.
- **Kafka**: Runs inside Docker for easy orchestration.
- **Airflow**: Automates the execution and scheduling of producer and consumer processes.


## Features

- KafkaProducer produces sample retail events with timestamps, user IDs, product IDs, quantities, and prices.
- KafkaConsumer aggregates event counts and total purchase amounts by product and user.
- Aggregation metrics output to the console.
- Uses Dockerized Kafka broker at `kafka:29092`.
- Python-based implementation using the `kafka-python` library.
- Consumer handles event types: `Item_Viewed`, `Added_to_Cart`, and `Order_Created`.
- Events are sent and consumed in JSON format.
- Automated orchestration with Airflow DAGs.


## Prerequisites

- Docker installed
- Python 3.7+ environment
- Kafka broker running in Docker accessible at `kafka:29092`
- Apache Airflow configured to run producer and consumer scripts


## Setup and Running

1. **Kafka Setup with Docker**

    Make sure your Kafka broker is running in Docker and accessible at the configured address.

2. **Python Dependencies**

    Install Python dependencies.

3. **Airflow Integration**

    Automate producer and consumer scripts with Airflow by creating DAGs that schedule these scripts accordingly.


## Files

- `producer.py` — Kafka producer generating retail events.
- `consumer.py` — Kafka consumer aggregating events by product and user.
- `Dockerfile` — Container image specification for running the application.
- `docker-compose.yml` — Docker Compose configuration to spin up Kafka and application services.
- `retail_event_dag.py` — Apache Airflow DAG for scheduling and automating the producer and consumer jobs.

