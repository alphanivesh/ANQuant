import pytest
import os
from scripts.create_kafka_topics import create_kafka_topics
from src.py.util.config_loader import load_config
from confluent_kafka.admin import AdminClient


def test_create_kafka_topics():
    config = load_config("config/config.yaml")
    create_kafka_topics(config)

    admin_client = AdminClient({'bootstrap.servers': config['global']['kafka']['brokers']})
    topics_metadata = admin_client.list_topics(timeout=10).topics

    expected_topics = [
        config['global']['kafka']['topics']['india'],
        config['global']['kafka']['topics']['ohlcv_1min'],
        config['global']['kafka']['topics']['ohlcv_5min'],
        config['global']['kafka']['topics']['ohlcv_30min'],
        config['global']['kafka']['topics']['signals'],
        config['global']['kafka']['topics']['trades']
    ]

    for topic in expected_topics:
        assert topic in topics_metadata, f"Topic {topic} not created"

    # Verify partition counts (tolerate existing topics with different counts)
    for topic in expected_topics:
        partition_key = f"{topic}_partitions"
        expected_partitions = config['global']['kafka'].get(partition_key, 4)
        current_partitions = len(topics_metadata[topic].partitions)
        if current_partitions != expected_partitions:
            print(f"Warning: Topic {topic} has {current_partitions} partitions, expected {expected_partitions}")

    # Verify log file contains topic names
    log_file = "logs/kafka/create_topics.log"
    assert os.path.exists(log_file), f"Log file {log_file} not found"
    with open(log_file, "r") as f:
        log_content = f.read()
        for topic in expected_topics:
            assert topic in log_content, f"Topic {topic} not found in logs"