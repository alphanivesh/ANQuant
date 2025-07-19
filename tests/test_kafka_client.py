# tests/test_kafka_client.py
import pytest
import asyncio
import json
from src.anquant.py.util.config_loader import load_config
from src.anquant.py.messaging.kafka_client import KafkaClient


@pytest.mark.asyncio
async def test_kafka_client():
    config = load_config("config/config.yaml")
    kafka_client = KafkaClient(config["global"]["kafka"])

    # Subscribe to ohlcv_5min
    topics = ["ohlcv_5min"]
    kafka_client.subscribe(topics)

    # Produce a test message
    message = {
        "tradingsymbol": "RELIANCE-EQ",
        "timestamp": "2025-07-19T19:30:00+05:30",
        "open": 100.0,
        "high": 102.0,
        "low": 98.0,
        "close": 100.0,
        "volume": 3000.0,
        "market": "india"
    }
    kafka_client.produce("ohlcv_5min", "RELIANCE-EQ", message)

    # Poll for the message
    result = kafka_client.poll(timeout=2.0)
    assert result is not None, "Failed to consume message"
    assert result["topic"] == "ohlcv_5min"
    assert result["key"] == "RELIANCE-EQ"
    assert result["value"]["tradingsymbol"] == "RELIANCE-EQ"
    assert result["value"]["close"] == 100.0

    # Verify logs
    log_file = "logs/messaging/kafka_client_2025-07-19.log"
    assert os.path.exists(log_file)
    with open(log_file, "r") as f:
        log_content = f.read()
        assert "Initialized KafkaClient" in log_content
        assert "Subscribed to topics: ['ohlcv_5min']" in log_content
        assert "Delivered message to partition" in log_content

    kafka_client.close()