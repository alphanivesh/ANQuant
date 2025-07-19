import os
from confluent_kafka.admin import AdminClient, NewTopic
from src.py.util.config_loader import load_config
from loguru import logger


def create_kafka_topics(config):
    """
    Create Kafka topics with specified partitions from config.yaml, logging topic names and partitions.

    Args:
        config (Dict): Configuration dictionary from config.yaml.
    """
    # Configure logging
    log_dir = os.path.join("logs", "kafka")
    os.makedirs(log_dir, exist_ok=True)
    logger.add(
        f"{log_dir}/create_topics.log",
        rotation="10 MB",
        retention="7 days",
        level="DEBUG",
        format="[{time:YYYY-MM-DD HH:mm:ss}] [{level}] topic={extra[topic]} {message}"
    )

    admin_client = AdminClient({'bootstrap.servers': config['global']['kafka']['brokers']})

    # Get existing topics
    try:
        existing_topics = admin_client.list_topics(timeout=10).topics
        existing_topic_info = {name: len(metadata.partitions) for name, metadata in existing_topics.items()}
        logger.debug(
            f"Retrieved existing topics: {', '.join(f'{name} ({partitions} partitions)' for name, partitions in existing_topic_info.items())}",
            topic="none")
    except Exception as e:
        logger.error(f"Failed to list existing topics: {e}", topic="none", exc_info=True)
        raise

    # Define topics to create
    topics = [
        {
            'name': config['global']['kafka']['topics']['india'],
            'partitions': config['global']['kafka']['nse_ticks_partitions'],
            'partition_key': 'nse_ticks_partitions'
        },
        {
            'name': config['global']['kafka']['topics']['ohlcv_1min'],
            'partitions': config['global']['kafka']['ohlcv_1min_partitions'],
            'partition_key': 'ohlcv_1min_partitions'
        },
        {
            'name': config['global']['kafka']['topics']['ohlcv_5min'],
            'partitions': config['global']['kafka']['ohlcv_5min_partitions'],
            'partition_key': 'ohlcv_5min_partitions'
        },
        {
            'name': config['global']['kafka']['topics']['ohlcv_30min'],
            'partitions': config['global']['kafka']['ohlcv_30min_partitions'],
            'partition_key': 'ohlcv_30min_partitions'
        },
        {
            'name': config['global']['kafka']['topics']['signals'],
            'partitions': config['global']['kafka']['signals_partitions'],
            'partition_key': 'signals_partitions'
        },
        {
            'name': config['global']['kafka']['topics']['trades'],
            'partitions': config['global']['kafka']['trades_partitions'],
            'partition_key': 'trades_partitions'
        },
    ]

    # Filter out topics with correct partition count
    new_topics = []
    for topic in topics:
        topic_name = topic['name']
        expected_partitions = topic['partitions']
        if topic_name in existing_topic_info:
            current_partitions = existing_topic_info[topic_name]
            if current_partitions == expected_partitions:
                logger.info(f"Topic {topic_name} already exists with correct {current_partitions} partitions",
                            topic=topic_name)
                continue
            else:
                logger.warning(
                    f"Topic {topic_name} exists with {current_partitions} partitions, expected {expected_partitions}. Cannot modify partitions.",
                    topic=topic_name)
                continue
        new_topics.append(NewTopic(topic_name, num_partitions=expected_partitions, replication_factor=1))

    if not new_topics:
        logger.info(f"No new topics to create; all topics exist: {', '.join(existing_topic_info.keys())}", topic="none")
        return

    try:
        futures = admin_client.create_topics(new_topics)
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(
                    f"Created topic {topic} with {config['global']['kafka'].get(f'{topic}_partitions', 4)} partitions",
                    topic=topic)
            except Exception as e:
                if "Topic already exists" in str(e):
                    logger.warning(f"Topic {topic} already exists", topic=topic)
                else:
                    logger.error(f"Failed to create topic {topic}: {e}", topic=topic, exc_info=True)
                    # Continue to next topic
    except Exception as e:
        logger.error(f"Failed to initiate topic creation: {e}", topic="none", exc_info=True)
        # Continue instead of raising


if __name__ == "__main__":
    try:
        config = load_config("config/config.yaml")
        create_kafka_topics(config)
    except Exception as e:
        logger.error(f"Script execution failed: {e}", topic="none", exc_info=True)
        raise