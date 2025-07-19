# D:\AlphaNivesh\ANQuant\src\anquant\py\messaging\kafka_client.py
import json
import time
from typing import Dict, List, Optional
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from src.anquant.py.util.logging import setup_logging

logger = setup_logging("kafka_client", log_type="messaging")

class KafkaClient:
    def __init__(self, config: Dict):
        """
        Initialize Kafka producer and consumer with market-specific logging.

        Args:
            config (Dict): Kafka configuration from config.yaml (e.g., brokers, topics).
        """
        self.brokers = config.get('brokers', 'localhost:9092')
        self.topics = config.get('topics', {})

        producer_config = {
            'bootstrap.servers': self.brokers,
            'client.id': 'anquant-producer',
            'queue.buffering.max.messages': 1000000,
            'queue.buffering.max.ms': 100,
            'message.max.bytes': 1000000,
            'compression.type': 'gzip',
            'retries': 3,
            'retry.backoff.ms': 500
        }

        consumer_config = {
            'bootstrap.servers': self.brokers,
            'group.id': 'anquant',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,  # Changed to manual commits for reliability
        }

        try:
            self.producer = Producer(producer_config)
            self.consumer = Consumer(consumer_config)
            logger.info("Initialized KafkaClient", market="none", topic="none")
        except KafkaException as e:
            logger.error(f"Failed to initialize KafkaClient: {str(e)}", market="none", topic="none", exc_info=True)
            raise

    def produce(self, topic: str, key: str, value: Dict, partition: Optional[int] = None):
        """
        Produce a message to a Kafka topic with market-specific logging.

        Args:
            topic (str): Kafka topic (e.g., nse_ticks, ohlcv_5min).
            key (str): Message key (e.g., tradingsymbol like RELIANCE-EQ).
            value (Dict): Message value with market or exchange field.
            partition (Optional[int]): Specific partition (optional).
        """
        try:
            logger.debug(f"[DEBUG] About to access value.get('market') for topic={topic}, key={key}, value={value}")
            market = value.get('market', value.get('exchange', 'unknown')).lower()
            logger.debug(f"[DEBUG] market resolved: {market}")
            logger.debug(f"[DEBUG] About to access value['tradingsymbol'] for topic={topic}, key={key}, value={value}")
            _ = value['tradingsymbol']
            logger.debug(f"[DEBUG] value['tradingsymbol'] accessed successfully: {value['tradingsymbol']}")
            logger.debug(f"[DEBUG] Preparing to serialize value for topic={topic}, key={key}, value={value}")
            value_str = json.dumps(value)
            logger.debug(f"[DEBUG] Serialization complete for topic={topic}, key={key}, value_str={value_str}")
            logger.debug(f"[DEBUG] About to call producer.produce for topic={topic}, key={key}")
            if partition is not None:
                self.producer.produce(
                    topic=topic,
                    key=key,
                    value=value_str,
                    partition=partition,
                    on_delivery=lambda err, msg: self._delivery_callback(err, msg, topic, key, market)
                )
            else:
                self.producer.produce(
                    topic=topic,
                    key=key,
                    value=value_str,
                    on_delivery=lambda err, msg: self._delivery_callback(err, msg, topic, key, market)
                )
            self.producer.flush(timeout=0.1)  # Ensure timely delivery
        except Exception as e:
            logger.error(f"[KafkaClient] Exception in produce for topic={topic}, key={key}: {e}", market=market, topic=topic, exc_info=True)
            raise

    def _delivery_callback(self, err, msg, topic: str, key: str, market: str):
        """
        Handle delivery confirmation or errors for produced messages.

        Args:
            err: Error information (None if successful).
            msg: Message metadata.
            topic (str): Kafka topic.
            key (str): Message key.
            market (str): Market identifier.
        """
        try:
            if err is not None:
                logger.error(f"[KafkaClient] Delivery failed for key {key}: {str(err)}", market=market, topic=topic, exc_info=True)
            else:
                logger.info(f"[KafkaClient] Delivered message to partition {msg.partition()} with key {key}", market=market, topic=topic)
                logger.debug(f"[KafkaClient] Message details - topic: {msg.topic()}, offset: {msg.offset()}, timestamp: {msg.timestamp()}, key: {key}, value: {msg.value()}", market=market, topic=topic)
        except Exception as e:
            logger.error(f"[KafkaClient] Exception in delivery callback for topic={topic}, key={key}: {e}", market=market, topic=topic, exc_info=True)

    def subscribe(self, topics: List[str]):
        """
        Subscribe consumer to a list of topics.

        Args:
            topics (List[str]): List of topics (e.g., ['nse_ticks', 'ohlcv_5min']).
        """
        try:
            self.consumer.subscribe(topics)
            logger.info(f"Subscribed to topics: {topics}", market="none", topic=",".join(topics))
        except KafkaException as e:
            logger.error(f"Failed to subscribe to topics {topics}: {str(e)}", market="none", topic="none", exc_info=True)
            raise

    def poll(self, timeout: float = 1.0) -> Optional[Dict]:
        """
        Poll for new messages from subscribed topics.

        Args:
            timeout (float): Poll timeout in seconds.

        Returns:
            Optional[Dict]: Deserialized message or None.
        """
        try:
            start_time = time.time()
            msg = self.consumer.poll(timeout=timeout)
            if msg is None:
                logger.debug(f"No message received after {timeout}s", market="none", topic="none")
                return None
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition for {msg.topic()}", market="none", topic=msg.topic())
                    return None
                logger.error(f"Kafka error for {msg.topic()}: {msg.error()}", market="none", topic=msg.topic(), exc_info=True)
                raise KafkaException(msg.error())
            value = json.loads(msg.value().decode('utf-8'))
            market = value.get('market', value.get('exchange', 'unknown')).lower()
            logger.debug(
                f"Consumed message from partition {msg.partition()} with key {msg.key().decode('utf-8') if msg.key() else 'None'}, "
                f"size {len(msg.value())} bytes, latency {time.time() - start_time:.3f}s",
                market=market,
                topic=msg.topic()
            )
            self.consumer.commit(msg)  # Manual commit for reliability
            return {
                'topic': msg.topic(),
                'partition': msg.partition(),
                'key': msg.key().decode('utf-8') if msg.key() else None,
                'value': value
            }
        except Exception as e:
            logger.error(f"Failed to poll messages: {str(e)}", market="none", topic="none", exc_info=True)
            raise

    def close(self):
        """
        Close producer and consumer connections.
        """
        try:
            self.producer.flush(timeout=5.0)
            self.consumer.close()
            logger.info("Closed KafkaClient connections", market="none", topic="none")
        except Exception as e:
            logger.error(f"Failed to close KafkaClient: {str(e)}", market="none", topic="none", exc_info=True)
            raise