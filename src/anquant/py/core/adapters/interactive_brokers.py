# src/py/core/adapters/interactive_brokers.py
import json
from .base_adapter import BaseAdapter
from typing import List, Dict, Any
import datetime
from loguru import logger
from confluent_kafka import Producer
from src.py.messaging.redis_client import RedisClient

class InteractiveBrokersAdapter(BaseAdapter):
    def __init__(self, credentials: Dict[str, str], mappings: Dict[str, str], kafka_config: Dict[str, str], config: Dict[str, Any]):
        self.credentials = credentials  # {'account_id': ..., 'api_key': ..., 'secret_key': ..., 'host': ..., 'port': ..., 'client_id': ...}
        self.mappings = mappings  # {'AAPL': '265598', ...}
        self.offline_mode = config.get('global', {}).get('offline_mode', False)
        if not self.offline_mode:
            kafka_producer_config = {'bootstrap.servers': kafka_config.get('brokers', 'localhost:9092')}
            self.kafka_producer = Producer(kafka_producer_config)
        else:
            self.kafka_producer = None
        self.redis_client = RedisClient(config.get('redis', {})) if not self.offline_mode else None
        logger.debug(f"Initialized InteractiveBrokersAdapter with mappings for {len(self.mappings)} symbols")

    async def connect(self) -> None:
        logger.info("Connecting to Interactive Brokers (stub)")
        if self.offline_mode:
            logger.debug("Skipping connection in offline mode")

    async def subscribe_to_ticks(self, watchlist: List[str]) -> None:
        if self.offline_mode:
            logger.debug("Skipping tick subscription in offline mode")
            return
        async def callback(tick):
            try:
                if self.redis_client:
                    await self.redis_client.publish(f"ticks:{tick['exchange']}:{tick['tradingsymbol']}", json.dumps(tick))
                if self.kafka_producer:
                    self.kafka_producer.produce('us_ticks', key=tick['tradingsymbol'], value=json.dumps(tick))
                    self.kafka_producer.flush()
                logger.debug(f"Published tick for {tick['tradingsymbol']}")
            except Exception as e:
                logger.error(f"Failed to publish tick: {e}")
        logger.info(f"Subscribing to ticks for {watchlist} (stub)")
        # Implement IB tick subscription using ib_insync or IB API

    async def unsubscribe_from_ticks(self, watchlist: List[str]) -> None:
        logger.info(f"Unsubscribing from ticks for {watchlist} (stub)")

    async def place_order(self, order_details: Dict[str, Any]) -> str:
        symbol = order_details.get('tradingsymbol')
        symboltoken = self.mappings.get(symbol)
        if not symboltoken:
            logger.error(f"No symboltoken for {symbol} in mappings")
            raise ValueError(f"No symboltoken for {symbol}")
        logger.info(f"Placing order {order_details} (stub)")
        return "mock_order_id" if self.offline_mode else ""

    async def cancel_order(self, order_id: str) -> None:
        logger.info(f"Cancelling order {order_id} (stub)")

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        logger.info(f"Getting order status for {order_id} (stub)")
        return {}

    async def fetch_historical_data(self, symbol: str, timeframe: str, from_date: datetime.datetime, to_date: datetime.datetime) -> List[Dict[str, Any]]:
        symboltoken = self.mappings.get(symbol)
        if not symboltoken:
            logger.error(f"No symboltoken for {symbol} in mappings")
            raise ValueError(f"No symboltoken for {symbol}")
        logger.info(f"Fetching historical data for {symbol} (stub)")
        return [] if not self.offline_mode else [
            {
                "timestamp": (datetime.datetime.now() - datetime.timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S"),
                "open": 100.0 - i,
                "high": 100.0 - i + 5,
                "low": 100.0 - i - 5,
                "close": 100.0 - i,
                "volume": 1000,
                "tradingsymbol": symbol,
                "symboltoken": symboltoken,
                "exchange": "NASDAQ"
            } for i in range(20)
        ]

    async def get_positions(self) -> List[Dict[str, Any]]:
        logger.info("Getting positions (stub)")
        return []

    async def get_account_info(self) -> Dict[str, Any]:
        logger.info("Getting account info (stub)")
        return {}

    async def disconnect(self) -> None:
        logger.info("Disconnecting from Interactive Brokers (stub)")
        if self.redis_client:
            await self.redis_client.close()

    async def get_quote(self, symbol: str, max_retries: int = 3, retry_delay: int = 5) -> Dict[str, Any]:
        symboltoken = self.mappings.get(symbol)
        if not symboltoken:
            logger.error(f"No symboltoken for {symbol} in mappings")
            raise ValueError(f"No symboltoken for {symbol}")
        logger.info(f"Getting quote for {symbol} (stub)")
        return {"status": True, "data": {"high": 105.0, "low": 95.0, "close": 100.0}} if self.offline_mode else {}