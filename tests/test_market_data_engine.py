# tests/test_market_data_engine.py
import asyncio

from anquant.py.core.adapters import AngelOneAdapter
from src.anquant.py.util.config_loader import load_config
from src.anquant.py.messaging.redis_client import RedisClient
from anquant.py.data_management.market_data import MarketDataEngine

async def test_market_data_engine():
    config = load_config("config/config.yaml")
    redis_client = RedisClient(config["global"]["redis"])
    await redis_client.connect()
    adapters = {'india': {'angelone': AngelOneAdapter(config["global"]["markets"]["india"]["brokers"]["angelone"])}}
    market_data_engine = MarketDataEngine(config, redis_client, adapters)
    await market_data_engine.initialize()
    await market_data_engine.start()
    await asyncio.sleep(5)
    await market_data_engine.stop()

if __name__ == "__main__":
    asyncio.run(test_market_data_engine())