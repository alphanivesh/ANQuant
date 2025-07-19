import pytest
import asyncio
import os
import yaml
from loguru import logger
from src.py.util.config_loader import load_config
from src.py.messaging.redis_client import RedisClient
from src.anquant.py.util.database import Database
from scripts.prefetch_historical_data import prefetch_historical_data

# Remove existing handlers and configure logging
logger.remove()
logger.add("logs/general/test_prefetch_historical_data.log", rotation="10 MB", retention="7 days", level="DEBUG",
           format="[{time:YYYY-MM-DD HH:mm:ss}] [{level}] {message}")


@pytest.mark.asyncio
async def test_prefetch_historical_data():
    config = load_config("config/config.yaml")
    config['global']['offline_mode'] = True
    await prefetch_historical_data(config)

    redis_client = RedisClient(config["global"]["redis"])
    database = Database(config["global"]["database"])
    watchlist_path = os.path.join(os.path.dirname(__file__), "../config/markets/india/watchlists/master.yaml")

    try:
        with open(watchlist_path, 'r') as f:
            watchlist = [stock['tradingsymbol'] for stock in yaml.safe_load(f)['stocks']][:10]
    except FileNotFoundError as e:
        logger.error(f"Failed to load watchlist: {e}")
        pytest.fail(f"Failed to load watchlist: {e}")

    for symbol in watchlist:
        for timeframe in config["global"]["historical_data"]["timeframes"]:
            cached = await redis_client.get(f"{symbol}:ohlcv:{timeframe}")
            assert cached is not None, f"No Redis cache for {symbol}:{timeframe}"
            assert len(cached) >= 60, f"Insufficient Redis records for {symbol}:{timeframe}"
            cursor = database.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM anquant.ohlcv WHERE tradingsymbol = %s AND timeframe = %s",
                           (symbol, timeframe))
            count = cursor.fetchone()[0]
            assert count >= 60, f"Insufficient PostgreSQL records for {symbol}:{timeframe} (found {count})"
            cursor.close()
    database.close()
    await redis_client.close()