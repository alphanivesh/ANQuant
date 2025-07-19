from loguru import logger
from src.py.util.logging_config import configure_logging
import os
import yaml
import asyncio
import pandas as pd
import datetime
from src.py.util.config_loader import load_config
from src.py.messaging.redis_client import RedisClient
from src.anquant.py.util.database import Database
from src.py.core.adapters.angelone import AngelOneAdapter

# Configure global logging
configure_logging()


async def fetch_ohlcv(symbol, timeframe, lookback, adapter):
    """Fetch OHLCV data via Angel One API or return mock data in offline mode."""
    logger.debug(f"Fetching OHLCV for {symbol}:{timeframe}")
    if adapter.offline_mode:
        return pd.DataFrame([
            {
                "timestamp": (datetime.datetime.now() - datetime.timedelta(
                    minutes=i * (1 if timeframe == "1min" else 5 if timeframe == "5min" else 30))).strftime(
                    "%Y-%m-%d %H:%M:%S"),
                "open": 100.0 - i,
                "high": 100.0 - i + 5,
                "low": 100.0 - i - 5,
                "close": 100.0 - i,
                "volume": 1000,
                "tradingsymbol": symbol,
                "symboltoken": adapter.mappings.get(symbol, "unknown"),
                "exchange": "NSE"
            } for i in range(lookback)
        ])
    from_date = datetime.datetime.now() - datetime.timedelta(days=7)
    to_date = datetime.datetime.now()
    return await adapter.fetch_historical_data(symbol, timeframe, from_date, to_date)


async def prefetch_historical_data(config):
    """Prefetch OHLCV data for stocks in master.yaml and store in Redis and PostgreSQL."""
    redis_client = RedisClient(config["global"]["redis"])
    database = Database(config["global"]["database"])

    # Resolve watchlist path
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    watchlist_path = os.path.join(project_root, config["global"]["markets"]["india"]["watchlists"]["master"])
    logger.debug(f"Attempting to load watchlist from: {watchlist_path}")

    try:
        with open(watchlist_path, 'r') as f:
            watchlist = [stock['tradingsymbol'] for stock in yaml.safe_load(f)['stocks']]
    except FileNotFoundError as e:
        logger.error(f"Failed to load watchlist from {watchlist_path}: {e}")
        raise

    # Initialize Angel One adapter
    try:
        credentials_path = config["global"]["markets"]["india"]["brokers"]["angelone"]["credentials"]
        mappings_path = config["global"]["markets"]["india"]["brokers"]["angelone"]["symbols"]
        credentials = load_config(os.path.join(project_root, credentials_path))
        mappings = load_config(os.path.join(project_root, mappings_path))
        adapter = AngelOneAdapter(credentials, mappings, config["global"]["kafka"], config)
    except Exception as e:
        logger.error(f"Failed to initialize AngelOneAdapter: {e}")
        raise

    for symbol in watchlist[:10]:  # Limit to 10 for testing
        for timeframe in config["global"]["historical_data"]["timeframes"]:
            try:
                ohlcv = await fetch_ohlcv(symbol, timeframe, config["global"]["historical_data"]["lookback_candles"],
                                          adapter)
                df = pd.DataFrame(ohlcv)
                await redis_client.cache(f"{symbol}:ohlcv:{timeframe}", df.to_dict('records'), ttl=86400)
                database.save_ohlcv(symbol, timeframe, df)
                logger.info(f"Prefetched OHLCV for {symbol}:{timeframe}")
            except Exception as e:
                logger.error(f"Failed to prefetch OHLCV for {symbol}:{timeframe}: {e}")
                continue  # Continue with next symbol/timeframe
    database.close()
    await redis_client.close()


if __name__ == "__main__":
    logger.debug(f"Loguru handlers before run: {logger._core.handlers}")
    config = load_config("config/config.yaml")
    config['global']['offline_mode'] = True
    asyncio.run(prefetch_historical_data(config))