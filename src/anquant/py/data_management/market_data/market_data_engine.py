# src/anquant/py/core/market_data/market_data_engine.py
import asyncio
import pandas as pd
import json
from typing import Dict, Any, Set
from datetime import datetime, time
import pytz
import asyncpg
from src.anquant.py.util.logging import setup_logging
from src.anquant.py.messaging.kafka_client import KafkaClient
from src.anquant.py.messaging.redis_client import RedisClient
from src.anquant.py.util.config_loader import load_config, load_credentials, load_symbol_mappings

logger = setup_logging("market_data_engine", log_type="market_data")

class MarketDataEngine:
    def __init__(self, config: Dict[str, Any], redis_client: RedisClient, adapters: Dict, watchlists: Dict):
        self.config = config
        self.redis_client = redis_client
        self.adapters = adapters
        self.watchlists = watchlists
        self.timeframes = config["global"]["historical_data"]["timeframes"]
        self.kafka_client = KafkaClient(config["global"]["kafka"]) if not config['global'].get('offline_mode', False) else None
        self.markets = config["global"]["markets"]
        self.offline_mode = config['global'].get('offline_mode', False)
        self.db_pool = None
        logger.info(f"Initialized MarketDataEngine with timeframes: {self.timeframes}, markets: {list(self.adapters.keys())}, adapters: {list(self.adapters.keys())}")

    async def initialize(self):
        """Initialize Kafka, PostgreSQL, Redis, and adapters."""
        logger.info("Initializing MarketDataEngine")
        try:
            if not self.offline_mode:
                self.kafka_client.subscribe(["nse_ticks"])
                logger.debug("Kafka client connected and subscribed to topic: nse_ticks")
            self.db_pool = await asyncpg.create_pool(
                host=self.config["global"]["database"]["host"],
                port=self.config["global"]["database"]["port"],
                database=self.config["global"]["database"]["dbname"],
                user=self.config["global"]["database"]["user"],
                password=self.config["global"]["database"]["password"]
            )
            await self.redis_client.connect()
            await self.redis_client.redis.ping()
            logger.debug("Verified Redis and PostgreSQL connectivity")
            tasks = []
            for market in self.adapters:
                for broker in self.adapters[market]:
                    logger.debug(f"Connecting adapter for {market}/{broker}")
                    tasks.append(self.adapters[market][broker].connect())
            await asyncio.gather(*tasks)
            logger.info("All adapters connected successfully")
            logger.info("MarketDataEngine initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize MarketDataEngine: {e}", exc_info=True)
            raise

    async def start(self):
        """Start MarketDataEngine, subscribe to ticks, backfill data, and process ticks."""
        logger.info("Starting MarketDataEngine")
        try:
            if self.offline_mode:
                logger.info("Offline mode: Skipping subscriptions and tick processing")
                return
            tasks = []
            for market in self.adapters:
                for watchlist_name, watchlist_path in self.watchlists[market].items():
                    symbols = await self._load_watchlist(watchlist_path)
                    for broker in self.adapters[market]:
                        logger.debug(f"Subscribing to {market}/{broker} with {len(symbols)} symbols")
                        tasks.append(self.adapters[market][broker].subscribe_to_ticks(symbols))
            await asyncio.gather(*tasks)
            await self._backfill_missing_data()
            asyncio.create_task(self._process_ticks())
            logger.info("MarketDataEngine started successfully")
        except Exception as e:
            logger.error(f"Failed to start MarketDataEngine: {e}", exc_info=True)
            raise

    async def _backfill_missing_data(self):
        """Fetch missing OHLCV data for the current session."""
        try:
            now = datetime.now(pytz.timezone('Asia/Kolkata'))
            async with self.db_pool.acquire() as conn:
                for market, market_config in self.markets.items():
                    open_time = datetime.strptime(market_config['open_time'], '%H:%M:%S').time()
                    open_datetime = datetime.combine(now.date(), open_time, tzinfo=pytz.timezone('Asia/Kolkata'))
                    if now.time() > open_time and now.date() == open_datetime.date():
                        logger.info(f"Backfilling missing data for {market} from {open_datetime} to {now}")
                        for timeframe in self.timeframes:
                            interval_map = {'1min': '1T', '5min': '5T', '15min': '15T', '30min': '30T', '1hr': '1H'}
                            interval = interval_map.get(timeframe, '5T')
                            num_candles = int((now - open_datetime).total_seconds() / pd.Timedelta(interval).total_seconds())
                            for watchlist_name, watchlist_path in market_config['watchlists'].items():
                                symbols = await self._load_watchlist(watchlist_path)
                                for symbol in symbols:
                                    redis_key = f"{symbol}:ohlcv:{timeframe}"
                                    existing_data = await self.redis_client.get(redis_key)
                                    if existing_data and len(existing_data) >= num_candles:
                                        logger.debug(f"Skipping backfill for {symbol} ({market}, {timeframe}): sufficient data in Redis")
                                        continue
                                    ohlcv = await self._fetch_historical_ohlcv(symbol, timeframe, open_datetime, now, market)
                                    if not ohlcv.empty:
                                        await self.redis_client.cache(redis_key, ohlcv.to_dict('records'), ttl=86400)
                                        async with conn.transaction():
                                            for _, row in ohlcv.iterrows():
                                                await conn.execute(
                                                    """
                                                    INSERT INTO anquant.ohlcv (timestamp, open, high, low, close, volume, tradingsymbol, exchange, timeframe, market)
                                                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                                                    ON CONFLICT (timestamp, tradingsymbol, timeframe) DO UPDATE
                                                    SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                                                        close = EXCLUDED.close, volume = EXCLUDED.volume, exchange = EXCLUDED.exchange,
                                                        market = EXCLUDED.market
                                                    """,
                                                    pd.to_datetime(row['timestamp']), row['open'], row['high'], row['low'],
                                                    row['close'], row['volume'], symbol, 'NSE', timeframe, market
                                                )
                                        for _, row in ohlcv.iterrows():
                                            row_dict = row.to_dict()
                                            row_dict['market'] = market
                                            if self.kafka_client:
                                                self.kafka_client.produce(f"ohlcv_{timeframe}", key=symbol, value=row_dict)
                                        logger.debug(f"Backfilled OHLCV for {symbol} ({market}, {timeframe}): {len(ohlcv)} candles")
        except Exception as e:
            logger.error(f"Error backfilling missing data: {e}", exc_info=True)
            raise

    async def _fetch_historical_ohlcv(self, symbol: str, timeframe: str, start: datetime, end: datetime, market: str) -> pd.DataFrame:
        """Fetch historical OHLCV from broker adapters."""
        logger.debug(f"Fetching historical OHLCV for {symbol} ({timeframe}, {market}) from {start} to {end}")
        try:
            for market_name, adapters in self.adapters.items():
                if market_name == market:
                    for broker, adapter in adapters.items():
                        ohlcv = await adapter.fetch_historical_ohlcv(symbol, timeframe, start, end)
                        logger.debug(f"Fetched {len(ohlcv)} candles for {symbol} ({timeframe}, {market}) from {broker}")
                        return ohlcv
            logger.warning(f"No adapter found for market {market}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching historical OHLCV for {symbol} ({timeframe}, {market}): {e}", exc_info=True)
            return pd.DataFrame()

    async def _process_ticks(self):
        """Process real-time ticks and aggregate into OHLCV."""
        logger.info("Starting tick processing loop")
        try:
            while True:
                msg = self.kafka_client.poll(timeout=1.0)
                if msg:
                    tick = msg['value']
                    symbol = tick['tradingsymbol']
                    market = tick.get('market', 'india')
                    timestamp = datetime.fromisoformat(tick['timestamp'].replace('Z', '+00:00'))
                    for timeframe in self.timeframes:
                        ohlcv = await self._aggregate_ohlcv(tick, timeframe)
                        if not ohlcv.empty:
                            ohlcv_dict = ohlcv.iloc[-1].to_dict()
                            ohlcv_dict['market'] = market
                            await self.redis_client.cache(f"{symbol}:ohlcv:{timeframe}", ohlcv.to_dict('records'), ttl=86400)
                            async with self.db_pool.acquire() as conn:
                                async with conn.transaction():
                                    await conn.execute(
                                        """
                                        INSERT INTO anquant.ohlcv (timestamp, open, high, low, close, volume, tradingsymbol, exchange, timeframe, market)
                                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                                        ON CONFLICT (timestamp, tradingsymbol, timeframe) DO UPDATE
                                        SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                                            close = EXCLUDED.close, volume = EXCLUDED.volume, exchange = EXCLUDED.exchange,
                                            market = EXCLUDED.market
                                        """,
                                        pd.to_datetime(ohlcv_dict['timestamp']), ohlcv_dict['open'], ohlcv_dict['high'], ohlcv_dict['low'],
                                        ohlcv_dict['close'], ohlcv_dict['volume'], symbol, 'NSE', timeframe, market
                                    )
                            if self.kafka_client:
                                self.kafka_client.produce(f"ohlcv_{timeframe}", key=symbol, value=ohlcv_dict)
                            logger.debug(f"Aggregated and cached OHLCV for {symbol} ({market}, {timeframe}): {json.dumps(ohlcv_dict)}")
                else:
                    logger.debug("No tick message received, continuing polling")
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Error in tick processing loop: {e}", exc_info=True)
            raise

    async def _aggregate_ohlcv(self, tick: Dict, timeframe: str) -> pd.DataFrame:
        """Aggregate ticks into OHLCV for the specified timeframe."""
        logger.debug(f"Aggregating tick for {tick['tradingsymbol']} ({timeframe})")
        try:
            interval_map = {'1min': '1T', '5min': '5T', '15min': '15T', '30min': '30T', '1hr': '1H'}
            interval = interval_map.get(timeframe, '5T')
            df = pd.DataFrame([{
                'timestamp': pd.to_datetime(tick['timestamp']),
                'open': tick.get('price', tick.get('ltp', 0.0)),
                'high': tick.get('price', tick.get('ltp', 0.0)),
                'low': tick.get('price', tick.get('ltp', 0.0)),
                'close': tick.get('price', tick.get('ltp', 0.0)),
                'volume': tick.get('volume', 0.0),
                'tradingsymbol': tick['tradingsymbol']
            }])
            df = df.set_index('timestamp').resample(interval).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'tradingsymbol': 'last'
            }).dropna().reset_index()
            logger.debug(f"Aggregated OHLCV for {tick['tradingsymbol']} ({timeframe}): {df.iloc[-1].to_dict()}")
            return df
        except Exception as e:
            logger.error(f"Error aggregating OHLCV for {tick['tradingsymbol']} ({timeframe}): {e}", exc_info=True)
            return pd.DataFrame()

    async def _load_watchlist(self, watchlist_path: str) -> Set[str]:
        """Load watchlist symbols from YAML file."""
        logger.debug(f"Loading watchlist: {watchlist_path}")
        try:
            import yaml
            import os
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../"))
            absolute_path = os.path.join(project_root, watchlist_path)
            with open(absolute_path, 'r', encoding='utf-8') as f:
                watchlist_config = yaml.safe_load(f)
            if not watchlist_config or 'stocks' not in watchlist_config:
                logger.warning(f"Empty or invalid watchlist: {watchlist_path}")
                return set()
            symbols = {item['tradingsymbol'] for item in watchlist_config['stocks']}
            logger.debug(f"Loaded watchlist with {len(symbols)} symbols")
            return symbols
        except Exception as e:
            logger.error(f"Failed to load watchlist {watchlist_path}: {e}", exc_info=True)
            return set()

    async def stop(self):
        """Stop MarketDataEngine, disconnect adapters, and close connections."""
        logger.info("Stopping MarketDataEngine")
        try:
            tasks = []
            for market in self.adapters:
                for broker in self.adapters[market]:
                    logger.debug(f"Disconnecting adapter for {market}/{broker}")
                    tasks.append(self.adapters[market][broker].disconnect())
            await asyncio.gather(*tasks)
            if self.kafka_client:
                self.kafka_client.close()
            if self.db_pool:
                await self.db_pool.close()
            await self.redis_client.close()
            logger.info("MarketDataEngine stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping MarketDataEngine: {e}", exc_info=True)
            raise