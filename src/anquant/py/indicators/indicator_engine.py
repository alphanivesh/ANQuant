# src/anquant/py/core/indicators/indicator_engine.py
import asyncio
import pandas as pd
import pandas_ta as ta
import json
import numpy as np
from typing import Dict, Any, List, Set
from datetime import datetime, timedelta
import pytz
from src.anquant.py.util.logging import setup_logging
from src.anquant.py.messaging.kafka_client import KafkaClient
from src.anquant.py.messaging.redis_client import RedisClient
from src.anquant.py.util.config_loader import load_config
from src.anquant.py.util.database import Database

logger = setup_logging("indicator_engine", log_type="indicators")


class IndicatorEngine:
    def __init__(self, config: Dict[str, Any], redis_client: RedisClient, adapters: Dict[str, Dict[str, Any]]):
        self.config = config
        self.redis_client = redis_client
        self.adapters = adapters
        self.kafka_client = KafkaClient(config["global"]["kafka"]) if not config['global'].get('offline_mode',
                                                                                               False) else None
        self.database = Database(config["global"]["database"])
        self.db_pool = None
        self.timeframes = config["global"]["historical_data"]["timeframes"]
        self.markets = config["global"]["markets"]
        self.watchlists = {market: {name: path for name, path in market_config["watchlists"].items()}
                           for market, market_config in self.markets.items()}
        self.offline_mode = config['global'].get('offline_mode', False)
        self.indicators_config = self._load_indicators_config()
        self.ohlcv_state = {}  # symbol: {timeframe: list of dicts}
        self.indicator_state = {}  # symbol: {timeframe: {'closes': np.array}}
        logger.info(
            f"Initialized IndicatorEngine with timeframes: {self.timeframes}, markets: {list(self.markets.keys())}")

    def _load_indicators_config(self) -> List[Dict]:
        """Load indicators from strategy YAML files."""
        try:
            indicators = []
            for strategy in self.config["global"]["strategies"]:
                strategy_file = strategy.get("strategy_file")
                if not strategy_file:
                    logger.warning(f"No strategy_file defined for strategy {strategy['name']}")
                    continue
                strategy_config = load_config(strategy_file, require_global=False)
                # Support both with and without 'global' section
                if "global" in strategy_config:
                    strategy_indicators = strategy_config["global"].get("indicators", [])
                else:
                    strategy_indicators = strategy_config.get("indicators", [])
                indicators.extend(strategy_indicators)
                logger.debug(f"Loaded indicators for strategy {strategy['name']}: {json.dumps(strategy_indicators)}")
            return indicators
        except Exception as e:
            logger.error(f"Failed to load indicators config: {e}", exc_info=True)
            raise

    def _compute_indicators(self, df: pd.DataFrame, indicators_config: List[Dict]) -> Dict:
        logger.debug(f"Computing indicators for DataFrame with {len(df)} rows")
        if df.empty or 'close' not in df.columns:
            logger.warning("DataFrame is empty or missing 'close' column, skipping indicator computation.")
            return {}
        try:
            indicators = {}
            for ind in indicators_config:
                ind_type = ind['type']
                ind_name = ind['name']
                if ind_type == 'rsi':
                    rsi = ta.rsi(df['close'], length=ind.get('period', 14))
                    indicators[ind_name] = rsi.iloc[-1] if not rsi.empty and not pd.isna(rsi.iloc[-1]) else 0.0
                    logger.debug(
                        f"Computed RSI (period={ind.get('period', 14)}): {indicators[ind_name]}")
                elif ind_type == 'bollinger_bands':
                    bb = ta.bbands(df['close'], length=ind.get('period', 20), std=ind.get('std', 2.0))
                    indicators[f"{ind_name}_upper"] = bb[f'BBU_{ind["period"]}_{ind["std"]}'].iloc[
                        -1] if not bb.empty and not pd.isna(bb.iloc[-1][0]) else 0.0
                    indicators[f"{ind_name}_mid"] = bb[f'BBM_{ind["period"]}_{ind["std"]}'].iloc[
                        -1] if not bb.empty and not pd.isna(bb.iloc[-1][1]) else 0.0
                    indicators[f"{ind_name}_lower"] = bb[f'BBL_{ind["period"]}_{ind["std"]}'].iloc[
                        -1] if not bb.empty and not pd.isna(bb.iloc[-1][2]) else 0.0
                    logger.debug(
                        f"Computed Bollinger Bands (period={ind.get('period', 20)}, std={ind.get('std', 2.0)}): {json.dumps(indicators)}")
                elif ind_type == 'atr':
                    atr = ta.atr(df['high'], df['low'], df['close'], length=ind.get('period', 14))
                    indicators[ind_name] = atr.iloc[-1] if not atr.empty and not pd.isna(atr.iloc[-1]) else 0.0
                    logger.debug(
                        f"Computed ATR (period={ind.get('period', 14)}): {indicators[ind_name]}")
            return indicators
        except Exception as e:
            logger.error(f"Error computing indicators: {e}", exc_info=True)
            return {}

    async def initialize(self):
        """Initialize adapters, preload historical OHLCV, and compute indicators."""
        logger.debug("Initializing IndicatorEngine")
        try:
            # Create database pool
            import asyncpg
            self.db_pool = await asyncpg.create_pool(
                host=self.config["global"]["database"]["host"],
                port=self.config["global"]["database"]["port"],
                database=self.config["global"]["database"]["dbname"],
                user=self.config["global"]["database"]["user"],
                password=self.config["global"]["database"]["password"]
            )
            
            # Connect adapters
            tasks = []
            for market in self.adapters:
                for broker in self.adapters[market]:
                    logger.debug(f"Connecting adapter for {market}/{broker}")
                    tasks.append(self.adapters[market][broker].connect())
            await asyncio.gather(*tasks)
            logger.info("All adapters connected successfully")

            # Preload historical data (60 candles per timeframe)
            lookback = self.config['global']['historical_data']['lookback_candles']
            for market in self.watchlists:
                for watchlist_name, watchlist_path in self.watchlists[market].items():
                    symbols = await self._load_watchlist(watchlist_path)
                    for symbol in symbols:
                        for timeframe in self.timeframes:
                            logger.debug(f"Preloading historical for {symbol}:{timeframe}")
                            historical = await self.load_historical(symbol, timeframe, lookback)
                            self.ohlcv_state.setdefault(symbol, {})[timeframe] = historical
                            closes = np.array([c['close'] for c in historical if 'close' in c])
                            self.indicator_state.setdefault(symbol, {})[timeframe] = {'closes': closes}
                            indicators = self._compute_indicators(pd.DataFrame(historical), self.indicators_config)
                            await self.redis_client.cache(f"{symbol}:indicators:{timeframe}", indicators, ttl=86400)
                            logger.debug(
                                f"Preloaded and cached indicators for {symbol} ({market}, {timeframe}): {json.dumps(indicators)}")
            logger.info("IndicatorEngine initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize IndicatorEngine: {e}", exc_info=True)
            raise

    async def load_historical(self, symbol: str, timeframe: str, lookback: int) -> List[Dict]:
        """Load historical OHLCV from Redis or PostgreSQL."""
        logger.debug(f"Loading historical OHLCV for {symbol}:{timeframe} with lookback {lookback}")
        try:
            # Try Redis first
            cached = await self.redis_client.get(f"{symbol}:ohlcv:{timeframe}")
            if cached and len(cached) >= lookback:
                logger.debug(f"Retrieved {len(cached)} candles from Redis for {symbol}:{timeframe}")
                return cached

            # Fallback to PostgreSQL
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT * FROM anquant.ohlcv 
                    WHERE tradingsymbol = $1 AND timeframe = $2 
                    ORDER BY timestamp DESC LIMIT $3
                """, symbol, timeframe, lookback)
                historical = [{
                    'timestamp': r['timestamp'].isoformat(),
                    'open': r['open'],
                    'high': r['high'],
                    'low': r['low'],
                    'close': r['close'],
                    'volume': r['volume'],
                    'tradingsymbol': r['tradingsymbol'],
                    'exchange': r['exchange'],
                    'timeframe': r['timeframe']
                } for r in rows]
                await self.redis_client.cache(f"{symbol}:ohlcv:{timeframe}", historical, ttl=86400)
                logger.debug(
                    f"Retrieved {len(historical)} candles from PostgreSQL and cached in Redis for {symbol}:{timeframe}")
                return historical
        except Exception as e:
            logger.error(f"Failed to load historical OHLCV for {symbol}:{timeframe}: {e}", exc_info=True)
            raise

    async def start(self):
        """Start IndicatorEngine, backfill indicators, and process ticks."""
        logger.info("Starting IndicatorEngine")
        try:
            if self.offline_mode:
                logger.info("Offline mode: Skipping tick processing")
                return

            if self.kafka_client:
                self.kafka_client.connect()
                self.kafka_client.subscribe(["nse_ticks"])
                logger.debug("Subscribed to Kafka topic: nse_ticks")
            await self.redis_client.connect()
            await self.redis_client.redis.ping()
            logger.debug("Verified Redis connectivity")
            await self._backfill_indicators()
            asyncio.create_task(self._process_ticks())
            logger.info("IndicatorEngine started successfully")
        except Exception as e:
            logger.error(f"Failed to start IndicatorEngine: {e}", exc_info=True)
            raise

    async def _backfill_indicators(self):
        """Compute indicators for backfilled OHLCV data."""
        try:
            now = datetime.now(pytz.timezone('Asia/Kolkata'))
            for market, market_config in self.markets.items():
                open_time = datetime.strptime(market_config['open_time'], '%H:%M:%S').time()
                open_datetime = datetime.combine(now.date(), open_time, tzinfo=pytz.timezone('Asia/Kolkata'))
                if now.time() > open_time and now.date() == open_datetime.date():
                    logger.info(f"Backfilling indicators for {market} from {open_datetime} to {now}")
                    for timeframe in self.timeframes:
                        for watchlist_name, watchlist_path in market_config['watchlists'].items():
                            symbols = await self._load_watchlist(watchlist_path)
                            for symbol in symbols:
                                ohlcv = await self.redis_client.get(f"{symbol}:ohlcv:{timeframe}")
                                if ohlcv:
                                    df = pd.DataFrame(ohlcv)
                                    indicators = self._compute_indicators(df, self.indicators_config)
                                    await self.redis_client.cache(f"{symbol}:indicators:{timeframe}", indicators,
                                                                  ttl=86400)
                                    logger.debug(
                                        f"Backfilled indicators for {symbol} ({market}, {timeframe}): {json.dumps(indicators)}")
                                else:
                                    logger.warning(
                                        f"No OHLCV data found for {symbol} ({market}, {timeframe}) during backfill")
        except Exception as e:
            logger.error(f"Error backfilling indicators: {e}", exc_info=True)
            raise

    async def _process_ticks(self):
        """Process real-time ticks and compute indicators."""
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
                        ohlcv = self.aggregate_ohlcv(symbol, timeframe, tick, timestamp)
                        if ohlcv:
                            df = pd.DataFrame([ohlcv])
                            indicators = self.compute_indicators(symbol, timeframe, ohlcv['close'])
                            await self.redis_client.cache(f"{symbol}:ohlcv:{timeframe}",
                                                          self.ohlcv_state[symbol][timeframe], ttl=86400)
                            await self.redis_client.cache(f"{symbol}:indicators:{timeframe}", indicators, ttl=86400)
                            await self.database.save_ohlcv(symbol, timeframe, df)
                            if self.kafka_client:
                                self.kafka_client.produce(f"ohlcv_{timeframe}", key=symbol, value=ohlcv)
                            logger.debug(
                                f"Processed tick for {symbol} ({market}, {timeframe}): OHLCV={json.dumps(ohlcv)}, Indicators={json.dumps(indicators)}")
                else:
                    logger.debug("No tick message received, continuing polling")
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Error in tick processing loop: {e}", exc_info=True)
            raise

    def aggregate_ohlcv(self, symbol: str, timeframe: str, tick: Dict, timestamp: datetime) -> Dict:
        """Aggregate ticks into OHLCV for the specified timeframe."""
        logger.debug(f"Aggregating tick for {symbol} ({timeframe})")
        try:
            duration_minutes = {'1min': 1, '5min': 5, '15min': 15, '30min': 30, '1hr': 60}
            duration = timedelta(minutes=duration_minutes.get(timeframe, 5))
            rounded_timestamp = timestamp - (timestamp - datetime.min) % duration

            self.ohlcv_state.setdefault(symbol, {}).setdefault(timeframe, [])
            ltp = tick.get('ltp', tick.get('price', 0.0))
            volume = tick.get('volume', 1)

            timeframe_state = self.ohlcv_state[symbol][timeframe]
            if timeframe_state and timeframe_state[-1]['timestamp'] == rounded_timestamp.isoformat():
                last_ohlcv = timeframe_state[-1]
                last_ohlcv['high'] = max(last_ohlcv['high'], ltp)
                last_ohlcv['low'] = min(last_ohlcv['low'], ltp)
                last_ohlcv['close'] = ltp
                last_ohlcv['volume'] += volume
                logger.debug(f"Updated OHLCV for {symbol} ({timeframe}): {json.dumps(last_ohlcv)}")
                return last_ohlcv

            ohlcv = {
                'tradingsymbol': symbol,
                'open': ltp,
                'high': ltp,
                'low': ltp,
                'close': ltp,
                'volume': volume,
                'timestamp': rounded_timestamp.isoformat(),
                'exchange': 'NSE',
                'market': 'india'
            }
            timeframe_state.append(ohlcv)
            logger.debug(f"Created new OHLCV for {symbol} ({timeframe}): {json.dumps(ohlcv)}")
            return ohlcv
        except Exception as e:
            logger.error(f"Error aggregating OHLCV for {symbol} ({timeframe}): {e}", exc_info=True)
            return {}

    def compute_indicators(self, symbol: str, timeframe: str, close: float) -> Dict:
        """Compute indicators for the given symbol and timeframe."""
        logger.debug(f"Computing indicators for {symbol} ({timeframe})")
        try:
            self.indicator_state.setdefault(symbol, {}).setdefault(timeframe, {'closes': np.array([])})
            closes = self.indicator_state[symbol][timeframe]['closes'].tolist()
            closes.append(close)
            self.indicator_state[symbol][timeframe]['closes'] = np.array(closes[-60:])  # Keep last 60 closes
            df = pd.DataFrame({'close': closes[-60:]})

            indicators = {}
            for ind in self.indicators_config:
                ind_type = ind['type']
                ind_name = ind['name']
                if ind_type == 'rsi':
                    rsi = ta.rsi(df['close'], length=ind.get('period', 14))
                    indicators[ind_name] = rsi.iloc[-1] if not rsi.empty and not pd.isna(rsi.iloc[-1]) else 0.0
                    logger.debug(
                        f"Computed RSI for {symbol} ({timeframe}, period={ind.get('period', 14)}): {indicators[ind_name]}")
                elif ind_type == 'bollinger_bands':
                    bb = ta.bbands(df['close'], length=ind.get('period', 20), std=ind.get('std', 2.0))
                    indicators[f"{ind_name}_upper"] = bb[f'BBU_{ind["period"]}_{ind["std"]}'].iloc[
                        -1] if not bb.empty and not pd.isna(bb.iloc[-1][0]) else 0.0
                    indicators[f"{ind_name}_mid"] = bb[f'BBM_{ind["period"]}_{ind["std"]}'].iloc[
                        -1] if not bb.empty and not pd.isna(bb.iloc[-1][1]) else 0.0
                    indicators[f"{ind_name}_lower"] = bb[f'BBL_{ind["period"]}_{ind["std"]}'].iloc[
                        -1] if not bb.empty and not pd.isna(bb.iloc[-1][2]) else 0.0
                    logger.debug(
                        f"Computed Bollinger Bands for {symbol} ({timeframe}, period={ind.get('period', 20)}, std={ind.get('std', 2.0)}): {json.dumps(indicators)}")
                elif ind_type == 'atr':
                    atr = ta.atr(df['high'], df['low'], df['close'], length=ind.get('period', 14))
                    indicators[ind_name] = atr.iloc[-1] if not atr.empty and not pd.isna(atr.iloc[-1]) else 0.0
                    logger.debug(
                        f"Computed ATR for {symbol} ({timeframe}, period={ind.get('period', 14)}): {indicators[ind_name]}")
            return indicators
        except Exception as e:
            logger.error(f"Error computing indicators for {symbol} ({timeframe}): {e}", exc_info=True)
            return {}

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

    async def close(self):
        """Close IndicatorEngine connections and adapters."""
        logger.info("Closing IndicatorEngine")
        try:
            tasks = []
            for market in self.adapters:
                for broker in self.adapters[market]:
                    logger.debug(f"Disconnecting adapter for {market}/{broker}")
                    tasks.append(self.adapters[market][broker].disconnect())
            await asyncio.gather(*tasks)
            if self.kafka_client:
                self.kafka_client.close()
            await self.redis_client.close()
            if self.db_pool:
                await self.db_pool.close()
            self.database.close()
            logger.info("IndicatorEngine closed successfully")
        except Exception as e:
            logger.error(f"Error closing IndicatorEngine: {e}", exc_info=True)
            raise