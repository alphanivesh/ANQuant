# src/anquant/py/core/flexirule/strategy_engine.py
import asyncio
import json
from typing import Dict, Any, List
import os
import yaml
from src.anquant.py.util.logging import setup_logging
from src.anquant.py.messaging.kafka_client import KafkaClient
from src.anquant.py.messaging.redis_client import RedisClient
from src.anquant.py.core.flexirule.validator import StrategyValidator
from src.anquant.py.core.flexirule.rule_engine import RuleEngine

logger = setup_logging("strategy_manager", log_type="strategy")

class StrategyManager:
    def __init__(self, config: Dict[str, Any], redis_client: RedisClient):
        self.config = config
        self.redis_client = redis_client
        self.kafka_client = KafkaClient(config["global"]["kafka"]) if not config['global'].get('offline_mode', False) else None
        self.validator = StrategyValidator(config)
        self.strategies = self._load_strategy_configs()
        self.rule_engines = {strategy['name']: RuleEngine(strategy, redis_client) for strategy in self.strategies}
        logger.debug(f"Initialized StrategyManager with {len(self.strategies)} strategies")

    def _load_strategy_configs(self) -> List[Dict[str, Any]]:
        """Load all strategy YAML files from config/markets/india/strategies/."""
        strategies = []
        strategy_dir = os.path.join(os.path.dirname(__file__), "../../../../config/markets/india/strategies")
        try:
            for strategy_file in os.listdir(strategy_dir):
                if strategy_file.endswith(('.yaml', '.yml')):
                    strategy_path = os.path.join(strategy_dir, strategy_file)
                    if self.validator.validate_strategy(strategy_path):
                        with open(strategy_path, 'r', encoding='utf-8') as f:
                            strategy_config = yaml.safe_load(f)
                            strategies.append(strategy_config)
                            scheduler.debug(f"Loaded strategy config: {strategy_file}")
                    else:
                        logger.warning(f"Skipping invalid strategy config: {strategy_file}")
        except Exception as e:
            logger.error(f"Failed to load strategy configs from {strategy_dir}: {e}", exc_info=True)
            raise
        return strategies

    async def initialize(self):
        """Initialize Kafka and Redis connections, and RuleEngines."""
        logger.info("Initializing StrategyManager")
        try:
            if self.kafka_client:
                self.kafka_client.connect()
                topics = list(set(f"ohlcv_{strategy['timeframe']}" for strategy in self.strategies))
                self.kafka_client.subscribe(topics)
                logger.debug(f"Subscribed to Kafka topics: {topics}")
            await self.redis_client.connect()
            await self.redis_client.redis.ping()
            logger.debug("Verified Redis connectivity")
            for name, rule_engine in self.rule_engines.items():
                await rule_engine.initialize()
                logger.debug(f"Initialized RuleEngine for strategy {name}")
            logger.info("StrategyManager initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize StrategyManager: {e}", exc_info=True)
            raise

    async def start(self):
        """Start processing OHLCV messages."""
        logger.info("Starting StrategyManager")
        try:
            if not self.config['global'].get('offline_mode', False):
                asyncio.create_task(self._process_messages())
            logger.info("StrategyManager started successfully")
        except Exception as e:
            logger.error(f"Failed to start StrategyManager: {e}", exc_info=True)
            raise

    async def _process_messages(self):
        """Process OHLCV messages from Kafka and evaluate strategies."""
        try:
            while True:
                msg = self.kafka_client.poll(timeout=1.0)
                if msg:
                    ohlcv = msg['value']
                    symbol = ohlcv['tradingsymbol']
                    topic = msg['topic']
                    timeframe = topic.replace('ohlcv_', '')
                    logger.debug(f"Processing OHLCV for {symbol} on {timeframe} timeframe")
                    await self._process_strategy_signals(symbol, timeframe, ohlcv)
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Error in strategy message processing loop: {e}", exc_info=True)
            raise

    async def _process_strategy_signals(self, symbol: str, timeframe: str, ohlcv: Dict):
        """Evaluate strategies for a given symbol and timeframe."""
        try:
            indicators_data = await self.redis_client.get(f"{symbol}:indicators:{timeframe}")
            indicators = indicators_data if isinstance(indicators_data, dict) else indicators_data[0] if indicators_data else {}
            for strategy in self.strategies:
                if strategy['timeframe'] == timeframe:
                    rule_engine = self.rule_engines[strategy['name']]
                    signal = await rule_engine.evaluate(symbol, ohlcv, indicators)
                    if signal and signal != "HOLD":
                        signal_data = {
                            'symbol': symbol,
                            'signal': signal,
                            'timestamp': ohlcv['timestamp'],
                            'price': ohlcv['close'],
                            'strategy': strategy['name'],
                            'market': ohlcv.get('market', 'india')
                        }
                        await self.redis_client.publish(f"signals:{strategy['name']}", json.dumps(signal_data))
                        if self.kafka_client:
                            self.kafka_client.produce("signals", key=symbol, value=signal_data)
                        logger.info(f"Generated signal for {symbol}: {signal} (strategy: {strategy['name']})")
        except Exception as e:
            logger.error(f"Error processing strategy signals for {symbol}:{timeframe}: {e}", exc_info=True)

    async def close(self):
        """Close StrategyManager connections."""
        logger.info("Closing StrategyManager")
        try:
            if self.kafka_client:
                self.kafka_client.close()
            await self.redis_client.close()
            for name, rule_engine in self.rule_engines.items():
                await rule_engine.close()
            logger.info("StrategyManager closed successfully")
        except Exception as e:
            logger.error(f"Error closing StrategyManager: {e}", exc_info=True)
            raise