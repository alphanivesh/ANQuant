# src/py/core/strategy/strategy_engine.py
import asyncio
from typing import Dict, Any
from src.anquant.py.messaging.kafka_client import KafkaClient
from src.anquant.py.messaging.redis_client import RedisClient
from src.anquant.py.util.logging import setup_logging
import json

logger = setup_logging("strategy", log_type="strategy")


class StrategyEngine:
    def __init__(self, config: Dict[str, Any], redis_client: RedisClient):
        self.config = config
        self.redis_client = redis_client
        self.kafka_client = KafkaClient(config["global"]["kafka"])
        self.strategies = config["global"]["strategies"]
        logger.debug(f"Initialized StrategyEngine with {len(self.strategies)} strategies")

    async def initialize(self):
        logger.debug("StrategyEngine initializing")
        try:
            topics = [f"ohlcv_{strategy['timeframe']}" for strategy in self.strategies]
            self.kafka_client.subscribe(topics)
            logger.info(f"StrategyEngine subscribed to Kafka topics: {topics}")
            await self.redis_client.redis.ping()
            logger.debug("Verified Redis connectivity for StrategyEngine")
        except Exception as e:
            logger.error(f"Failed to initialize StrategyEngine: {e}")
            raise

    async def start(self):
        logger.debug("StrategyEngine starting")
        # Start the message processing loop as a background task
        asyncio.create_task(self._process_messages())
        logger.info("StrategyEngine started")

    async def _process_messages(self):
        """Background task to process OHLCV messages and generate strategy signals."""
        try:
            while True:
                msg = self.kafka_client.poll(timeout=1.0)
                if msg:
                    ohlcv = msg['value']  # Already deserialized by KafkaClient
                    symbol = ohlcv['tradingsymbol']
                    topic = msg['topic']

                    # Extract timeframe from topic (e.g., "ohlcv_1min" -> "1min")
                    timeframe = topic.replace('ohlcv_', '')

                    logger.debug(f"Processing OHLCV for {symbol} on {timeframe} timeframe")

                    # Process strategy signals
                    await self._process_strategy_signals(symbol, timeframe, ohlcv)
                else:
                    # No message received, continue polling
                    await asyncio.sleep(0.1)  # Small delay to prevent busy waiting
        except Exception as e:
            logger.error(f"Error in strategy message processing loop: {e}", exc_info=True)
            raise

    async def _process_strategy_signals(self, symbol: str, timeframe: str, ohlcv: Dict):
        """Process strategy signals for a given symbol and timeframe."""
        try:
            # Get indicators from Redis
            indicators_data = await self.redis_client.get(f"{symbol}:indicators:{timeframe}")
            if not indicators_data:
                logger.debug(f"No indicators available for {symbol}:{timeframe}")
                return

            # Convert to dict if it's a list
            indicators = indicators_data if isinstance(indicators_data, dict) else indicators_data[0] if indicators_data else {}

            # Apply strategy logic
            for strategy in self.strategies:
                if strategy['timeframe'] == timeframe:
                    signal = await self._apply_strategy(strategy, symbol, ohlcv, indicators)
                    if signal:
                        logger.info(f"Strategy signal for {symbol}: {signal}")
                        # Store signal in Redis for order execution
                        signal_data = {
                            'symbol': symbol,
                            'signal': signal,
                            'timestamp': ohlcv['timestamp'],
                            'price': ohlcv['close']
                        }
                        await self.redis_client.publish(f"signals:{strategy['name']}", json.dumps(signal_data))
        except Exception as e:
            logger.error(f"Error processing strategy signals for {symbol}:{timeframe}: {e}", exc_info=True)

    async def _apply_strategy(self, strategy: Dict, symbol: str, ohlcv: Dict, indicators: Dict) -> str:
        """Apply a specific strategy and return signal (BUY/SELL/HOLD)."""
        try:
            # Simple example strategy using Bollinger Bands
            if 'bb_upper' in indicators and 'bb_lower' in indicators:
                close = ohlcv['close']
                bb_upper = indicators['bb_upper']
                bb_lower = indicators['bb_lower']

                if close <= bb_lower:
                    return "BUY"
                elif close >= bb_upper:
                    return "SELL"
                else:
                    return "HOLD"

            return "HOLD"  # Default to HOLD if no indicators available
        except Exception as e:
            logger.error(f"Error applying strategy {strategy.get('name', 'unknown')}: {e}", exc_info=True)
            return "HOLD"

    async def stop(self):
        logger.debug("StrategyEngine stopping")
        self.kafka_client.close()