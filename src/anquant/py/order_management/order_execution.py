# src/py/core/order_execution.py
from typing import Dict, Any
from loguru import logger
from src.anquant.py.core.adapters import get_adapters
from src.anquant.py.util.logging import setup_logging

logger = setup_logging("order_execution_engine", log_type="general")

class OrderExecutionEngine:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.adapters = get_adapters(config)
        logger.debug("Initialized OrderExecutionEngine with adapters for markets: {}".format(list(self.adapters.keys())))

    async def initialize(self):
        logger.debug("OrderExecutionEngine initializing")
        try:
            for market, brokers in self.adapters.items():
                for broker, adapter in brokers.items():
                    await adapter.connect()
                    logger.info(f"Connected to {broker} adapter for market {market}")
        except Exception as e:
            logger.error(f"Failed to initialize OrderExecutionEngine: {e}")
            raise

    async def start(self):
        logger.debug("OrderExecutionEngine starting")  # Order execution runs passively, no active tasks needed
        return None

    async def stop(self):
        logger.debug("OrderExecutionEngine stopping")
        # No cleanup needed for passive order execution