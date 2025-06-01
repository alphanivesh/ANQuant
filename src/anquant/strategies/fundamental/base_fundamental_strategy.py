# src/anquant/strategies/fundamental/base_fundamental_strategy.py
from abc import ABC
from typing import Dict, Any, Optional
from loguru import logger
from src.anquant.strategies.base_strategy import BaseStrategy
from src.anquant.models.signal import Signal


class BaseFundamentalStrategy(BaseStrategy, ABC):
    """Base class for fundamental strategies."""

    STRATEGY_TYPE = "fundamental"
    SUB_TYPE = "value"
    TIME_HORIZON = "positional"

    def __init__(self, name: str, config: Dict[str, Any], broker=None):
        self.name = name
        self.config = config
        self.broker = broker

    def fetch_financial_data(self, symbol: str) -> Dict[str, Any]:
        logger.warning(f"Financial data fetching not implemented for {symbol}")
        return {}