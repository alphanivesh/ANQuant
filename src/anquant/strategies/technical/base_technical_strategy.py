# src/anquant/strategies/technical/base_technical_strategy.py
from abc import ABC
from typing import Dict, Any, Optional
import pandas as pd
from loguru import logger
from src.anquant.strategies.base_strategy import BaseStrategy
from src.anquant.models.signal import Signal


class BaseTechnicalStrategy(BaseStrategy, ABC):
    """Base class for technical strategies."""

    STRATEGY_TYPE = "technical"
    SUB_TYPE = "momentum"
    TIME_HORIZON = "positional"

    def __init__(self, name: str, config: Dict[str, Any], broker=None):
        self.name = name
        self.config = config
        self.broker = broker

    def calculate_moving_average(self, data: pd.DataFrame, length: int = 20) -> pd.Series:
        """Calculate moving average for the given data."""
        return data["close"].rolling(window=length).mean()