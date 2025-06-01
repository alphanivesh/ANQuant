# src/anquant/strategies/base_strategy.py
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from src.anquant.models.signal import Signal


class BaseStrategy(ABC):
    """Abstract base class for all trading strategies."""

    # Default metadata (override in subclasses or config.yaml)
    STRATEGY_TYPE = "technical"
    SUB_TYPE = "momentum"
    TIME_HORIZON = "positional"

    @abstractmethod
    def generate_signal(self, position: Dict[str, Any]) -> Optional[Signal]:
        """Generate a trading signal based on the given position."""
        pass

    @abstractmethod
    def on_market_data(self, data: Dict[str, Any]) -> None:
        """Handle incoming market data."""
        pass

    @abstractmethod
    def execute(self, data: Dict[str, Any]) -> None:
        """Execute the strategy on the given data."""
        pass