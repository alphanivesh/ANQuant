# src/anquant/adapters/base_broker.py
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Callable, Any  # Added Any to the imports
import pandas as pd
from src.anquant.models.signal import Signal


class BaseBroker(ABC):
    """Abstract base class for broker adapters."""

    @abstractmethod
    def authenticate(self):
        """Authenticate with the broker."""
        pass

    @abstractmethod
    def get_historical_data(self, symbol: str, symboltoken: str, interval: str, start_date: datetime,
                            end_date: datetime) -> pd.DataFrame:
        """Fetch historical data for a given symbol."""
        pass

    @abstractmethod
    def get_portfolio(self) -> List[Dict[str, any]]:
        """Fetch the current portfolio."""
        pass

    @abstractmethod
    def place_order(self, signal: Signal) -> Dict[str, any]:
        """Place an order based on the signal."""
        pass

    @abstractmethod
    def start_market_feed(self, symbols: List[Dict[str, str]], callback: Callable[[Dict[str, Any]], None]):
        """Start the market feed using WebSocket."""
        pass

    @abstractmethod
    def stop_market_feed(self):
        """Stop the market feed."""
        pass