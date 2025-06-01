from abc import ABC, abstractmethod
from datetime import datetime
import pandas as pd
from typing import List, Dict, Any


class BrokerAdapter(ABC):
    """Abstract base class for broker adapters."""

    @abstractmethod
    def authenticate(self) -> bool:
        """Authenticate with the broker and establish a session."""
        pass

    @abstractmethod
    def get_historical_data(self, symbol: str, interval: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Fetch historical OHLCV data for a symbol."""
        pass

    @abstractmethod
    def start_market_feed(self, symbols: List[Dict[str, str]], callback: callable):
        """Start real-time market data feed for given symbols."""
        pass

    @abstractmethod
    def stop_market_feed(self):
        """Stop the real-time market data feed."""
        pass

    @abstractmethod
    def place_order(self, symbol: str, quantity: int, order_type: str, side: str, price: float = 0,
                    product_type: str = "INTRADAY") -> str:
        """Place an order and return the order ID."""
        pass

    @abstractmethod
    def get_portfolio(self) -> Dict[str, Any]:
        """Fetch portfolio holdings and balance."""
        pass

    @abstractmethod
    def get_order_details(self, order_id: str) -> Dict[str, Any]:
        """Fetch details of a specific order."""
        pass

    @abstractmethod
    def execute_order(self, symbol: str, quantity: int, order_type: str, side: str, price: float = 0,
                      product_type: str = "INTRADAY") -> str:
        """Execute an order (wrapper around place_order with additional logic if needed)."""
        pass