from abc import ABC, abstractmethod
from typing import List, Dict, Any
import datetime

class BaseAdapter(ABC):
    @abstractmethod
    def __init__(self, credentials: Dict[str, str], mappings: Dict[str, str], kafka_config: Dict[str, str], config: Dict[str, Any]):
        """Initialize the adapter with credentials, symbol mappings, Kafka config, and general config."""
        pass

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the broker and authenticate."""
        pass

    @abstractmethod
    async def subscribe_to_ticks(self, watchlist: List[str]) -> None:
        """Subscribe to real-time tick data for the given watchlist of symbols."""
        pass

    @abstractmethod
    async def unsubscribe_from_ticks(self, watchlist: List[str]) -> None:
        """Unsubscribe from real-time tick data for the given watchlist."""
        pass

    @abstractmethod
    async def place_order(self, order_details: Dict[str, Any]) -> str:
        """Place an order with the given details and return the order ID."""
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str) -> None:
        """Cancel the order with the given order ID."""
        pass

    @abstractmethod
    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """Get the status of the order with the given order ID."""
        pass

    @abstractmethod
    async def fetch_historical_data(self, symbol: str, timeframe: str, from_date: datetime.datetime, to_date: datetime.datetime) -> List[Dict[str, Any]]:
        """Fetch historical OHLCV data for the symbol in the given timeframe and date range."""
        pass

    @abstractmethod
    async def get_positions(self) -> List[Dict[str, Any]]:
        """Get current positions and P&L."""
        pass

    @abstractmethod
    async def get_account_info(self) -> Dict[str, Any]:
        """Get account balance and margins."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Cleanly disconnect from the broker."""
        pass

    @abstractmethod
    async def get_quote(self, symbol: str, max_retries: int = 3, retry_delay: int = 5) -> Dict[str, Any]:
        """Get real-time quote for the symbol."""
        pass