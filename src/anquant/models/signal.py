# src/anquant/models/signal.py
from dataclasses import dataclass

@dataclass
class Signal:
    """Class representing a trading signal."""
    symbol: str
    symboltoken: str
    exchange: str
    order_type: str  # "BUY" or "SELL"
    quantity: int