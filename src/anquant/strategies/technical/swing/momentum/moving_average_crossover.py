# src/anquant/strategies/technical/swing/momentum/moving_average_crossover.py
from typing import Dict, Any, Optional
import pandas as pd
from loguru import logger
from src.anquant.strategies.technical.base_technical_strategy import BaseTechnicalStrategy
from src.anquant.models.signal import Signal


class MovingAverageCrossoverStrategy(BaseTechnicalStrategy):
    """Swing trading strategy using moving average crossovers."""

    STRATEGY_TYPE = "technical"
    SUB_TYPE = "momentum"
    TIME_HORIZON = "swing"

    def __init__(self, name: str, config: Dict[str, Any], broker=None):
        super().__init__(name, config, broker)
        self.short_ma_length = config.get("short_ma_length", 20)
        self.long_ma_length = config.get("long_ma_length", 50)

    def generate_signal(self, position: Dict[str, Any]) -> Optional[Signal]:
        symbol = position.get("tradingsymbol", "Unknown")
        data = pd.DataFrame(position.get("historical_data", []))
        if data.empty:
            return None

        data["short_ma"] = self.calculate_moving_average(data, self.short_ma_length)
        data["long_ma"] = self.calculate_moving_average(data, self.long_ma_length)

        if len(data) < 2:
            return None

        if data["short_ma"].iloc[-1] > data["long_ma"].iloc[-1] and data["short_ma"].iloc[-2] <= data["long_ma"].iloc[-2]:
            return Signal(
                symbol=symbol,
                symboltoken=position.get("symboltoken", ""),
                exchange=position.get("exchange", "NSE"),
                order_type="BUY",
                quantity=1
            )
        elif data["short_ma"].iloc[-1] < data["long_ma"].iloc[-1] and data["short_ma"].iloc[-2] >= data["long_ma"].iloc[-2]:
            return Signal(
                symbol=symbol,
                symboltoken=position.get("symboltoken", ""),
                exchange=position.get("exchange", "NSE"),
                order_type="SELL",
                quantity=1
            )
        return None

    def on_market_data(self, data: Dict[str, Any]) -> None:
        self.execute(data)

    def execute(self, data: Dict[str, Any]) -> None:
        pass