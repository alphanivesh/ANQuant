# src/anquant/strategies/fundamental/value/dividend_yield_strategy.py
from typing import Dict, Any, Optional
from loguru import logger
from src.anquant.strategies.fundamental.base_fundamental_strategy import BaseFundamentalStrategy
from src.anquant.models.signal import Signal


class DividendYieldStrategy(BaseFundamentalStrategy):
    """Value strategy based on high dividend yield."""

    STRATEGY_TYPE = "fundamental"
    SUB_TYPE = "value"
    TIME_HORIZON = "positional"

    def __init__(self, name: str, config: Dict[str, Any], broker=None):
        super().__init__(name, config, broker)
        self.min_dividend_yield = config.get("min_dividend_yield", 5.0)

    def generate_signal(self, position: Dict[str, Any]) -> Optional[Signal]:
        symbol = position.get("tradingsymbol", "Unknown")
        financial_data = self.fetch_financial_data(symbol)
        dividend_yield = financial_data.get("dividend_yield", 0.0)

        if dividend_yield > self.min_dividend_yield:
            logger.info(f"High dividend yield detected for {symbol}: {dividend_yield}%")
            return Signal(
                symbol=symbol,
                symboltoken=position.get("symboltoken", ""),
                exchange=position.get("exchange", "NSE"),
                order_type="BUY",
                quantity=1
            )
        return None

    def on_market_data(self, data: Dict[str, Any]) -> None:
        pass

    def execute(self, data: Dict[str, Any]) -> None:
        pass