# src/anquant/strategies/technical/positional/momentum/momentum_strategy.py
from typing import Dict, Any, Optional
from loguru import logger
from src.anquant.strategies.technical.base_technical_strategy import BaseTechnicalStrategy
from src.anquant.models.signal import Signal


class MomentumStrategy(BaseTechnicalStrategy):
    """A simple momentum-based trading strategy."""

    STRATEGY_TYPE = "technical"
    SUB_TYPE = "momentum"
    TIME_HORIZON = "positional"

    def __init__(self, name: str, config: Dict[str, Any], broker=None):
        super().__init__(name, config, broker)
        self.reference_price = config.get("reference_price", 800)

    def generate_signal(self, position: Dict[str, Any]) -> Optional[Signal]:
        symbol = position.get("tradingsymbol", "Unknown")
        current_price = float(position.get("ltp", 0))
        logger.debug(f"Generating signal for {symbol}, Current Price: {current_price}")

        threshold_buy = self.reference_price * 1.01
        threshold_sell = self.reference_price * 0.99

        if current_price > threshold_buy:
            return Signal(
                symbol=symbol,
                symboltoken=position.get("symboltoken", ""),
                exchange=position.get("exchange", "NSE"),
                order_type="BUY",
                quantity=1
            )
        elif current_price < threshold_sell:
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
        symbol = data.get("tradingsymbol", "Unknown")
        ltp = float(data.get("ltp", 0))
        logger.debug(f"Processing market data: {symbol}, LTP: {ltp}")

        threshold = self.reference_price * 1.01

        if symbol == "SBIN-EQ" and ltp > threshold:
            signal = Signal(
                symbol=symbol,
                symboltoken=data.get("token", ""),
                exchange=data.get("exchange", "NSE"),
                order_type="BUY",
                quantity=1
            )
            if self.broker and self.config.get("testing", {}).get("place_orders", False):
                order_response = self.broker.place_order(signal)
                logger.info(f"Momentum strategy: Buy order placed for {symbol}, Response: {order_response}")
            else:
                logger.info("Momentum strategy: Buy order skipped (testing.place_orders is false or no broker)")
        else:
            logger.debug("Momentum condition not met")