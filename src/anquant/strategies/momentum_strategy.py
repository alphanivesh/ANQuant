# src/anquant/strategies/momentum_strategy.py
from src.anquant.strategies.base_strategy import BaseStrategy
from src.anquant.models.signal import Signal
from src.anquant.utils.redis_bus import event_bus
from loguru import logger
import time

class MomentumStrategy(BaseStrategy):
    def __init__(self, config: Dict[str, Any], broker=None):
        self.config = config
        self.broker = broker
        self.name = config.get("strategy_name", "MomentumStrategy")
        self.reference_price = config.get("reference_price", 800)
        self.active = False

    async def start(self):
        async def handle_tick(data):
            if self.active:
                await self.execute(data)
        async def handle_market_start(data):
            if data["market"] == "NSE":
                self.active = True
                logger.info(f"{self.name} activated for NSE")
        async def handle_market_stop(data):
            if data["market"] == "NSE":
                self.active = False
                logger.info(f"{self.name} deactivated for NSE")
        for symbol in self.config.get("symbols", [{"tradingsymbol": "SBIN-EQ", "symboltoken": "3045"}]):
            await event_bus.subscribe(f"ticks:NSE:{symbol['tradingsymbol']}", handle_tick)
        await event_bus.subscribe("market:NSE:start", handle_market_start)
        await event_bus.subscribe("market:NSE:stop", handle_market_stop)

    async def execute(self, data: Dict[str, Any]):
        symbol = data.get("tradingsymbol", "")
        symboltoken = data.get("symboltoken", "")
        ltp = float(data.get("ltp", 0))
        if not all([symbol, symboltoken, ltp]):
            logger.warning(f"Invalid market data: {data}")
            return
        logger.debug(f"Processing: {symbol}, LTP: {ltp}")
        threshold = self.reference_price * 1.01
        if ltp > threshold:
            signal = Signal(
                symbol=symbol,
                symboltoken=symboltoken,
                exchange="NSE",
                order_type="BUY",
                quantity=1,
                price=ltp * 1.005
            )
            signal_data = signal.__dict__
            signal_data["timestamp"] = time.perf_counter()
            await event_bus.publish(f"signals:momentum:NSE", signal_data)