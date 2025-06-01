# src/anquant/portfolio/portfolio_manager.py
from src.anquant.utils.redis_bus import event_bus
from src.anquant.models.signal import Signal
from loguru import logger
import time

class PortfolioManager:
    async def start(self):
        async def handle_signal(signal_data):
            signal = Signal(**signal_data)
            if self.config.get("testing_place_orders", False):
                signal_data["timestamp"] = time.perf_counter()
                await event_bus.publish("orders:NSE", signal_data)
        await event_bus.subscribe("signals:momentum:NSE", handle_signal)