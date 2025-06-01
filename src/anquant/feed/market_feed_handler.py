# src/anquant/feed/market_feed_handler.py
from src.anquant.utils.redis_bus import event_bus
from loguru import logger
import time

class MarketFeedHandler:
    def __init__(self, broker):
        self.broker = broker
        self.running = False

    async def start_market_feed(self, symbols: list):
        if self.running:
            logger.warning("Market feed already running")
            return
        self.running = True
        async def on_data(data):
            symbol = data.get("tradingsymbol")
            if symbol:
                data["timestamp"] = time.perf_counter()
                await event_bus.publish(f"ticks:NSE:{symbol}", data)
        try:
            await self.broker.start_websocket(on_data)
        except Exception as e:
            logger.error(f"Failed to start market feed: {e}")
            self.running = False
            raise

    def stop(self):
        self.running = False
        logger.info("Market feed stopped")