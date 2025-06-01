# src/anquant/core/market_scheduler.py
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
from loguru import logger
from src.anquant.utils.redis_bus import event_bus

class MarketScheduler:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.markets = {
            "NSE": {
                "timezone": "Asia/Kolkata",
                "open": "09:15",
                "close": "15:30",
                "days": "mon-fri"
            }
            # Phase 2: Add US market
        }

    async def start_market(self, market: str):
        logger.info(f"Starting {market} market operations")
        await event_bus.publish(f"market:{market}:start", {"market": market})

    async def stop_market(self, market: str):
        logger.info(f"Stopping {market} market operations")
        await event_bus.publish(f"market:{market}:stop", {"market": market})

    def schedule_markets(self):
        for market, config in self.markets.items():
            tz = pytz.timezone(config["timezone"])
            self.scheduler.add_job(
                self.start_market,
                CronTrigger(
                    hour=config["open"].split(":")[0],
                    minute=config["open"].split(":")[1],
                    day_of_week=config["days"],
                    timezone=tz
                ),
                args=[market]
            )
            self.scheduler.add_job(
                self.stop_market,
                CronTrigger(
                    hour=config["close"].split(":")[0],
                    minute=config["close"].split(":")[1],
                    day_of_week=config["days"],
                    timezone=tz
                ),
                args=[market]
            )

    async def start(self):
        self.scheduler.start()
        logger.info("Market scheduler started")

    async def shutdown(self):
        self.scheduler.shutdown()
        logger.info("Market scheduler stopped")