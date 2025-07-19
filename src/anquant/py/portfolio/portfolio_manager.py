# src/py/core/portfolio_manager.py
from typing import Dict, Any
from loguru import logger
from src.anquant.py.messaging.redis_client import RedisClient
from src.anquant.py.util.database import Database
from src.anquant.py.util.logging import setup_logging

logger = setup_logging("portfolio_manager", log_type="general")

class PortfolioManager:
    def __init__(self, config: Dict[str, Any], redis_client: RedisClient, database: Database):
        self.config = config
        self.redis_client = redis_client
        self.database = database
        logger.debug("Initialized PortfolioManager")

    async def initialize(self):
        logger.debug("PortfolioManager initializing")
        cursor = self.database.conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS anquant.trades (
                    trade_id VARCHAR(50) PRIMARY KEY,
                    timestamp TIMESTAMP,
                    tradingsymbol VARCHAR(50),
                    exchange VARCHAR(10),
                    side VARCHAR(10),
                    quantity INTEGER,
                    price FLOAT,
                    strategy VARCHAR(50)
                )
            """)
            self.database.conn.commit()
            logger.debug("Ensured anquant.trades table exists")
        except Exception as e:
            self.database.conn.rollback()
            logger.error(f"Failed to initialize anquant.trades table: {e}")
            raise
        finally:
            cursor.close()

    async def start(self):
        logger.debug("PortfolioManager starting")  # Portfolio management runs passively, no active tasks needed
        return None

    async def stop(self):
        logger.debug("PortfolioManager stopping")
        # No cleanup needed for passive portfolio management