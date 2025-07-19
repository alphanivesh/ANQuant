# src/anquant/py/core/historical_data_manager.py
from typing import Dict, Any
from src.anquant.py.messaging.redis_client import RedisClient
from src.anquant.py.util.database import Database
from anquant.py.corporate_actions.corporate_actions_manager import CorporateActionManager
from src.anquant.py.util.logging import setup_logging

logger = setup_logging("historical_data_manager", log_type="general")

class HistoricalDataManager:
    def __init__(self, config: Dict[str, Any], redis_client: RedisClient, database: Database):
        self.config = config
        self.redis_client = redis_client
        self.database = database
        self.corporate_action_manager = CorporateActionManager(config, redis_client)
        logger.debug("Initialized HistoricalDataManager")

    async def initialize(self):
        logger.debug("HistoricalDataManager initializing")
        cursor = self.database.conn.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS anquant.ohlcv (
                    timestamp TIMESTAMP,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    volume BIGINT,
                    tradingsymbol VARCHAR(50),
                    exchange VARCHAR(10),
                    timeframe VARCHAR(10),
                    PRIMARY KEY (timestamp, tradingsymbol, timeframe)
                )
            """)
            self.database.conn.commit()
            logger.debug("Ensured anquant.ohlcv table exists")
        except Exception as e:
            self.database.conn.rollback()
            logger.error(f"Failed to initialize anquant.ohlcv table: {e}")
            raise
        finally:
            cursor.close()

    async def start(self):
        logger.debug("HistoricalDataManager starting")  # Historical data management runs passively, no active tasks needed
        return None

    async def stop(self):
        logger.debug("HistoricalDataManager stopping")
        # No cleanup needed for passive historical data management