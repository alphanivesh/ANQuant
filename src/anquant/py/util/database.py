from typing import Dict, Any
import psycopg2
from loguru import logger
from src.anquant.py.util.logging import setup_logging
import pandas as pd

logger = setup_logging("database", log_type="general")

class Database:
    def __init__(self, config: Dict[str, Any]):
        self.conn = psycopg2.connect(
            dbname=config.get("dbname", "ANQuantDB"),
            user=config.get("user", "postgres"),
            password=config.get("password", ""),
            host=config.get("host", "localhost"),
            port=config.get("port", 5432)
        )
        cursor = self.conn.cursor()
        try:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS anquant")
            cursor.execute("SET search_path TO anquant")
            cursor.execute("GRANT CREATE, USAGE ON SCHEMA anquant TO CURRENT_USER")
            self.conn.commit()
            logger.debug("Initialized Database connection with anquant schema")
        except psycopg2.errors.InsufficientPrivilege as e:
            self.conn.rollback()
            logger.error(f"Insufficient privileges for anquant schema in {config.get('dbname', 'ANQuantDB')}: {e}")
            raise
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to initialize anquant schema: {e}")
            raise
        finally:
            cursor.close()

    def save_ohlcv(self, symbol: str, timeframe: str, df: pd.DataFrame):
        cursor = self.conn.cursor()
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
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO anquant.ohlcv (timestamp, open, high, low, close, volume, tradingsymbol, exchange, timeframe)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp, tradingsymbol, timeframe) DO UPDATE
                    SET open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        exchange = EXCLUDED.exchange
                """, (
                    row['timestamp'], row['open'], row['high'], row['low'],
                    row['close'], row['volume'], symbol, row['exchange'], timeframe
                ))
            self.conn.commit()
            logger.debug(f"Saved OHLCV data for {symbol}:{timeframe} to PostgreSQL")
        except psycopg2.errors.InsufficientPrivilege as e:
            self.conn.rollback()
            logger.error(f"Insufficient privileges to create table for {symbol}:{timeframe} in anquant schema: {e}")
            raise
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to save OHLCV for {symbol}:{timeframe}: {e}")
            raise
        finally:
            cursor.close()

    def close(self):
        self.conn.close()
        logger.debug("Closed Database connection")