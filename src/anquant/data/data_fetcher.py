# src/anquant/data/data_fetcher.py
import time
from datetime import datetime, timedelta
import pandas as pd
from loguru import logger

from src.anquant.data.script_master import ScriptMaster
from src.anquant.adapters.base_broker import BaseBroker


class DataFetcher:
    """Handles fetching historical data for stocks."""

    def __init__(self, broker: BaseBroker):
        self.broker = broker

    def fetch_nse_500_historical_data(self, strategies: list) -> dict:
        """Fetch historical data for the last 6 months for all NSE 500 stocks."""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=180)
            script_master = ScriptMaster()
            nse_500_stocks = script_master.get_nse_500_stocks()

            delay_between_requests = 0.333
            max_retries = 3

            # Dictionary to store historical data for each stock
            historical_data_dict = {}

            for stock in nse_500_stocks:
                symbol = stock["tradingsymbol"]
                token = stock["symboltoken"]
                for attempt in range(max_retries):
                    try:
                        intervals = {"intraday": "FIVE_MINUTE", "swing": "ONE_HOUR", "positional": "ONE_DAY"}
                        time_horizons = [s.config.get("time_horizon", "positional") for s in strategies]
                        shortest_horizon = min(time_horizons, key=lambda x: intervals[x])
                        interval = intervals[shortest_horizon]

                        historical_data = self.broker.get_historical_data(
                            symbol=symbol,
                            symboltoken=token,
                            interval=interval,
                            start_date=start_date,
                            end_date=end_date
                        )
                        if not historical_data.empty:
                            # Log the closing prices for debugging
                            logger.debug(f"Closing prices for {symbol} (last 14 periods): {historical_data['close'].tail(14).values}")
                            historical_data_dict[symbol] = historical_data
                        break
                    except Exception as e:
                        error_message = str(e)
                        if "Access denied because of exceeding access rate" in error_message:
                            if attempt < max_retries - 1:
                                logger.warning(f"Rate limit exceeded for {symbol}. Retrying in 5 seconds... (Attempt {attempt + 1}/{max_retries})")
                                time.sleep(5)
                                continue
                            else:
                                logger.error(f"Failed to fetch historical data for {symbol} after {max_retries} attempts: {error_message}")
                                break
                        else:
                            logger.error(f"Failed to fetch historical data for {symbol}: {error_message}")
                            break
                    finally:
                        time.sleep(delay_between_requests)

            return historical_data_dict

        except Exception as e:
            logger.error(f"Failed to fetch NSE 500 historical data: {e}")
            raise