# src/anquant/data/script_master.py
import requests
import pandas as pd
from loguru import logger
from typing import List, Dict, Any


class ScriptMaster:
    """Utility to fetch and process the Angel One Script Master file."""

    SCRIPT_MASTER_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

    def __init__(self):
        self.script_data = None

    def fetch_script_master(self) -> List[Dict[str, Any]]:
        """Fetch the Script Master file from Angel One."""
        try:
            logger.info("Fetching Script Master file...")
            response = requests.get(self.SCRIPT_MASTER_URL)
            response.raise_for_status()  # Raise an exception for HTTP errors
            self.script_data = response.json()
            logger.info(f"Fetched Script Master with {len(self.script_data)} records")
            return self.script_data
        except Exception as e:
            logger.error(f"Failed to fetch Script Master: {e}")
            raise

    def get_nse_500_stocks(self) -> List[Dict[str, str]]:
        """Extract NSE 500 stocks from the Script Master."""
        if self.script_data is None:
            self.fetch_script_master()

        try:
            nse_equity_stocks = []
            for script in self.script_data:
                # Log the first few entries to debug the structure
                if len(nse_equity_stocks) < 5 and not nse_equity_stocks:
                    logger.debug(f"Script entry: {script}")

                # Filter for NSE equity stocks in the cash segment
                symbol = script.get("symbol", "")
                if (script.get("exch_seg") == "NSE" and
                        script.get("instrumenttype") == "" and
                        symbol.endswith("-EQ") and
                        "NSETEST" not in symbol and  # Exclude test symbols
                        not symbol.endswith("ETF-EQ") and  # Exclude ETFs
                        not symbol.endswith("BEES-EQ") and  # Exclude ETFs
                        script.get("name") not in ["", "NIFTY", "BANKNIFTY", "FINNIFTY"]):  # Exclude indices
                    # Additional check: series should be "EQ" if present
                    if "series" in script and script.get("series") != "EQ":
                        logger.debug(f"Filtered out {symbol} due to series: {script.get('series')}")
                        continue

                    nse_equity_stocks.append({
                        "tradingsymbol": symbol,
                        "symboltoken": script["token"]
                    })
                    # Log the first few matching entries
                    if len(nse_equity_stocks) <= 5:
                        logger.debug(f"Matched NSE equity stock: {symbol} (Token: {script['token']})")

            # For this example, limit to 500 stocks
            # In practice, cross-reference with the official Nifty 500 list
            nse_500_stocks = nse_equity_stocks[:500]
            logger.info(f"Extracted {len(nse_500_stocks)} NSE 500 stocks")

            # Log the first few extracted stocks for verification
            if nse_500_stocks:
                logger.debug(f"First few NSE 500 stocks: {nse_500_stocks[:5]}")
            else:
                logger.warning("No NSE 500 stocks extracted. Check filtering conditions.")

            return nse_500_stocks
        except Exception as e:
            logger.error(f"Failed to extract NSE 500 stocks: {e}")
            raise