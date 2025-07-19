# src/anquant/py/core/corporate_actions_manager.py
import asyncio
import pandas as pd
from typing import Dict, Any, List
from src.anquant.py.messaging.redis_client import RedisClient
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from io import StringIO
import json
from datetime import datetime
from src.anquant.py.util.logging import setup_logging

logger = setup_logging("corporate_action_manager", log_type="general")


class CorporateActionManager:
    def __init__(self, config: Dict[str, Any], redis_client: RedisClient):
        self.config = config
        self.redis_client = redis_client
        logger.debug("Initialized CorporateActionManager")

    def setup_driver(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("window-size=1920,1080")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/117.0.0.0 Safari/537.36")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                  get: () => undefined
                });
            """
        })
        return driver

    async def initialize(self):
        logger.debug("CorporateActionManager initializing")
        try:
            await self.redis_client.redis.ping()
            logger.debug("Verified Redis connectivity for CorporateActionManager")
            # Pre-fetch corporate actions during initialization (optional)
            try:
                await self.fetch_and_cache_all_actions()
            except Exception as e:
                logger.warning(f"Failed to fetch corporate actions during initialization: {e}")
                logger.info("CorporateActionManager will continue without pre-fetched data")
        except Exception as e:
            logger.error(f"Failed to initialize CorporateActionManager: {e}")
            raise

    async def start(self):
        logger.debug("CorporateActionManager starting")  # Corporate actions management runs passively, no active tasks needed
        return None

    async def stop(self):
        logger.debug("CorporateActionManager stopping")
        # No cleanup needed for passive corporate actions management

    async def fetch_and_cache_all_actions(self):
        """Fetch corporate actions from NSE and cache in Redis."""
        logger.info("Fetching corporate actions from NSE")
        url = "https://www.nseindia.com/companies-listing/corporate-filings-actions"
        driver = self.setup_driver()
        try:
            driver.get(url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            await asyncio.sleep(5)  # Allow async context for page load
            html = driver.page_source
            raw_tables = pd.read_html(StringIO(html), flavor="html5lib")

            target_df = None
            for t in raw_tables:
                lower_cols = [str(col).strip().lower() for col in t.columns]
                if 'symbol' in lower_cols and 'purpose' in lower_cols:
                    target_df = t
                    break

            if target_df is None:
                raise Exception("No corporate actions table found")

            df = target_df.copy()
            df.columns = [str(col).strip().replace(" ", "_").lower() for col in df.columns]
            df = df.rename(columns={
                'symbol': 'tradingsymbol',
                'purpose': 'type',
                'ex-date': 'ex_date',
                'record_date': 'record_date'
            })

            # Cache actions per symbol in Redis
            for _, row in df.iterrows():
                symbol = row['tradingsymbol']
                if not isinstance(symbol, str):
                    continue
                symbol_clean = symbol.replace("-EQ", "")
                action = {
                    "type": row['type'],
                    "ex_date": row['ex_date'],
                    "record_date": row.get('record_date', ''),
                    "amount": float(row.get('dividend_amount', 0)) if 'dividend' in row['type'].lower() else 0,
                    "ratio": row.get('ratio', '') if 'bonus' in row['type'].lower() or 'rights' in row[
                        'type'].lower() else ''
                }
                cache_key = f"{symbol_clean}:corporate_actions"
                await self.redis_client.cache(cache_key, [action], ttl=86400)
                logger.debug(f"Cached corporate actions for {symbol_clean}")

            logger.info("Completed caching corporate actions")
        except Exception as e:
            logger.error(f"Error fetching corporate actions: {e}")
            raise
        finally:
            driver.quit()

    async def fetch_actions(self, symbol: str) -> List[Dict]:
        """Retrieve cached corporate actions from Redis."""
        symbol_clean = symbol.replace("-EQ", "")
        cache_key = f"{symbol_clean}:corporate_actions"
        try:
            actions = await self.redis_client.get(cache_key)
            if actions:
                logger.debug(f"Retrieved corporate actions for {symbol} from Redis")
                return actions
            # Fallback to fetching if not cached
            await self.fetch_and_cache_all_actions()
            actions = await self.redis_client.get(cache_key)
            return actions if actions else []
        except Exception as e:
            logger.error(f"Failed to fetch corporate actions for {symbol}: {e}")
            return []

    async def adjust_ohlcv(self, symbol: str, timeframe: str, ohlcv_df: pd.DataFrame,
                           actions: List[Dict]) -> pd.DataFrame:
        """Adjust OHLCV data for corporate actions."""
        df = ohlcv_df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        for action in actions:
            ex_date = pd.to_datetime(action["ex_date"])
            if action["type"].lower() == "dividend" and ex_date <= pd.Timestamp("2025-07-15"):
                df.loc[df["timestamp"] < ex_date, ["open", "high", "low", "close"]] -= action["amount"]
                logger.debug(f"Adjusted {symbol} ({timeframe}) for dividend: {action['amount']}")
            elif action["type"].lower() == "rights":
                ratio = eval(action["ratio"].replace(":", "/"))
                df.loc[df["timestamp"] < ex_date, ["open", "high", "low", "close"]] *= (1 / (1 + ratio))
            elif action["type"].lower() == "bonus":
                ratio = eval(action["ratio"].replace(":", "/"))
                df.loc[df["timestamp"] < ex_date, ["open", "high", "low", "close"]] /= (1 + ratio)
                df.loc[df["timestamp"] < ex_date, "volume"] *= (1 + ratio)
        return df