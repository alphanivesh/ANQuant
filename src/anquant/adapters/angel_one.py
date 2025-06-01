# src/anquant/adapters/angel_one.py
import asyncio
import time
from typing import Dict, Any, Optional
import pandas as pd
from loguru import logger
from smartapi import SmartConnect, SmartWebSocket
from pyotp import TOTP
from src.anquant.adapters.base_broker import BaseBroker
from src.anquant.models.signal import Signal
from src.anquant.utils.redis_bus import event_bus


class AngelOneAdapter(BaseBroker):
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize AngelOneAdapter with configuration.

        Args:
            config (Dict[str, Any]): Configuration containing credentials and settings.
        """
        self.config = config
        self.credentials = config.get("credentials", {})
        self.api_key = self.credentials.get("api_key")
        self.client_code = self.credentials.get("client_code")
        self.pin = self.credentials.get("pin")
        self.totp_token = self.credentials.get("totp_token")
        self.smart_api = SmartConnect(api_key=self.api_key)
        self.websocket = None
        self.authenticated = False
        self.running = False
        logger.info("AngelOneAdapter initialized")

    async def authenticate(self) -> None:
        """
        Authenticate with Angel One API using TOTP.

        Raises:
            Exception: If authentication fails.
        """
        try:
            totp = TOTP(self.totp_token)
            totp_code = totp.now()
            logger.info(f"Generated TOTP code: {totp_code}")
            session = self.smart_api.generateSession(
                clientCode=self.client_code,
                password=self.pin,
                totp=totp_code
            )
            if session.get("status"):
                self.authenticated = True
                logger.info("Angel One authentication successful")
            else:
                raise Exception("Authentication failed: Invalid credentials")
        except Exception as e:
            logger.error(f"Angel One authentication failed: {e}")
            raise

    async def get_historical_data(
            self,
            symbol: str,
            symboltoken: str,
            interval: str,
            exchange: str = "NSE",
            **kwargs
    ) -> pd.DataFrame:
        """
        Fetch historical data for a symbol.

        Args:
            symbol (str): Trading symbol (e.g., "SBIN-EQ").
            symboltoken (str): Symbol token (e.g., "3045").
            interval (str): Time interval (e.g., "ONE_MINUTE", "ONE_DAY").
            exchange (str): Exchange (default: "NSE").
            **kwargs: Additional parameters (e.g., start_date, end_date).

        Returns:
            pd.DataFrame: Historical data with OHLCV columns.
        """
        try:
            from_date = kwargs.get("start_date", pd.Timestamp.now() - pd.Timedelta(days=30))
            to_date = kwargs.get("end_date", pd.Timestamp.now())
            data = self.smart_api.getCandleData({
                "exchange": exchange,
                "symboltoken": symboltoken,
                "interval": interval,
                "fromdate": from_date.strftime("%Y-%m-%d %H:%M"),
                "todate": to_date.strftime("%Y-%m-%d %H:%M")
            })
            if data.get("status"):
                df = pd.DataFrame(
                    data["data"],
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                )
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                logger.debug(f"Fetched {len(df)} rows for {symbol}")
                return df
            else:
                logger.warning(f"No historical data for {symbol}")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Failed to fetch historical data for {symbol}: {e}")
            return pd.DataFrame()

    async def place_order(self, signal: Signal) -> Dict[str, Any]:
        """
        Place an order based on the signal.

        Args:
            signal (Signal): Signal containing order details.

        Returns:
            Dict[str, Any]: Order response from Angel One.

        Raises:
            Exception: If order placement fails.
        """
        try:
            order_params = {
                "variety": "NORMAL",
                "tradingsymbol": signal.symbol,
                "symboltoken": signal.symboltoken,
                "transactiontype": signal.order_type,
                "exchange": signal.exchange,
                "ordertype": "LIMIT",
                "producttype": "INTRADAY",
                "duration": "DAY",
                "price": str(signal.price),
                "quantity": str(signal.quantity)
            }
            response = self.smart_api.placeOrder(order_params)
            if response.get("status"):
                logger.info(f"Order placed for {signal.symbol}: {response}")
                return response
            else:
                raise Exception(f"Order failed: {response.get('message')}")
        except Exception as e:
            logger.error(f"Order failed for {signal.symbol}: {e}")
            raise

    async def start_websocket(self, on_data: Callable) -> None:
        """
        Start WebSocket for real-time market data.

        Args:
            on_data (Callable): Callback to handle incoming data.

        Raises:
            Exception: If WebSocket connection fails.
        """
        try:
            symbols = self.config.get("symbols", [])
            if not symbols:
                raise ValueError("No symbols provided for WebSocket")

            feed_token = self.smart_api.getfeedToken()
            client_code = self.client_code
            task = "mw"
            ws = SmartWebSocket(feed_token, client_code)

            async def on_message(data):
                try:
                    await on_data({
                        "tradingsymbol": data.get("tradingsymbol"),
                        "symboltoken": data.get("symboltoken"),
                        "ltp": data.get("ltp"),
                        "timestamp": time.perf_counter()
                    })
                except Exception as e:
                    logger.error(f"Error processing WebSocket data: {e}")

            def on_open():
                logger.info("WebSocket connection opened")
                ws.subscribe(task, [
                    {"exchange": symbol.get("exchange", "NSE"), "token": symbol["symboltoken"]}
                    for symbol in symbols
                ])

            def on_error(error):
                logger.error(f"WebSocket error: {error}")

            def on_close():
                logger.info("WebSocket connection closed")
                self.running = False

            ws.on_open = on_open
            ws.on_message = on_message
            ws.on_error = on_error
            ws.on_close = on_close

            self.websocket = ws
            self.running = True
            logger.info("Starting Angel One WebSocket")
            ws.connect()
            while self.running:
                await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"WebSocket failed: {e}")
            self.running = False
            raise

    async def start(self) -> None:
        """
        Start the adapter, subscribing to order events via Redis Pub/Sub.
        """

        async def handle_order(signal_data: Dict[str, Any]):
            try:
                signal = Signal(**signal_data)
                start = time.perf_counter()
                response = await self.place_order(signal)
                latency = (time.perf_counter() - start) * 1000
                logger.info(f"Order placed for {signal.symbol}: {response}")
                await event_bus.publish("metrics:NSE", {
                    "symbol": signal.symbol,
                    "latency": latency,
                    "slippage": response.get("price", 0) - signal.price
                })
            except Exception as e:
                logger.error(f"Order failed for {signal_data.get('symbol')}: {e}")

        try:
            await event_bus.subscribe("orders:NSE", handle_order)
        except Exception as e:
            logger.error(f"Failed to start AngelOneAdapter: {e}")
            raise

    def stop(self) -> None:
        """
        Stop the WebSocket and adapter.
        """
        if self.websocket and self.running:
            self.websocket.close()
            self.running = False
        logger.info("Angel One WebSocket thread stopped")