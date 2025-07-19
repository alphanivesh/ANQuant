# D:\AlphaNivesh\ANQuant\src\anquant\py\core\adapters\angelone.py
import asyncio
import json
import time
import queue
from typing import Dict, List, Any, Callable, Optional
from .base_adapter import BaseAdapter
from src.anquant.py.messaging.kafka_client import KafkaClient
from src.anquant.py.messaging.redis_client import RedisClient
from SmartApi import SmartConnect, SmartWebSocket
import pyotp
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from functools import wraps
from src.anquant.py.util.logging import setup_logging
import websocket

logger = setup_logging("angelone", log_type="general")


def rate_limit(calls_per_second: float = 5):
    """Rate limit decorator to control API calls."""

    def decorator(func):
        last_call = 0

        @wraps(func)
        async def wrapper(*args, **kwargs):
            nonlocal last_call
            elapsed = time.time() - last_call
            wait_time = 1 / calls_per_second - elapsed
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            last_call = time.time()
            return await func(*args, **kwargs)

        return wrapper

    return decorator


class AngelOneAdapter(BaseAdapter):
    INTERVAL_MAPPING = {
        "1min": "ONE_MINUTE",
        "5min": "FIVE_MINUTE",
        "30min": "THIRTY_MINUTE",
    }

    def __init__(self, credentials: Dict[str, str], mappings: Dict[str, str], kafka_config: Dict[str, str],
                 config: Dict[str, Any]):
        self.credentials = credentials
        self.mappings = mappings
        self.offline_mode = config.get('global', {}).get('offline_mode', False)
        self.smart_api = None if self.offline_mode else SmartConnect(api_key=credentials.get('api_key', ''))
        self.kafka_client = None if self.offline_mode else KafkaClient(kafka_config)
        self.redis_client = RedisClient(config.get('global', {}).get('redis', {}))
        self.watchlist = []
        self.auth_token = None
        self.feed_token = None
        self.client_code = None
        self.websocket = None
        self.running = False
        self.authenticated = False
        self.last_auth_time = 0
        self.callback_handler = None
        self.message_queue = queue.Queue()
        self.processing_task = None
        logger.info(f"Initialized AngelOneAdapter, offline_mode={self.offline_mode}, symbols={len(self.mappings)}")

    async def connect(self) -> None:
        try:
            await self.authenticate()
            logger.info("Connected to Angel One")
        except Exception as e:
            logger.error(f"Failed to connect: {e}", exc_info=True)
            raise

    async def authenticate(self, retries: int = 3, delay: int = 5) -> Dict[str, Any]:
        if self.offline_mode:
            logger.debug("Skipping authentication in offline mode")
            self.authenticated = True
            self.feed_token = "offline"
            self.auth_token = "offline"
            self.client_code = self.credentials.get('client_code', 'mock')
            return {'clientcode': self.client_code, 'feedToken': 'offline', 'jwtToken': 'offline'}

        current_time = time.time()
        if current_time - self.last_auth_time < 15:
            await asyncio.sleep(15 - (current_time - self.last_auth_time))
            logger.debug("Rate-limiting authentication")

        if self.authenticated and self.feed_token and self.auth_token:
            try:
                if self.smart_api is not None:
                    self.smart_api.getProfile(self.auth_token)
                    logger.debug("Reusing existing session")
                    return {'clientcode': self.client_code, 'feedToken': self.feed_token, 'jwtToken': self.auth_token}
                else:
                    logger.warning("SmartAPI is None, cannot reuse session")
                    self.authenticated = False
            except Exception as e:
                logger.warning(f"Existing session invalid: {e}")
                self.authenticated = False

        for attempt in range(1, retries + 1):
            try:
                start_time = time.time()
                if self.smart_api is None:
                    raise ValueError("SmartAPI is not initialized")

                totp = pyotp.TOTP(self.credentials['totp_secret']).now()
                session = self.smart_api.generateSession(self.credentials['client_code'], self.credentials['pin'], totp)

                if isinstance(session, dict) and session.get('status', False) and session.get('data'):
                    session_data = session['data']
                    if isinstance(session_data, dict):
                        self.feed_token = session_data.get('feedToken')
                        self.auth_token = session_data.get('jwtToken')
                        self.client_code = session_data.get('clientcode')
                        self.authenticated = True
                        self.last_auth_time = time.time()
                        logger.info(f"Authentication successful, latency {time.time() - start_time:.3f}s")
                        return session_data
                    else:
                        logger.error(f"Invalid session data format: {type(session_data)}")
                        raise ValueError(f"Invalid session data format: {type(session_data)}")
                else:
                    error_message = session.get('message', 'Unknown error') if isinstance(session,
                                                                                          dict) else 'Invalid response format'
                    logger.error(f"Authentication failed: {error_message}")
                    raise ValueError(f"Authentication failed: {error_message}")
            except Exception as e:
                logger.error(f"Authentication attempt {attempt} failed: {e}", exc_info=True)
                if attempt < retries:
                    logger.info(f"Retrying in {delay}s")
                    await asyncio.sleep(delay)
                else:
                    logger.error("Max authentication retries reached")
                    raise ValueError(f"Max retries reached: {str(e)}")

    @rate_limit(calls_per_second=5)
    async def fetch_historical_data(self, symbol: str, timeframe: str, from_date: datetime, to_date: datetime) -> List[
        Dict[str, Any]]:
        try:
            start_time = time.time()
            if self.offline_mode:
                logger.debug(f"Generating mock historical data for {symbol}")
                data = [
                    {
                        "timestamp": (datetime.now(ZoneInfo("Asia/Kolkata")) - pd.Timedelta(minutes=i)).strftime(
                            "%Y-%m-%d %H:%M:%S"),
                        "open": 3000.0 - i * 10,
                        "high": 3000.0 - i * 10 + 50,
                        "low": 2950.0 - i * 10,
                        "close": 3000.0 - i * 10 + (100 if i == 0 else 0),
                        "volume": 1000 + i * 100,
                        "tradingsymbol": symbol,
                        "symboltoken": self.mappings.get(symbol, "mock"),
                        "exchange": "NSE"
                    } for i in range(20)
                ]
                await self.redis_client.publish(f"historical_data:NSE:{symbol}", json.dumps(data))
                logger.debug(
                    f"Published mock historical data, size {len(json.dumps(data))} bytes, latency {time.time() - start_time:.3f}s")
                return data

            await self.authenticate()
            params = {
                "exchange": "NSE",
                "symboltoken": self.mappings.get(symbol, ""),
                "interval": self.INTERVAL_MAPPING.get(timeframe, "ONE_MINUTE"),
                "fromdate": from_date.strftime("%Y-%m-%d %H:%M"),
                "todate": to_date.strftime("%Y-%m-%d %H:%M")
            }
            if not params["symboltoken"]:
                logger.error(f"No symboltoken for {symbol}")
                raise ValueError(f"No symboltoken for {symbol}")

            if self.smart_api is None:
                raise ValueError("SmartAPI is not initialized")

            response = self.smart_api.getCandleData(params)
            if isinstance(response, dict) and response.get('status', False):
                response_data = response.get('data', [])
                if isinstance(response_data, list):
                    data = [
                        {
                            "timestamp": d[0],
                            "open": float(d[1]),
                            "high": float(d[2]),
                            "low": float(d[3]),
                            "close": float(d[4]),
                            "volume": int(d[5]),
                            "tradingsymbol": symbol,
                            "symboltoken": params["symboltoken"],
                            "exchange": "NSE"
                        } for d in response_data
                    ]
                    logger.info(f"Fetched {len(data)} candles, latency {time.time() - start_time:.3f}s")
                    return data
                else:
                    logger.error(f"Invalid response data format: {type(response_data)}")
                    raise Exception(f"Invalid response data format: {type(response_data)}")
            else:
                error_message = response.get('message', 'Unknown error') if isinstance(response,
                                                                                       dict) else 'Invalid response format'
                logger.error(f"Historical data fetch failed: {error_message}")
                raise Exception(f"Historical data fetch failed: {error_message}")
        except Exception as e:
            logger.error(f"Failed to fetch historical data for {symbol}: {e}", exc_info=True)
            raise

    async def subscribe_to_ticks(self, watchlist: List[str]) -> None:
        try:
            start_time = time.time()
            if not self.feed_token:
                logger.error("Not connected. Call connect() first.")
                raise Exception("Not connected. Call connect() first.")

            self.watchlist = [
                {"tradingsymbol": symbol.upper(), "symboltoken": self.mappings.get(symbol, ""), "exchange": "NSE"}
                for symbol in watchlist
            ]
            invalid_symbols = [item['tradingsymbol'] for item in self.watchlist if not item['symboltoken']]
            if invalid_symbols:
                logger.error(f"Invalid symbols in watchlist: {invalid_symbols}")
                raise ValueError(f"Invalid symbols: {invalid_symbols}")

            logger.debug(f"Subscribing to {len(self.watchlist)} symbols")
            await self.start_websocket_async(self._process_tick)
            logger.info(f"Subscribed to ticks, latency {time.time() - start_time:.3f}s")
        except Exception as e:
            logger.error(f"Failed to subscribe to ticks: {e}", exc_info=True)
            raise

    async def _process_tick(self, tick: Dict[str, Any]) -> None:
        try:
            start_time = time.time()
            symbol = tick['tradingsymbol']
            logger.debug(f"[TRACE] Entered _process_tick for {symbol}")

            # Publish to Redis
            try:
                logger.debug(f"[TRACE] Before await redis_client.publish for {symbol}")
                await self.redis_client.publish(f"ticks:NSE:{symbol}", json.dumps(tick))
                logger.debug(f"[TRACE] After await redis_client.publish for {symbol}")
                logger.debug(f"Redis publish completed for {symbol}")
            except Exception as redis_error:
                logger.error(f"[TRACE] Redis publish failed for {symbol}: {redis_error}", exc_info=True)
                # Continue with Kafka even if Redis fails

            logger.debug(f"[TRACE] After Redis publish, before Kafka produce for {symbol}")
            logger.debug(
                f"[TRACE] kafka_client is {'set' if self.kafka_client else 'None'} in _process_tick for {symbol}")

            if self.kafka_client:
                try:
                    logger.debug(f"[TRACE] Before Kafka produce for {symbol}")
                    logger.debug(f"[TRACE] Kafka client type: {type(self.kafka_client)}")
                    logger.debug(f"[TRACE] About to call kafka_client.produce with topic=nse_ticks, key={symbol}")
                    logger.debug(f"[TRACE] Tick type: {type(tick)}, value: {tick}")
                    self.kafka_client.produce('nse_ticks', symbol, tick)
                    logger.debug(f"[TRACE] After Kafka produce for {symbol}")
                    logger.debug(f"[TRACE] Kafka produce completed for {symbol}")
                except Exception as kafka_error:
                    logger.error(f"[TRACE] Kafka publish failed for {symbol}: {kafka_error}", exc_info=True)

            logger.debug(f"[TRACE] Processed tick for {symbol}, latency {time.time() - start_time:.3f}s")
        except Exception as e:
            logger.error(f"[TRACE] Failed to process tick for {symbol}: {e}", exc_info=True)
            raise

    async def unsubscribe_from_ticks(self, watchlist: List[str]) -> None:
        try:
            start_time = time.time()
            self.stop_websocket()
            logger.info(f"Unsubscribed from {len(watchlist)} symbols, latency {time.time() - start_time:.3f}s")
        except Exception as e:
            logger.error(f"Failed to unsubscribe: {e}", exc_info=True)
            raise

    async def place_order(self, order_details: Dict[str, Any]) -> str:
        try:
            start_time = time.time()
            symbol = order_details.get('tradingsymbol', 'unknown')
            if self.offline_mode:
                logger.debug(f"Simulating order placement for {symbol}")
                return f"mock_order_{symbol}"

            await self.authenticate()
            order_details['symboltoken'] = self.mappings.get(symbol, "")
            order_details['exchange'] = order_details.get('exchange', 'NSE')
            if not order_details['symboltoken']:
                logger.error(f"No symboltoken for {symbol}")
                raise ValueError(f"No symboltoken for {symbol}")

            order_id = self.smart_api.placeOrder(order_details)
            logger.info(f"Placed order {order_id} for {symbol}, latency {time.time() - start_time:.3f}s")
            return order_id
        except Exception as e:
            logger.error(f"Failed to place order for {symbol}: {e}", exc_info=True)
            raise

    async def cancel_order(self, order_id: str) -> None:
        try:
            start_time = time.time()
            self.smart_api.cancelOrder(order_id, variety="NORMAL")
            logger.info(f"Cancelled order {order_id}, latency {time.time() - start_time:.3f}s")
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}", exc_info=True)
            raise

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        try:
            start_time = time.time()
            order_book = self.smart_api.orderBook()
            for order in order_book.get('data', []):
                if order['orderid'] == order_id:
                    logger.debug(f"Retrieved order status for {order_id}, latency {time.time() - start_time:.3f}s")
                    return order
            logger.warning(f"Order {order_id} not found")
            return {}
        except Exception as e:
            logger.error(f"Failed to get order status for {order_id}: {e}", exc_info=True)
            raise

    async def get_positions(self) -> List[Dict[str, Any]]:
        try:
            start_time = time.time()
            response = self.smart_api.position()
            positions = response.get('data', []) if response.get('status', False) else []
            logger.debug(f"Retrieved {len(positions)} positions, latency {time.time() - start_time:.3f}s")
            return positions
        except Exception as e:
            logger.error(f"Failed to get positions: {e}", exc_info=True)
            raise

    async def get_account_info(self) -> Dict[str, Any]:
        try:
            start_time = time.time()
            response = self.smart_api.rmsLimit()
            account_info = response.get('data', {}) if response.get('status', False) else {}
            logger.debug(f"Retrieved account info, latency {time.time() - start_time:.3f}s")
            return account_info
        except Exception as e:
            logger.error(f"Failed to get account info: {e}", exc_info=True)
            raise

    async def disconnect(self) -> None:
        try:
            start_time = time.time()
            self.stop_websocket()
            if not self.offline_mode:
                self.smart_api.terminateSession(self.credentials['client_code'])
            logger.info(f"Disconnected, latency {time.time() - start_time:.3f}s")
        except Exception as e:
            logger.error(f"Failed to disconnect: {e}", exc_info=True)
            raise

    async def get_quote(self, symbol: str, max_retries: int = 3, retry_delay: int = 5) -> Dict[str, Any]:
        try:
            start_time = time.time()
            if self.offline_mode:
                logger.debug(f"Returning mock quote for {symbol}")
                return {
                    'status': True,
                    'data': {
                        'high': 3050.0,
                        'low': 2950.0,
                        'close': 3000.0
                    }
                }

            for attempt in range(1, max_retries + 1):
                try:
                    await self.authenticate()
                    symboltoken = self.mappings.get(symbol, "")
                    if not symboltoken:
                        logger.error(f"No symboltoken for {symbol}")
                        raise ValueError(f"No symboltoken for {symbol}")
                    ltp_data = self.smart_api.getLtpData(exchange="NSE", tradingsymbol=symbol, symboltoken=symboltoken)
                    if ltp_data.get('status', False) and ltp_data.get('data'):
                        logger.debug(f"Retrieved quote for {symbol}, latency {time.time() - start_time:.3f}s")
                        return {
                            'status': True,
                            'data': {
                                'fetched': [{
                                    'token': symboltoken,
                                    'tradingsymbol': symbol,
                                    'ltp': ltp_data['data'].get('ltp', 0.0),
                                    'volume': ltp_data['data'].get('vtt', 0),
                                    'close': ltp_data['data'].get('close', 0.0)
                                }]
                            }
                        }
                    logger.warning(f"No LTP data for {symbol} on attempt {attempt}")
                    if attempt < max_retries:
                        await asyncio.sleep(retry_delay)
                except Exception as e:
                    logger.error(f"Quote fetch attempt {attempt} failed for {symbol}: {e}", exc_info=True)
                    if attempt >= max_retries:
                        raise ValueError(f"Max retries reached: {str(e)}")
            return {}
        except Exception as e:
            logger.error(f"Failed to get quote for {symbol}: {e}", exc_info=True)
            raise

    async def start_websocket_async(self, callback_handler: Callable) -> None:
        if self.offline_mode:
            logger.debug("Skipping WebSocket in offline mode")
            return

        try:
            start_time = time.time()
            self.callback_handler = callback_handler
            ws_url = (
                f"wss://smartapisocket.angelone.in/smart-stream?"
                f"clientCode={self.client_code}&feedToken={self.feed_token}&apiKey={self.credentials['api_key']}"
            )
            self.websocket = websocket.WebSocketApp(
                ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close
            )
            self.running = True

            def run_websocket():
                self.websocket.run_forever(ping_interval=0, ping_timeout=None)

            loop = asyncio.get_event_loop()
            loop.run_in_executor(None, run_websocket)

            self.processing_task = asyncio.create_task(self._process_message_queue())

            await asyncio.sleep(1)
            logger.info(f"Started WebSocket, latency {time.time() - start_time:.3f}s")
        except Exception as e:
            logger.error(f"Failed to start WebSocket: {e}", exc_info=True)
            self.running = False
            raise

    async def _process_message_queue(self):
        try:
            while self.running:
                try:
                    tick = self.message_queue.get(timeout=1.0)
                    if tick and self.callback_handler:
                        logger.debug(f"Calling callback handler for tick: {tick['tradingsymbol']}")
                        await self.callback_handler(tick)
                        logger.debug(f"Callback handler completed for tick: {tick['tradingsymbol']}")
                    else:
                        if not tick:
                            logger.debug("No tick data received from queue")
                        if not self.callback_handler:
                            logger.warning("No callback handler set")
                except queue.Empty:
                    continue
                except Exception as e:
                    logger.error(f"Error processing message from queue: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error in message queue processing: {e}", exc_info=True)

    def _on_open(self, ws):
        try:
            subscription = {
                "correlationID": "anquant_ticks",
                "action": 1,
                "params": {
                    "mode": 2,
                    "tokenList": [{"exchangeType": 1, "tokens": [item["symboltoken"] for item in self.watchlist]}]
                }
            }
            ws.send(json.dumps(subscription))
            logger.info(f"Subscribed to {len(self.watchlist)} tokens")
        except Exception as e:
            logger.error(f"WebSocket subscription failed: {e}", exc_info=True)
            self.running = False

    def _on_message(self, ws, message: Any):
        try:
            if message == "pong":
                logger.debug("Received pong heartbeat")
                return
            subscription_mode = int.from_bytes(message[0:1], "little")
            exchange_type = int.from_bytes(message[1:2], "little")
            token = message[2:27].decode("utf-8").rstrip("\x00")
            tradingsymbol = next((item["tradingsymbol"] for item in self.watchlist if item["symboltoken"] == token),
                                 token)

            timestamp_ms = int.from_bytes(message[35:43], "little")
            timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=ZoneInfo("Asia/Kolkata"))

            tick = {
                "tradingsymbol": tradingsymbol,
                "symboltoken": token,
                "ltp": int.from_bytes(message[43:51], "little") / 100,
                "volume": int.from_bytes(message[51:59], "little"),
                "timestamp": timestamp.isoformat(),
                "exchange": {1: "NSE", 3: "BSE", 5: "MCX"}.get(exchange_type, "NSE")
            }
            if subscription_mode == 2:
                try:
                    tick.update({
                        "open": int.from_bytes(message[59:67], "little") / 100,
                        "high": int.from_bytes(message[67:75], "little") / 100,
                        "low": int.from_bytes(message[75:83], "little") / 100,
                        "close": int.from_bytes(message[83:91], "little") / 100,
                    })
                except IndexError:
                    logger.warning(f"Incomplete FULL mode data for {tradingsymbol}")
            self.message_queue.put(tick)
            logger.debug(f"Processed tick for {tradingsymbol}")
        except Exception as e:
            logger.error(f"Failed to process WebSocket message for {tradingsymbol}: {e}", exc_info=True)

    def _on_error(self, ws, error: Any):
        logger.error(f"WebSocket error: {error}", exc_info=True)
        self.running = False

    def _on_close(self, ws, code=None, reason=None):
        logger.info(f"WebSocket closed: code={code}, reason={reason}")
        self.running = False
        self.websocket = None

    def stop_websocket(self) -> None:
        try:
            if self.offline_mode:
                logger.debug("No WebSocket to stop in offline mode")
                return
            if self.websocket and self.running:
                self.running = False
                self.websocket.close()

                if self.processing_task and not self.processing_task.done():
                    self.processing_task.cancel()

                logger.info("Stopped WebSocket")
        except Exception as e:
            logger.error(f"Failed to stop WebSocket: {e}", exc_info=True)