import pytest
import asyncio
from src.py.util.config_loader import load_config
from src.py.core.adapters.angelone import AngelOneAdapter
from src.py.messaging.kafka_client import KafkaClient
import yaml
from datetime import datetime
import pandas as pd
from loguru import logger


@pytest.mark.asyncio
async def test_angelone_historical_data():
    config = load_config("config/config.yaml")
    config['global']['offline_mode'] = True
    credentials = load_config(config["global"]["markets"]["india"]["brokers"]["angelone"]["credentials"])
    mappings = load_config(config["global"]["markets"]["india"]["brokers"]["angelone"]["symbols"])
    adapter = AngelOneAdapter(credentials, mappings, config["global"]["kafka"], config)

    symbol = "RELIANCE-EQ"
    timeframe = "1min"
    to_date = datetime.now()
    from_date = to_date - pd.Timedelta(days=7)

    data = await adapter.fetch_historical_data(symbol, timeframe, from_date, to_date)
    assert len(data) >= 20, f"Insufficient historical data for {symbol}:{timeframe}"
    assert data[0]["exchange"] == "NSE", f"Unexpected exchange for {symbol}"
    assert data[0]["tradingsymbol"] == symbol, f"Unexpected tradingsymbol for {symbol}"
    logger.info("Historical data test passed", market="nse", symbol=symbol)


@pytest.mark.asyncio
async def test_angelone_ticks():
    config = load_config("config/config.yaml")
    offline_mode = config['global']['offline_mode']
    credentials = load_config(config["global"]["markets"]["india"]["brokers"]["angelone"]["credentials"])
    mappings = load_config(config["global"]["markets"]["india"]["brokers"]["angelone"]["symbols"])
    adapter = AngelOneAdapter(credentials, mappings, config["global"]["kafka"], config)

    watchlist = [stock['tradingsymbol'] for stock in
                 yaml.safe_load(open(config['global']['markets']['india']['watchlists']['master']))['stocks']][:10]

    await adapter.connect()
    await adapter.subscribe_to_ticks(watchlist)
    await asyncio.sleep(10 if not offline_mode else 2)  # Longer wait for online mode

    if not offline_mode:
        # Verify Kafka messages
        kafka_client = KafkaClient(config["global"]["kafka"])
        kafka_client.subscribe(["nse_ticks"])
        ticks_received = 0
        for _ in range(10):
            msg = kafka_client.poll(timeout=2.0)
            if msg:
                ticks_received += 1
                assert msg['topic'] == "nse_ticks", f"Unexpected topic: {msg['topic']}"
                assert msg['key'] in watchlist, f"Unexpected key: {msg['key']}"
                assert msg['value']['exchange'] == "NSE", f"Unexpected exchange: {msg['value']['exchange']}"
                assert 'ltp' in msg['value'], "Missing LTP in tick data"
                assert 'timestamp' in msg['value'], "Missing timestamp in tick data"
        assert ticks_received > 0, "No ticks received in nse_ticks"
        kafka_client.close()

    await adapter.unsubscribe_from_ticks(watchlist)
    await adapter.disconnect()
    logger.info("Tick subscription test passed", market="nse", symbol="none")