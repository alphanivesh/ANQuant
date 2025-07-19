# D:\AlphaNivesh\ANQuant\tests\test_anquant.py
import asyncio
import pytest
from src.anquant.py.util.config_loader import load_config
from src.anquant.py.messaging.redis_client import RedisClient
from src.anquant.py.messaging.kafka_client import KafkaClient
from anquant.py.data_management.market_data import MarketDataEngine
from anquant.py.indicators.indicator_engine import IndicatorEngine
from src.anquant.py.core.flexirule.strategy_engine import StrategyEngine


@pytest.mark.asyncio
async def test_anquant_initialization():
    config = load_config("config/config.yaml")
    config["global"]["offline_mode"] = True
    redis_client = RedisClient(config["global"]["redis"])
    await redis_client.connect()
    adapters = {'india': {'angelone': {'symbols': ['RELIANCE-EQ', 'SBIN-EQ']}}}
    watchlists = {'india': {'meanhunter': ['RELIANCE-EQ', 'SBIN-EQ']}}

    components = [
        MarketDataEngine(config, redis_client, adapters, watchlists),
        IndicatorEngine(config, redis_client, adapters, watchlists),
        StrategyEngine(config, redis_client)
    ]

    await asyncio.gather(*(component.initialize() for component in components))

    kafka_client = KafkaClient(config["global"]["kafka"])
    message = {
        "tradingsymbol": "RELIANCE-EQ",
        "timestamp": "2025-07-19T20:10:00+05:30",
        "open": 100.0,
        "high": 102.0,
        "low": 98.0,
        "close": 100.0,
        "volume": 3000.0,
        "market": "india"
    }
    kafka_client.produce("ohlcv_5min", "RELIANCE-EQ", message)

    log_files = [
        "logs/market_data/market_data_engine_2025-07-19.log",
        "logs/indicators/indicator_engine_2025-07-19.log",
        "logs/strategy/strategy_engine_2025-07-19.log"
    ]
    for log_file in log_files:
        assert os.path.exists(log_file)
        with open(log_file, "r") as f:
            assert "initialized successfully" in f.read().lower()

    await asyncio.gather(*(component.stop() for component in components))
    await redis_client.close()