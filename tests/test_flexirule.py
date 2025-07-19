# tests/test_flexirule.py
import asyncio
import pytest
import json
import os
from src.anquant.py.util.config_loader import load_config
from src.anquant.py.messaging.redis_client import RedisClient
from src.anquant.py.core.flexirule.strategy_engine import StrategyEngine
from src.anquant.py.core.flexirule.validator import StrategyValidator

@pytest.mark.asyncio
async def test_strategy_validation():
    config = load_config("config/config.yaml")
    validator = StrategyValidator(config)
    strategy_path = "config/markets/india/strategies/meanhunter_strategy.yaml"
    assert validator.validate_strategy(strategy_path), "Strategy validation failed"
    log_file = "logs/strategy/flexirule_validator_2025-07-19.log"
    assert os.path.exists(log_file)
    with open(log_file, 'r') as f:
        assert "Successfully validated strategy: meanhunter" in f.read()

@pytest.mark.asyncio
async def test_rule_evaluation():
    config = load_config("config/config.yaml")
    config["global"]["offline_mode"] = True
    redis_client = RedisClient(config["global"]["redis"])
    await redis_client.connect()
    strategy_config = {
        "name": "meanhunter",
        "timeframe": "5min",
        "watchlist": {"india": "config/markets/india/watchlists/meanhunter.yaml"},
        "threshold": 0.75,
        "indicators": [
            {"name": "bbands", "type": "bollinger_bands", "period": 20, "std": 2.0},
            {"name": "rsi", "type": "rsi", "period": 14}
        ],
        "entry_rules": [
            {"condition": "close < bb_lower", "weight": 0.6},
            {"condition": "rsi < 30", "weight": 0.2},
            {"condition": "volume > volume_threshold", "weight": 0.2}
        ],
        "exit_rules": [
            {"condition": "close > bb_upper", "weight": 0.8}
        ],
        "stop_loss": {
            "type": "multi",
            "rules": [{"type": "fixed", "value": "2%", "id": "fixed"}]
        },
        "target": {
            "type": "multi",
            "rules": [{"type": "fixed", "value": "5%", "partial_exit": "50%", "id": "partial_1"}]
        },
        "trade_management": {
            "breakeven": {"trigger": 2.0}
        },
        "market_params": {
            "india": {"volume_threshold": "avg_volume_20 * 1.5"}
        }
    }
    strategy_engine = StrategyEngine(config, redis_client)
    strategy_engine.strategies = [strategy_config]
    strategy_engine.rule_engines = {strategy_config['name']: RuleEngine(strategy_config, redis_client)}
    await strategy_engine.initialize()

    ohlcv = {
        "tradingsymbol": "RELIANCE-EQ",
        "timestamp": "2025-07-19T18:42:00+05:30",
        "close": 100.0,
        "volume": 3000.0,
        "market": "india"
    }
    indicators = {
        "bb_lower": 110.0,
        "bb_upper": 120.0,
        "rsi": 25.0,
        "avg_volume_20": 1500.0
    }
    signal = await strategy_engine.rule_engines["meanhunter"].evaluate("RELIANCE-EQ", ohlcv, indicators)
    assert signal == "BUY", f"Expected BUY signal, got {signal}"

    # Simulate partial target
    ohlcv['close'] = 106.0  # Above 5% target (105.0)
    signal = await strategy_engine.rule_engines["meanhunter"].evaluate("RELIANCE-EQ", ohlcv, indicators)
    assert signal == "PARTIAL_SELL:50:partial_1", f"Expected PARTIAL_SELL signal, got {signal}"

    # Verify audit trail
    audit_data = await redis_client.redis.get(f"signals:audit:meanhunter")
    assert audit_data, "Audit trail not found"
    audit_data = json.loads(audit_data)
    assert audit_data['symbol'] == "RELIANCE-EQ"
    assert audit_data['signal'] in ["BUY", "PARTIAL_SELL:50:partial_1"]
    assert audit_data['reason'] in ["Entry rule triggered", "Target triggered (PARTIAL_SELL:50:partial_1)"]

    await strategy_engine.stop()
    log_file = "logs/strategy/rule_engine_2025-07-19.log"
    assert os.path.exists(log_file)
    with open(log_file, 'r') as f:
        assert "RuleEngine for meanhunter initialized successfully" in f.read()