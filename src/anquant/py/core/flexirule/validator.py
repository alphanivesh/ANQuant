# D:\AlphaNivesh\ANQuant\src\anquant\py\core\flexirule\validator.py
from pydantic import BaseModel, ValidationError, Field
from typing import List, Dict, Any, Optional
from src.anquant.py.util.logging import setup_logging
from src.anquant.py.util.config_loader import load_config
import yaml
import os

logger = setup_logging("flexirule_validator", log_type="strategy")


class IndicatorConfig(BaseModel):
    name: str
    type: str
    period: Optional[int] = None
    std: Optional[float] = None
    fast: Optional[int] = None
    slow: Optional[int] = None
    signal: Optional[int] = None


class PatternConfig(BaseModel):
    name: str
    type: str
    lookback: Optional[int] = 20
    criteria: Optional[str] = ""


class RuleConfig(BaseModel):
    condition: str
    weight: float = Field(ge=0.0, le=1.0)


class StopLossRule(BaseModel):
    type: str = Field(pattern=r"^(fixed|trailing)$")
    value: str = Field(pattern=r"^[0-9]+%?$")
    partial_exit: Optional[str] = None
    id: Optional[str] = None


class StopLossConfig(BaseModel):
    type: str = Field(pattern=r"^(fixed|trailing|multi)$")
    rules: Optional[List[StopLossRule]] = None


class TargetRule(BaseModel):
    type: str = Field(pattern=r"^(fixed|trailing)$")
    value: str = Field(pattern=r"^[0-9]+%?$")
    partial_exit: Optional[str] = None
    id: Optional[str] = None


class TargetConfig(BaseModel):
    type: str = Field(pattern=r"^(fixed|trailing|multi)$")
    rules: Optional[List[TargetRule]] = None


class TradeManagementConfig(BaseModel):
    breakeven: Optional[Dict[str, float]] = None


class MarketParamsConfig(BaseModel):
    india: Optional[Dict[str, str]] = None


class StrategyConfig(BaseModel):
    name: str
    timeframe: str = Field(pattern=r"^(1min|5min|15min|30min|1hr)$")
    watchlist: Dict[str, str]
    threshold: Optional[float] = 0.75
    indicators: Optional[List[IndicatorConfig]] = []
    patterns: Optional[List[PatternConfig]] = []
    entry_rules: List[RuleConfig]
    exit_rules: List[RuleConfig]
    stop_loss: Optional[StopLossConfig] = None
    target: Optional[TargetConfig] = None
    trade_management: Optional[TradeManagementConfig] = None
    market_params: Optional[MarketParamsConfig] = None


class StrategyValidator:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.supported_markets = list(config["global"]["markets"].keys())
        self.supported_timeframes = config["global"]["historical_data"]["timeframes"]
        logger.info("Initialized StrategyValidator with supported markets: %s, timeframes: %s",
                    self.supported_markets, self.supported_timeframes)

    def validate_strategy(self, strategy_path: str) -> bool:
        """Validate a strategy YAML file."""
        try:
            with open(strategy_path, 'r', encoding='utf-8') as f:
                strategy_data = yaml.safe_load(f)

            # Validate using Pydantic
            strategy_config = StrategyConfig(**strategy_data)

            # Check watchlist existence
            for market, watchlist_path in strategy_config.watchlist.items():
                if market not in self.supported_markets:
                    logger.error(f"Unsupported market {market} in strategy {strategy_config.name}")
                    return False
                if not os.path.exists(watchlist_path):
                    logger.error(f"Watchlist file not found: {watchlist_path}")
                    return False

            # Validate timeframe
            if strategy_config.timeframe not in self.supported_timeframes:
                logger.error(f"Invalid timeframe {strategy_config.timeframe} in strategy {strategy_config.name}")
                return False

            # Validate indicator types
            supported_indicators = ["bollinger_bands", "rsi", "sma", "macd"]
            for indicator in strategy_config.indicators:
                if indicator.type not in supported_indicators:
                    logger.error(f"Unsupported indicator type {indicator.type} in strategy {strategy_config.name}")
                    return False

            # Validate pattern types
            supported_patterns = ["smc", "price_action", "harmonic", "wave"]
            for pattern in strategy_config.patterns:
                if pattern.type not in supported_patterns:
                    logger.error(f"Unsupported pattern type {pattern.type} in strategy {strategy_config.name}")
                    return False

            # Validate stop-loss and target rules
            if strategy_config.stop_loss and strategy_config.stop_loss.type == "multi" and not strategy_config.stop_loss.rules:
                logger.error(f"Multi stop-loss requires rules in strategy {strategy_config.name}")
                return False
            if strategy_config.target and strategy_config.target.type == "multi" and not strategy_config.target.rules:
                logger.error(f"Multi target requires rules in strategy {strategy_config.name}")
                return False

            logger.info(f"Successfully validated strategy: {strategy_config.name}")
            return True
        except ValidationError as e:
            logger.error(f"Validation error for strategy {strategy_path}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error validating strategy {strategy_path}: {e}", exc_info=True)
            return False