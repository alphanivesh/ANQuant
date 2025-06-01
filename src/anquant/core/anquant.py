# src/anquant/core/anquant.py
import os
from typing import Dict, List, Any
from loguru import logger

from src.anquant.adapters.base_broker import BaseBroker
from src.anquant.core.factory import Factory
from src.anquant.strategies.base_strategy import BaseStrategy
from src.anquant.utils.helpers import load_config
from src.anquant.data.data_fetcher import DataFetcher
from src.anquant.portfolio.portfolio_manager import PortfolioManager
from src.anquant.feed.market_feed_handler import MarketFeedHandler


class ANQuant:
    """Main class for the AlphaNivesh Quant (ANQuant) system."""

    def __init__(self, config_path: str):
        self.config = load_config(config_path)
        self.rsi_threshold = self.config.get("rsi_threshold", 30)
        self.factory = Factory()
        self.broker: BaseBroker = self.factory.create_broker(self.config["broker"]["name"], self.config["broker"])

        self.data_fetcher = DataFetcher(self.broker)
        self.portfolio_manager = PortfolioManager(self.broker)
        self.market_feed_handler = MarketFeedHandler(self.broker)

        strategies_configs = self.config.get("strategies", [])
        self.strategies: List[BaseStrategy] = []

        for strategy_config in strategies_configs:
            strategy_name = strategy_config["strategy_name"]
            strategy_type = strategy_config.get("strategy_type", "technical")
            sub_type = strategy_config.get("sub_type", "momentum")
            time_horizon = strategy_config.get("time_horizon", "positional")
            config = strategy_config.get("config", {})

            self.validate_strategy_metadata(strategy_name, strategy_type, sub_type, time_horizon)

            config["strategy_type"] = strategy_type
            config["sub_type"] = sub_type
            config["time_horizon"] = time_horizon

            strategy = self.factory.create_strategy(strategy_name, config, self.broker)
            self.strategies.append(strategy)
            logger.info(f"Loaded strategy: {strategy_name} ({strategy_type}, {sub_type}, {time_horizon})")

        logger.info("ANQuant initialized")

    def validate_strategy_metadata(self, strategy_name: str, strategy_type: str, sub_type: str, time_horizon: str) -> None:
        expected_metadata = {
            "momentum": ("technical", "momentum", "positional"),
            "moving_average_crossover": ("technical", "momentum", "swing"),
            "dividend_yield": ("fundamental", "value", "positional"),
        }

        expected = expected_metadata.get(strategy_name, ("technical", "momentum", "positional"))
        expected_type, expected_sub_type, expected_horizon = expected

        if strategy_type != expected_type:
            logger.warning(f"Strategy {strategy_name}: Expected strategy_type {expected_type}, but got {strategy_type}")
        if sub_type != expected_sub_type:
            logger.warning(f"Strategy {strategy_name}: Expected sub_type {expected_sub_type}, but got {sub_type}")
        if time_horizon != expected_horizon:
            logger.warning(f"Strategy {strategy_name}: Expected time_horizon {expected_horizon}, but got {time_horizon}")

    def authenticate(self):
        self.broker.authenticate()

    def start(self):
        try:
            logger.info("Starting ANQuant")
            self.authenticate()
            self.data_fetcher.fetch_nse_500_historical_data(self.strategies)  # Remove rsi_threshold
            portfolio = self.portfolio_manager.get_portfolio()
            self.portfolio_manager.process_portfolio(portfolio, self.strategies)
            # Subscribe to SBIN-EQ for now
            self.market_feed_handler.start_market_feed(
                [{"exchange": "NSE", "tradingsymbol": "SBIN-EQ", "symboltoken": "3045"}],
                self.on_market_data
            )
        except Exception as e:
            logger.error(f"Failed to start ANQuant: {e}", exc_info=True)
            raise

    def on_market_data(self, data: Dict[str, Any]) -> None:
        self.market_feed_handler.on_market_data(data, self.strategies)

    def stop(self):
        try:
            self.market_feed_handler.stop_market_feed()
            logger.info("ANQuant stopped")
        except Exception as e:
            logger.error(f"Failed to stop ANQuant: {e}")
            raise