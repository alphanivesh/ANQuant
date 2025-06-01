# src/anquant/core/factory.py
from typing import Dict, Any
import importlib
from src.anquant.adapters.angel_one import AngelOneAdapter
from src.anquant.strategies.base_strategy import BaseStrategy
from src.anquant.adapters.base_broker import BaseBroker


class Factory:
    """Factory class to create broker and strategy instances based on configuration."""

    def __init__(self):
        self._broker_classes = {
            "angelone": AngelOneAdapter
        }
        self._strategy_classes = {}
        self._load_strategies()

    def _load_strategies(self):
        """Dynamically load all strategy classes from the strategies directory."""
        strategy_paths = [
            ("technical.positional.momentum.momentum_strategy", "MomentumStrategy"),
            ("technical.swing.momentum.moving_average_crossover", "MovingAverageCrossoverStrategy"),
            ("fundamental.value.dividend_yield_strategy", "DividendYieldStrategy"),
        ]

        for module_path, class_name in strategy_paths:
            try:
                module = importlib.import_module(f"src.anquant.strategies.{module_path}")
                strategy_class = getattr(module, class_name)
                strategy_name = module_path.split(".")[-1].replace("_strategy", "")
                self._strategy_classes[strategy_name] = strategy_class
            except Exception as e:
                print(f"Failed to load strategy {class_name}: {e}")

    def create_broker(self, broker_name: str, config: Dict[str, str]) -> BaseBroker:
        broker_name = broker_name.lower()
        broker_class = self._broker_classes.get(broker_name)
        if broker_class is None:
            raise ValueError(f"Unsupported broker: {broker_name}")
        return broker_class(config)

    def create_strategy(self, strategy_name: str, config: Dict[str, Any], broker: BaseBroker = None) -> BaseStrategy:
        strategy_name = strategy_name.lower()
        strategy_class = self._strategy_classes.get(strategy_name)
        if strategy_class is None:
            raise ValueError(f"Unsupported strategy: {strategy_name}")
        return strategy_class(strategy_name, config, broker)

    def register_broker(self, broker_name: str, broker_class: type) -> None:
        self._broker_classes[broker_name.lower()] = broker_class

    def register_strategy(self, strategy_name: str, strategy_class: type) -> None:
        self._strategy_classes[strategy_name.lower()] = strategy_class