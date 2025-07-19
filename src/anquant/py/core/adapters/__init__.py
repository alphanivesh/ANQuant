from typing import Dict
from src.anquant.py.util.config_loader import load_credentials, load_symbol_mappings
from .base_adapter import BaseAdapter
from .angelone import AngelOneAdapter
try:
    from .interactive_brokers import InteractiveBrokersAdapter
except ImportError:
    InteractiveBrokersAdapter = None  # Fallback if not implemented
from src.anquant.py.util.config_loader import load_credentials, load_symbol_mappings

from loguru import logger

def get_adapters(config: Dict) -> Dict[str, Dict[str, BaseAdapter]]:
    """
    Factory function to return a dictionary of broker adapters for each market and active broker.
    Returns: {market: {broker: adapter_instance}}
    """
    try:
        adapters = {}
        active_brokers = config['global']['brokers']['active_brokers']
        for market, brokers in active_brokers.items():
            adapters[market] = {}
            for broker in brokers:
                credentials = load_credentials(config, market, broker)
                mappings = load_symbol_mappings(config, market, broker)
                kafka_config = config['global']['kafka']
                if broker == 'angelone':
                    logger.info(f"Loading AngelOneAdapter for market: {market}")
                    adapters[market][broker] = AngelOneAdapter(credentials, mappings, kafka_config, config)
                elif broker == 'interactive_brokers':
                    if InteractiveBrokersAdapter is None:
                        logger.warning(f"InteractiveBrokersAdapter not available for market: {market}")
                        continue
                    logger.info(f"Loading InteractiveBrokersAdapter for market: {market}")
                    adapters[market][broker] = InteractiveBrokersAdapter(credentials, mappings, kafka_config, config)
                else:
                    logger.error(f"Unknown broker: {broker} for market: {market}")
                    raise ValueError(f"Unknown broker: {broker}")
        logger.info(f"Loaded adapters for markets: {list(adapters.keys())}")
        return adapters
    except Exception as e:
        logger.error(f"Failed to load adapters: {e}")
        raise