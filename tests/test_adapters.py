from src.py.util.config_loader import load_config
from src.py.core.adapters import get_adapters
import pytest
import asyncio
import yaml

@pytest.mark.asyncio
async def test_multi_broker_initialization():
    config = load_config("config/config.yaml")
    adapters = get_adapters(config)
    assert 'india' in adapters
    assert 'usa' in adapters
    assert 'angelone' in adapters['india']
    assert 'interactive_brokers' in adapters['usa']
    assert len(adapters['india']['angelone'].mappings) > 0
    assert len(adapters['usa']['interactive_brokers'].mappings) > 0

    # Load watchlists
    with open(config['global']['markets']['india']['watchlists']['meanhunter'], 'r') as f:
        india_watchlist = [stock['tradingsymbol'] for stock in yaml.safe_load(f)['stocks']]
    with open(config['global']['markets']['usa']['watchlists']['xyzstrategy'], 'r') as f:
        usa_watchlist = [stock['tradingsymbol'] for stock in yaml.safe_load(f)['stocks']]

    if config['global']['offline_mode']:
        await adapters['india']['angelone'].connect()
        await adapters['usa']['interactive_brokers'].connect()
        await adapters['india']['angelone'].subscribe_to_ticks(india_watchlist)
        await adapters['usa']['interactive_brokers'].subscribe_to_ticks(usa_watchlist)
        assert adapters['india']['angelone'].authenticated