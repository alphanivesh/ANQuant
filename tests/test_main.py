import pytest
import asyncio
from src.py.main import main, load_watchlist
from src.py.util.config_loader import load_config

@pytest.mark.asyncio
async def test_main_initialization():
    config = load_config("config/config.yaml")
    watchlists = {}
    for market in config['global']['brokers']['active_brokers']:
        watchlist_path = config['global']['markets'][market]['watchlists']['meanhunter' if market == 'india' else 'xyzstrategy']
        watchlists[market] = await load_watchlist(watchlist_path)
    assert 'india' in watchlists
    assert 'usa' in watchlists
    assert len(watchlists['india']) > 0
    assert len(watchlists['usa']) > 0
    # Run main briefly in offline mode
    config['global']['offline_mode'] = True
    task = asyncio.create_task(main())
    await asyncio.sleep(1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass