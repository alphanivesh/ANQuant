from src.py.util.config_loader import load_config
from src.py.core.adapters import get_adapters
import asyncio
import yaml


async def main():
    config = load_config("config/config.yaml")
    adapters = get_adapters(config)

    # Load watchlists
    with open(config['global']['markets']['india']['watchlists']['meanhunter'], 'r') as f:
        india_watchlist = [stock['tradingsymbol'] for stock in yaml.safe_load(f)['stocks']]
    with open(config['global']['markets']['usa']['watchlists']['xyzstrategy'], 'r') as f:
        usa_watchlist = [stock['tradingsymbol'] for stock in yaml.safe_load(f)['stocks']]

    await adapters['india']['angelone'].connect()
    await adapters['usa']['interactive_brokers'].connect()
    await adapters['india']['angelone'].subscribe_to_ticks(india_watchlist)
    await adapters['usa']['interactive_brokers'].subscribe_to_ticks(usa_watchlist)
    await asyncio.sleep(60)
    await adapters['india']['angelone'].disconnect()
    await adapters['usa']['interactive_brokers'].disconnect()


if __name__ == "__main__":
    asyncio.run(main())