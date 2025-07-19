# D:\AlphaNivesh\ANQuant\src\anquant\py\main.py
import asyncio
import yaml
import os
from typing import List
from src.anquant.py.util.logging import setup_logging
from src.anquant.py.util.config_loader import load_config
from src.anquant.py.messaging.redis_client import RedisClient
from src.anquant.py.util.database import Database
from src.anquant.py.core.adapters import get_adapters
from anquant.py.data_management.market_data.market_data_engine import MarketDataEngine
from anquant.py.indicators.indicator_engine import IndicatorEngine
from src.anquant.py.core.strategy.strategy_engine import StrategyEngine  # Updated import
from src.anquant.py.core.risk_management.risk_management import RiskManagementEngine
from anquant.py.order_management.order_execution import OrderExecutionEngine
from anquant.py.portfolio.portfolio_manager import PortfolioManager
from anquant.py.data_management.historical.historical_data_manager import HistoricalDataManager
from anquant.py.corporate_actions.corporate_actions_manager import CorporateActionManager
from asyncio import iscoroutine, Future

logger = setup_logging("main", log_type="general")

async def load_watchlist(watchlist_path: str) -> List[str]:
    """Load tradingsymbols from a watchlist file."""
    try:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
        absolute_path = os.path.join(project_root, watchlist_path)
        logger.debug(f"Attempting to load watchlist from: {absolute_path}")
        with open(absolute_path, 'r', encoding='utf-8') as f:
            watchlist = yaml.safe_load(f)['stocks']
        return [stock['tradingsymbol'] for stock in watchlist]
    except Exception as e:
        logger.error(f"Failed to load watchlist from {watchlist_path}: {e}")
        raise

async def main():
    logger.debug("Starting ANQuant Trading application")
    try:
        config = load_config("config/config.yaml")
        redis_client = RedisClient(config['global']['redis'])
        database = Database(config["global"]["database"])
        adapters = get_adapters(config)
        watchlists = {}
        for market in config['global']['brokers']['active_brokers']:
            watchlist_path = config['global']['markets'][market]['watchlists'][
                'meanhunter' if market == 'india' else 'xyzstrategy']
            watchlists[market] = await load_watchlist(watchlist_path)
            logger.debug(f"Loaded watchlist for {market}: {watchlists[market]}")

        components = [
            MarketDataEngine(config, redis_client, adapters, watchlists),
            *[IndicatorEngine(config, redis_client, adapters) for _ in range(4)],
            StrategyEngine(config, redis_client),
            RiskManagementEngine(config),
            OrderExecutionEngine(config),
            PortfolioManager(config, redis_client, database),
            HistoricalDataManager(config, redis_client, database),
            CorporateActionManager(config, redis_client)
        ]

        logger.debug("Initializing components")
        await asyncio.gather(*(component.initialize() for component in components))

        logger.debug("Starting components")
        tasks = []
        for component in components:
            start_result = component.start()
            if isinstance(start_result, Future):
                tasks.append(start_result)
            elif iscoroutine(start_result):
                tasks.append(asyncio.create_task(start_result))
            else:
                raise TypeError(f"Unexpected start result type for {component}: {type(start_result)}")

        try:
            await asyncio.sleep(5)
            logger.info("Test run completed successfully")
        except asyncio.CancelledError:
            logger.info("Test run cancelled")
            raise
    except Exception as e:
        logger.error(f"Application failed: {str(e)}", exc_info=True)
        raise
    finally:
        logger.info("Shutting down ANQuant application")
        for component in components:
            if hasattr(component, 'stop'):
                try:
                    await component.stop()
                except Exception as e:
                    logger.error(f"Failed to stop component {component}: {str(e)}")
        for market, broker_adapters in adapters.items():
            for broker, adapter in broker_adapters.items():
                try:
                    await adapter.disconnect()
                except Exception as e:
                    logger.error(f"Failed to disconnect broker {broker} for market {market}: {str(e)}")
        await redis_client.close()
        database.close()
        logger.info("Exiting ANQuant application")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt; shutting down")
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task(loop)]
        for task in tasks:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.run_until_complete(loop.shutdown_default_executor())
    except Exception as e:
        logger.error(f"Application failed: {str(e)}", exc_info=True)
        raise
    finally:
        loop.close()
        logger.info("Event loop closed")