import pytest
import asyncio
from src.py.main import main
from src.py.util.config_loader import load_config

@pytest.mark.asyncio
async def test_component_initialization():
    config = load_config("config/config.yaml")
    config["global"]["offline_mode"] = True
    task = asyncio.create_task(main())
    await asyncio.sleep(1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    # Check logs/general/main.log for initialization messages