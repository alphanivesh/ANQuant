# src/anquant/main.py
import os
import sys
from pathlib import Path
from loguru import logger
from src.anquant.core.anquant import ANQuant
from src.anquant.utils.helpers import load_config


if __name__ == "__main__":
    # Define the path to the config file using pathlib for better path handling
    config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"

    # Resolve the absolute path
    config_path = config_path.resolve()
    logger.info(f"Computed config path: {config_path}")

    # Check if the config file exists
    if not config_path.exists():
        logger.error(
            f"Configuration file not found at {config_path}. Please create the config.yaml file with the required settings.")
        logger.error("Expected structure of config.yaml:")
        logger.error("""
        broker:
          name: "angelone"
          credentials:
            api_key: "your_api_key"
            client_code: "your_client_id"
            pin: "your_pin"
            totp_token: "your_totp_token"
        logging:
          level: "DEBUG"
          rotation: "10 MB"
          retention: "30 days"
        testing:
          place_orders: false
        strategies:
          - strategy_name: "momentum"
            strategy_type: "technical"
            sub_type: "momentum"
            time_horizon: "positional"
            config:
              reference_price: 800
          - strategy_name: "moving_average_crossover"
            strategy_type: "technical"
            sub_type: "momentum"
            time_horizon: "swing"
            config:
              short_ma_length: 20
              long_ma_length: 50
        """)
        raise FileNotFoundError(f"Configuration file not found at {config_path}")

    # Load config to configure logging
    config = load_config(config_path)
    logging_config = config.get("logging", {})

    # Remove default handler
    logger.remove()

    # Add file-based handler with rotation and retention
    log_file = "logs/app.log"
    os.makedirs(os.path.dirname(log_file), exist_ok=True)  # Ensure the logs directory exists
    logger.add(
        log_file,
        level=logging_config.get("level", "DEBUG"),
        rotation=logging_config.get("rotation", "10 MB"),
        retention=logging_config.get("retention", "30 days"),
        format="{time} | {level} | {message}"
    )

    # Add console handler (sys.stderr) without rotation/retention
    logger.add(
        sys.stderr,
        level=logging_config.get("level", "DEBUG"),
        format="{time} | {level} | {message}"
    )

    # Instantiate and run ANQuant
    anquant = None
    try:
        anquant = ANQuant(str(config_path))
        anquant.start()
    except Exception as e:
        logger.error(f"Application failed: {e}")
        sys.exit(1)
    finally:
        if anquant:
            anquant.stop()