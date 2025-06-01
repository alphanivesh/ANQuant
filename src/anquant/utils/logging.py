from loguru import logger
import sys


def setup_logging(config: dict):
    """Configure Loguru for Log4j-like logging."""
    try:
        logger.remove()

        # Console output
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            level=config["logging"]["level"]
        )

        # File output with rotation
        logger.add(
            "logs/anquant_{time:YYYYMMDD}.log",
            rotation=config["logging"]["rotation"],
            retention=config["logging"]["retention"],
            compression="zip",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level=config["logging"]["level"]
        )

        # Structured JSON logs
        logger.add(
            "logs/anquant_structured_{time:YYYYMMDD}.json",
            rotation=config["logging"]["rotation"],
            format="{time} | {level} | {message} | {extra}",
            serialize=True,
            level="INFO"
        )

        logger.info("Logging setup complete")
    except Exception as e:
        logger.error(f"Failed to setup logging: {e}")
        raise