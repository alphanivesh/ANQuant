# src/py/anquant/util/config_loader.py
import os
import yaml
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""

    def format(self, record):
        log_record = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "line": record.lineno,
            "process": record.process,
            "thread": record.thread
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)


def setup_logging(name: str, log_type: str = "general") -> logging.Logger:
    """Configure logger with JSON formatting and file output."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    try:
        log_dir = os.path.join("logs", log_type)
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(log_dir, f"{name}_{datetime.now().strftime('%Y-%m-%d')}.log")
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(JSONFormatter())

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(JSONFormatter())

        logger.handlers = [file_handler, console_handler]
        logger.propagate = False

        logger.info(f"Logging initialized for {name} (type: {log_type}, file: {log_file})")
        return logger
    except Exception as e:
        fallback_logger = logging.getLogger(f"{name}_fallback")
        fallback_logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(JSONFormatter())
        fallback_logger.handlers = [console_handler]
        fallback_logger.error(f"Failed to initialize logging for {name} (type: {log_type}): {e}", exc_info=True)
        return fallback_logger


logger = setup_logging("config_loader", log_type="general")


def load_config(config_path: str = "config/config.yaml", require_global: bool = True) -> Dict[str, Any]:
    """Load and validate configuration from a YAML file."""
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))  # ANQuant/
    absolute_path = os.path.join(project_root, config_path)
    logger.info(f"Loading configuration from {absolute_path}")
    try:
        if not os.path.exists(absolute_path):
            logger.error(f"Configuration file {absolute_path} does not exist")
            raise FileNotFoundError(f"Configuration file {absolute_path} not found")

        with open(absolute_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        if config is None:
            logger.error(f"Configuration file {absolute_path} is empty or invalid")
            raise ValueError(f"Configuration file {absolute_path} is empty or invalid")

        # Validate key sections
        if require_global:
            required_sections = ['global']
            for section in required_sections:
                if section not in config:
                    logger.error(f"Missing required section '{section}' in {absolute_path}")
                    raise ValueError(f"Missing required section '{section}' in {absolute_path}")

            # Validate global sub-sections
            global_required = ['kafka', 'redis', 'database', 'historical_data', 'strategies', 'markets', 'audit']
            for key in global_required:
                if key not in config['global']:
                    logger.warning(f"Missing global section '{key}' in {absolute_path}, using default configuration")
                    config['global'][key] = {}

            # Log loaded markets
            markets = list(config['global']['markets'].keys()) if 'markets' in config['global'] else []
            logger.info(f"Loaded markets: {markets}")

        logger.info(f"Successfully loaded and validated configuration from {absolute_path}")
        logger.debug(f"Configuration content: {json.dumps(config, default=str)}")
        return config
    except yaml.YAMLError as e:
        logger.error(f"Failed to parse YAML in {absolute_path}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Failed to load configuration from {absolute_path}: {e}", exc_info=True)
        raise


def load_symbol_mappings(config: Dict[str, Any], market: str, broker: str) -> Dict[str, Any]:
    """Load broker-specific symbol mappings for a given market."""
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))
    try:
        mappings_path = config["global"]["markets"][market]["brokers"][broker]["symbols"]
        absolute_path = os.path.join(project_root, mappings_path)
        logger.debug(f"Attempting to load symbol mappings from: {absolute_path}")

        if not os.path.exists(absolute_path):
            logger.error(f"Symbol mappings file {absolute_path} not found")
            raise FileNotFoundError(f"Symbol mappings file {absolute_path} not found")

        with open(absolute_path, 'r', encoding='utf-8') as f:
            mappings = yaml.safe_load(f)

        if mappings is None:
            logger.warning(f"Symbol mappings file {absolute_path} is empty, returning empty dict")
            mappings = {}

        logger.info(f"Loaded symbol mappings for {market}/{broker} from {absolute_path}")
        logger.debug(f"Symbol mappings content: {json.dumps(mappings, default=str)}")
        return mappings
    except yaml.YAMLError as e:
        logger.error(f"Failed to parse YAML in {absolute_path}: {e}", exc_info=True)
        raise
    except KeyError as e:
        logger.error(f"Invalid config structure for {market}/{broker} symbol mappings: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Failed to load symbol mappings for {market}/{broker}: {e}", exc_info=True)
        raise


def load_credentials(config: Dict[str, Any], market: str, broker: str) -> Dict[str, Any]:
    """Load credentials for a given market and broker from file or Vault."""
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))
    try:
        offline_mode = config['global'].get('offline_mode', False)
        vault_enabled = config['global'].get('vault', {}).get('enabled', False)

        if offline_mode or not vault_enabled:
            cred_path = config['global']['markets'][market]['brokers'][broker]['credentials']
            absolute_path = os.path.join(project_root, cred_path)
            logger.debug(f"Attempting to load credentials from: {absolute_path}")

            if not os.path.exists(absolute_path):
                logger.error(f"Credentials file {absolute_path} not found")
                raise FileNotFoundError(f"Credentials file {absolute_path} not found")

            with open(absolute_path, 'r', encoding='utf-8') as f:
                credentials = yaml.safe_load(f)

            if credentials is None:
                logger.error(f"Credentials file {absolute_path} is empty")
                raise ValueError(f"Credentials file {absolute_path} is empty")

            logger.info(f"Loaded credentials from {absolute_path}")
            logger.debug(f"Credentials content (sanitized): {json.dumps({k: '***' for k in credentials.keys()})}")
            return credentials
        else:
            from hvac import Client
            vault_url = config['global']['vault'].get('url', 'http://vault:8200')
            logger.debug(f"Attempting to load credentials from Vault at {vault_url}")

            vault_client = Client(url=vault_url)
            vault_path = f"anquant/secrets/{market}/{broker}"
            secret = vault_client.secrets.kv.v2.read_secret_version(path=vault_path, mount_point='anquant')
            credentials = secret['data']['data']

            logger.info(f"Loaded credentials from Vault for {market}/{broker}")
            logger.debug(f"Credentials content (sanitized): {json.dumps({k: '***' for k in credentials.keys()})}")
            return credentials
    except yaml.YAMLError as e:
        logger.error(f"Failed to parse YAML credentials for {market}/{broker}: {e}", exc_info=True)
        raise
    except KeyError as e:
        logger.error(f"Invalid config structure for {market}/{broker} credentials: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Failed to load credentials for {market}/{broker}: {e}", exc_info=True)
        raise