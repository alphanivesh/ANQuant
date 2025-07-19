import os
import sys
import yaml
import stat
from loguru import logger
from datetime import datetime

def setup_logging(log_name: str, log_type: str = "general", log_dir: str = "logs", config_path: str = "config/config.yaml"):
    """Initialize logging with Loguru for the specified log name and type."""
    try:
        # Resolve project root (anquant/) relative to this file
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../'))  # src/anquant/py/util/ -> ANQuant/
        config_path = os.path.join(project_root, config_path)  # D:\AlphaNivesh\ANQuant\config\config.yaml
        log_dir = os.path.join(project_root, log_dir)  # D:\AlphaNivesh\ANQuant\logs
        logger.debug(f"Using config path: {config_path}, log dir: {log_dir}")

        # Load main config to get logging config path
        try:
            with open(config_path, "r", encoding='utf-8') as f:
                main_config = yaml.safe_load(f) or {}
            logging_config_path = main_config.get("global", {}).get("logging_config", "config/logging.yaml")
            logging_config_path = os.path.join(project_root, logging_config_path)  # D:\AlphaNivesh\ANQuant\config\logging.yaml
        except FileNotFoundError:
            logger.error(f"Main config file {config_path} not found; using default logging config path")
            logging_config_path = os.path.join(project_root, "config/logging.yaml")
        except Exception as e:
            logger.error(f"Failed to load main config from {config_path}: {e}; using default logging config path")
            logging_config_path = os.path.join(project_root, "config/logging.yaml")

        # Load logging configuration
        try:
            with open(logging_config_path, "r", encoding='utf-8') as f:
                config = yaml.safe_load(f) or {}
            logging_config = config.get(log_type, {})
            directory = os.path.abspath(os.path.join(project_root, logging_config.get("directory", "logs")))  # Ensure absolute path
            date_str = datetime.now().strftime("%Y-%m-%d")
            filename_template = logging_config.get("filename_template", "{name}_{date}.log").replace("{date}", date_str)
            if log_type == "kafka":
                filename_template = f"kafka/{log_name}_{date_str}.log"
            log_level = logging_config.get("level", "DEBUG").upper()
            rotation = logging_config.get("rotation", "10 MB")
            retention = logging_config.get("retention", "30 days")
            compression = logging_config.get("compression", "zip")
            log_format = logging_config.get("format", "{time:YYYY-MM-DD HH:mm:ss} | {level} | {name} | {message}")
            stdout_format = logging_config.get("stdout_format", "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{name}</cyan> | <level>{message}</level>")
            enqueue = logging_config.get("enqueue", True)
        except FileNotFoundError:
            logger.error(f"Logging config file {logging_config_path} not found; using default settings")
            directory = os.path.abspath(os.path.join(project_root, "logs"))  # Default to D:\AlphaNivesh\ANQuant\logs
            date_str = datetime.now().strftime("%Y-%m-%d")
            filename_template = f"{log_name}_{date_str}.log"
            if log_type == "kafka":
                filename_template = f"kafka/{log_name}_{date_str}.log"
            log_level = "DEBUG"
            rotation = "10 MB"
            retention = "30 days"
            compression = "zip"
            log_format = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {name} | {message}"
            stdout_format = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{name}</cyan> | <level>{message}</level>"
            enqueue = True
        except Exception as e:
            logger.error(f"Failed to load logging config from {logging_config_path}: {e}; using default settings")
            directory = os.path.abspath(os.path.join(project_root, "logs"))  # Default to D:\AlphaNivesh\ANQuant\logs
            date_str = datetime.now().strftime("%Y-%m-%d")
            filename_template = f"{log_name}_{date_str}.log"
            if log_type == "kafka":
                filename_template = f"kafka/{log_name}_{date_str}.log"
            log_level = "DEBUG"
            rotation = "10 MB"
            retention = "30 days"
            compression = "zip"
            log_format = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {name} | {message}"
            stdout_format = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{name}</cyan> | <level>{message}</level>"
            enqueue = True

        # Resolve absolute directory path
        directory = os.path.abspath(directory)
        logger.debug(f"Creating log directory: {directory}")

        # Create logs directory (outside src/) with write permissions
        try:
            os.makedirs(directory, exist_ok=True)
            os.chmod(directory, stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH | stat.S_IXOTH)
            if log_type == "kafka":
                kafka_dir = os.path.join(directory, "kafka")
                os.makedirs(kafka_dir, exist_ok=True)
                os.chmod(kafka_dir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH | stat.S_IXOTH)
        except OSError as e:
            logger.error(f"Failed to create or set permissions for directory {directory}: {e}")
            raise

        # Verify directory exists and is writable
        if not os.path.exists(directory):
            logger.error(f"Directory {directory} does not exist after creation attempt")
            raise OSError(f"Directory creation failed: {directory}")
        if not os.access(directory, os.W_OK):
            logger.error(f"No write permissions for directory {directory}")
            raise OSError(f"No write permissions: {directory}")

        # Format filename with log_name
        log_file = os.path.join(directory, filename_template.format(name=log_name))
        logger.debug(f"Log file path: {log_file}")

        # Verify log file can be created
        try:
            with open(log_file, 'a') as f:
                pass  # Test file creation
        except Exception as e:
            logger.error(f"Cannot create or write to log file {log_file}: {e}")
            raise

        # Create a new logger instance
        component_logger = logger.bind(name=log_name)
        component_logger.remove()  # Remove default handlers

        # Add file handler
        try:
            handler_id = component_logger.add(
                log_file,
                level=log_level,
                rotation=rotation,
                retention=retention,
                compression=compression,
                format=log_format,
                enqueue=enqueue,
                backtrace=True,
                diagnose=True
            )
            logger.debug(f"Added file handler for {log_file}, handler ID: {handler_id}")
        except Exception as e:
            logger.error(f"Failed to add file handler for {log_file}: {e}")
            raise

        # Add stdout handler
        component_logger.add(
            sys.stdout,
            level=log_level,
            format=stdout_format,
            colorize=True
        )

        component_logger.info(f"Logging initialized for {log_name} (type: {log_type}, file: {log_file})")
        return component_logger
    except Exception as e:
        logger.error(f"Failed to initialize logging for {log_name}: {e}", exc_info=True)
        logger.add(sys.stdout, level="DEBUG")
        return logger

# Global default logger if needed
default_logger = logger