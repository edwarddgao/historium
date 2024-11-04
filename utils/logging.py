# utils/logging.py
import logging
import sys
from typing import Optional
from datetime import datetime
from pathlib import Path

def setup_logging(level: str = "INFO", log_file: Optional[str] = None):
    """Configure logging with better formatting and error handling"""
    # Create formatters
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Set up root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Console handler with color (if supported)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (if specified)
    if log_file:
        try:
            # Ensure directory exists
            log_dir = Path(log_file).parent
            log_dir.mkdir(parents=True, exist_ok=True)
            
            # Add timestamp to log filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_path = log_dir / f"{Path(log_file).stem}_{timestamp}{Path(log_file).suffix}"
            
            file_handler = logging.FileHandler(log_path)
            file_handler.setFormatter(file_formatter)
            root_logger.addHandler(file_handler)
            
            logging.info(f"Logging to file: {log_path}")
        except Exception as e:
            logging.error(f"Failed to set up file logging: {e}")
    
    # Quiet some noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("motor").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    
    # Log library versions
    logging.info("Starting museum crawler")
    try:
        import aiohttp
        import motor
        import pymongo
        logging.info(f"aiohttp version: {aiohttp.__version__}")
        logging.info(f"motor version: {motor.version}")
        logging.info(f"pymongo version: {pymongo.version}")
    except Exception as e:
        logging.warning(f"Failed to log library versions: {e}")

def log_error_with_context(logger: logging.Logger, message: str, error: Exception, **context):
    """Helper function to log errors with additional context"""
    error_details = {
        "error_type": type(error).__name__,
        "error_message": str(error),
        **context
    }
    logger.error(f"{message}: {error_details}")