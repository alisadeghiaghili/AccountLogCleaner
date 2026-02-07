"""
Account Log Cleaner
A robust, production-ready application for processing and cleaning account validation logs.

This module serves as the main entry point for the Account Log Cleaner application.
It orchestrates the entire data processing pipeline from file extraction to database insertion.

Author: Ali Sadeghi Aghili
License: MIT
"""

import sys
import logging
from pathlib import Path

from app.config import Config
from app.logger import setup_logger
from app.processor import LogProcessor
from app.exceptions import AccountLogCleanerError


def main() -> None:
    """
    Main entry point for the Account Log Cleaner application.
    
    Raises:
        AccountLogCleanerError: If critical errors occur during processing.
        SystemExit: On fatal errors.
    """
    try:
        # Initialize configuration
        config = Config()
        
        # Setup logging
        logger = setup_logger(
            name="AccountLogCleaner",
            log_level=config.LOG_LEVEL,
            log_file=config.LOG_FILE
        )
        
        logger.info("=" * 80)
        logger.info("Account Log Cleaner Started")
        logger.info("=" * 80)
        logger.debug(f"Configuration initialized")
        
        # Initialize and run processor
        processor = LogProcessor(config, logger)
        processor.run()
        
        logger.info("=" * 80)
        logger.info("Account Log Cleaner Completed Successfully")
        logger.info("=" * 80)
        
    except AccountLogCleanerError as e:
        logger.error(f"Application Error: {str(e)}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        if 'logger' in locals():
            logger.critical(f"Unexpected Error: {str(e)}", exc_info=True)
        else:
            print(f"Critical Error (logger not initialized): {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
