"""
Main Data Processing Logic for Account Log Cleaner

Orchestrates the entire data processing pipeline including:
- File extraction and validation
- Data cleaning and normalization
- Data enrichment
- Database insertion
"""

import logging
from pathlib import Path
from typing import List, Optional
from datetime import datetime

import pandas as pd

from app.config import Config
from app.exceptions import FileProcessingError, DataValidationError, DatabaseError
from app.file_handler import FileHandler
from app.data_processor import DataProcessor
from app.database import DatabaseManager


class LogProcessor:
    """
    Main processor class that orchestrates the entire data processing pipeline.
    
    Responsibilities:
        - Coordinate file processing
        - Manage data transformation pipeline
        - Handle database operations
        - Provide progress tracking and logging
    """
    
    def __init__(self, config: Config, logger: logging.Logger) -> None:
        """
        Initialize the log processor.
        
        Args:
            config: Configuration instance.
            logger: Configured logger instance.
        """
        self.config = config
        self.logger = logger
        self.file_handler = FileHandler(config, logger)
        self.data_processor = DataProcessor(config, logger)
        self.db_manager = DatabaseManager(config, logger)
        
        self.processed_files = 0
        self.processed_records = 0
        self.failed_files = []
    
    def run(self) -> None:
        """
        Execute the main processing pipeline.
        
        Pipeline steps:
            1. Extract log files
            2. Validate files
            3. Load and clean data
            4. Enrich data
            5. Insert into database
            6. Archive processed files
        
        Raises:
            FileProcessingError: If file extraction fails.
        """
        try:
            self.logger.info(f"Input path: {self.config.LOGS_INPUT_PATH}")
            
            # Extract files
            files = self.file_handler.extract_wanted_files()
            self.logger.info(f"Found {len(files)} files to process")
            
            if not files:
                self.logger.warning("No files found to process")
                return
            
            # Process each file
            for file_path in files:
                try:
                    self._process_file(file_path)
                except Exception as e:
                    self.logger.error(f"Error processing file {file_path}: {str(e)}", exc_info=True)
                    self.failed_files.append(str(file_path))
                    continue
            
            # Archive processed files
            self.logger.info("Archiving processed files...")
            self.file_handler.move_logs(files)
            
            # Log summary
            self._log_summary()
            
        except Exception as e:
            self.logger.error(f"Fatal error in processing pipeline: {str(e)}", exc_info=True)
            raise
    
    def _process_file(self, file_path: Path) -> None:
        """
        Process a single log file through the entire pipeline.
        
        Args:
            file_path: Path to the file to process.
            
        Raises:
            FileProcessingError: If file processing fails.
            DataValidationError: If data validation fails.
            DatabaseError: If database insert fails.
        """
        self.logger.info(f"Processing file: {file_path.name}")
        
        # Detect file type
        file_type = self.file_handler.detect_file_type(file_path)
        self.logger.debug(f"Detected file type: {file_type}")
        
        # Load data
        data = self.file_handler.load_text_files(file_path, file_type)
        
        if data.empty:
            self.logger.warning(f"File {file_path.name} is empty, skipping")
            return
        
        self.logger.debug(f"Loaded {len(data)} records from {file_path.name}")
        
        # Clean data
        data = self.data_processor.make_data_clean(data, file_type)
        self.logger.debug(f"Data cleaning completed")
        
        # Enrich data
        data = self.data_processor.enrich_data(data, file_path.name, file_type)
        self.logger.debug(f"Data enrichment completed")
        
        # Save pickle
        if self.config.ENABLE_PICKLE:
            self.data_processor.create_pickle(data, file_type)
            self.logger.debug(f"Pickle file saved")
        
        # Insert into database
        if self.config.ENABLE_DB_INSERT and not self.config.DRY_RUN:
            self.db_manager.insert_data(data, file_type)
            self.logger.info(f"Inserted {len(data)} records to database")
        elif self.config.DRY_RUN:
            self.logger.info(f"[DRY RUN] Would insert {len(data)} records to database")
        
        self.processed_files += 1
        self.processed_records += len(data)
        self.logger.info(f"File {file_path.name} processed successfully ({len(data)} records)")
    
    def _log_summary(self) -> None:
        """
        Log processing summary statistics.
        """
        self.logger.info("=" * 80)
        self.logger.info("PROCESSING SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"Total files processed: {self.processed_files}")
        self.logger.info(f"Total records processed: {self.processed_records}")
        self.logger.info(f"Failed files: {len(self.failed_files)}")
        
        if self.failed_files:
            self.logger.warning(f"Failed files: {', '.join(self.failed_files)}")
        
        self.logger.info("=" * 80)
