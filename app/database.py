"""
Database Operations for Account Log Cleaner

Handles:
- Database connection management
- Data insertion
- Transaction handling
- Connection pooling
"""

import logging
from typing import Optional

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, Engine
from sqlalchemy.pool import QueuePool

from app.config import Config
from app.data_processor import DataProcessor
from app.exceptions import DatabaseError


class DatabaseManager:
    """
    Manages all database operations.
    """
    
    # Table names by file type
    TABLE_MAPPING = {
        "ValidationOk": "ValidationOk",
        "ValidationError": "ValidationError",
    }
    
    def __init__(self, config: Config, logger: logging.Logger) -> None:
        """
        Initialize database manager.
        
        Args:
            config: Configuration instance.
            logger: Logger instance.
        """
        self.config = config
        self.logger = logger
        self.engine: Optional[Engine] = None
        
        if config.ENABLE_DB_INSERT:
            self._initialize_connection()
    
    def _initialize_connection(self) -> None:
        """
        Initialize database connection with connection pooling.
        
        Raises:
            DatabaseError: If connection fails.
        """
        try:
            connection_string = self.config.get_database_connection_string()
            
            self.engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_recycle=3600,  # Recycle connections after 1 hour
                echo=False,
                fast_executemany=True,
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))
            
            self.logger.info("Database connection established successfully")
        
        except Exception as e:
            raise DatabaseError(f"Failed to establish database connection: {str(e)}")
    
    def insert_data(self, data: pd.DataFrame, file_type: str) -> None:
        """
        Insert data into appropriate database table.
        
        Args:
            data: DataFrame to insert.
            file_type: Type of the file (determines table).
            
        Raises:
            DatabaseError: If insertion fails.
        """
        if not self.engine:
            raise DatabaseError("Database connection not initialized")
        
        try:
            table_name = self.TABLE_MAPPING.get(file_type)
            if not table_name:
                raise DatabaseError(f"Unknown file type: {file_type}")
            
            # Fix column sizes
            data = DataProcessor.fix_column_size(data, file_type)
            
            # Get column types
            dtype = DataProcessor.get_database_types(file_type)
            
            # Insert data
            data.to_sql(
                name=table_name,
                con=self.engine,
                schema=self.config.DB_SCHEMA,
                if_exists="append",
                index=False,
                dtype=dtype,
                chunksize=self.config.CHUNK_SIZE,
            )
            
            self.logger.info(
                f"Successfully inserted {len(data)} records to {table_name}"
            )
        
        except DatabaseError:
            raise
        except Exception as e:
            raise DatabaseError(
                f"Error inserting data into {table_name}: {str(e)}"
            )
    
    def close(self) -> None:
        """
        Close database connection and dispose engine.
        """
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database connection closed")
    
    def __enter__(self):
        """
        Context manager entry.
        """
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit with cleanup.
        """
        self.close()
