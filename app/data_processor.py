"""
Data Processing and Transformation Logic

Handles:
- Data cleaning and normalization
- Data enrichment
- Data validation
- Pickle serialization
"""

import logging
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

import pandas as pd
import sqlalchemy as sa

from app.config import Config
from app.exceptions import DataValidationError, DataEnrichmentError


class DataProcessor:
    """
    Handles all data processing and transformation operations.
    """
    
    # Column mappings
    VALIDATION_OK_COLS = [
        "BankName", "AccountNumber", "ShebaNumber",
        "NationalCode", "TransactionTime", "Status"
    ]
    
    VALIDATION_ERROR_COLS = [
        "BankName", "AccountNumber", "ShebaNumber",
        "NationalCode", "TransactionTime", "ErrorCode", "Status"
    ]
    
    def __init__(self, config: Config, logger: logging.Logger) -> None:
        """
        Initialize data processor.
        
        Args:
            config: Configuration instance.
            logger: Logger instance.
        """
        self.config = config
        self.logger = logger
    
    def make_data_clean(self, data: pd.DataFrame, file_type: str) -> pd.DataFrame:
        """
        Clean and normalize data.
        
        Operations:
        - Remove prefixes (e.g., "FieldName:" -> "")
        - Strip whitespace
        - Handle null values
        
        Args:
            data: DataFrame to clean.
            file_type: Type of the file.
            
        Returns:
            pd.DataFrame: Cleaned data.
        """
        try:
            # Remove field name prefixes (FieldName: value -> value)
            for col in data.columns:
                if isinstance(col, str) and col in self.VALIDATION_OK_COLS + self.VALIDATION_ERROR_COLS:
                    data[col] = (
                        data[col]
                        .astype(str)
                        .str.replace(r"^.*?:", "", regex=True)
                        .str.strip()
                    )
            
            # Drop completely empty rows
            data = data.dropna(how="all")
            
            # Fill NaN values with empty string
            data = data.fillna("")
            
            return data
        
        except Exception as e:
            raise DataValidationError(f"Error cleaning data: {str(e)}")
    
    def enrich_data(
        self,
        data: pd.DataFrame,
        filename: str,
        file_type: str
    ) -> pd.DataFrame:
        """
        Enrich data with additional columns.
        
        Adds:
        - Date: extracted from TransactionTime
        - FileName: source filename
        - Type: file type
        - ProcessedAt: processing timestamp
        
        Args:
            data: DataFrame to enrich.
            filename: Source filename.
            file_type: Type of the file.
            
        Returns:
            pd.DataFrame: Enriched data with reordered columns.
            
        Raises:
            DataEnrichmentError: If enrichment fails.
        """
        try:
            # Extract date from transaction time
            if "TransactionTime" in data.columns:
                data["Date"] = data["TransactionTime"].str[:10]
            else:
                data["Date"] = ""
            
            # Add metadata columns
            data["FileName"] = filename
            data["Type"] = file_type
            data["ProcessedAt"] = datetime.now().isoformat()
            
            # Reorder columns
            if file_type == "ValidationOk":
                column_order = [
                    "BankName", "AccountNumber", "ShebaNumber", "NationalCode",
                    "Date", "TransactionTime", "Status",
                    "FileName", "Type", "ProcessedAt"
                ]
            else:  # ValidationError
                column_order = [
                    "BankName", "AccountNumber", "ShebaNumber", "NationalCode",
                    "Date", "TransactionTime", "ErrorCode", "Status",
                    "FileName", "Type", "ProcessedAt"
                ]
            
            # Keep only columns that exist
            column_order = [col for col in column_order if col in data.columns]
            data = data[column_order]
            
            return data
        
        except Exception as e:
            raise DataEnrichmentError(f"Error enriching data: {str(e)}")
    
    def create_pickle(self, data: pd.DataFrame, file_type: str) -> None:
        """
        Save data as pickle file for backup.
        
        Args:
            data: DataFrame to save.
            file_type: Type of the file.
            
        Raises:
            DataValidationError: If pickle creation fails.
        """
        try:
            pickle_file = self.config.PICKLES_PATH / f"{file_type}_{datetime.now().isoformat()}.pickle"
            data.to_pickle(pickle_file)
            self.logger.debug(f"Pickle saved to {pickle_file}")
        
        except Exception as e:
            raise DataValidationError(f"Error creating pickle: {str(e)}")
    
    @staticmethod
    def get_database_types(file_type: str) -> Dict[str, Any]:
        """
        Get SQLAlchemy data types for database columns.
        
        Args:
            file_type: Type of the file.
            
        Returns:
            Dict: Column name to SQLAlchemy type mapping.
        """
        common_types = {
            "BankName": sa.types.NVARCHAR(length=100),
            "AccountNumber": sa.types.VARCHAR(length=50),
            "ShebaNumber": sa.types.VARCHAR(length=50),
            "NationalCode": sa.types.VARCHAR(length=30),
            "Date": sa.types.VARCHAR(length=10),
            "TransactionTime": sa.types.VARCHAR(length=21),
            "Status": sa.types.NVARCHAR(length=1000),
            "FileName": sa.types.NVARCHAR(length=256),
            "Type": sa.types.VARCHAR(length=20),
            "ProcessedAt": sa.types.VARCHAR(length=30),
        }
        
        if file_type == "ValidationOk":
            return common_types
        else:  # ValidationError
            common_types["ErrorCode"] = sa.types.VARCHAR(length=10)
            return common_types
    
    @staticmethod
    def fix_column_size(data: pd.DataFrame, file_type: str) -> pd.DataFrame:
        """
        Truncate columns to database limits.
        
        Args:
            data: DataFrame to truncate.
            file_type: Type of the file.
            
        Returns:
            pd.DataFrame: Truncated data.
        """
        column_limits = {
            "BankName": 100,
            "AccountNumber": 50,
            "ShebaNumber": 50,
            "NationalCode": 30,
            "Date": 10,
            "TransactionTime": 21,
            "Status": 1000,
            "FileName": 256,
            "Type": 20,
            "ErrorCode": 10,
            "ProcessedAt": 30,
        }
        
        for col, limit in column_limits.items():
            if col in data.columns:
                data[col] = data[col].astype(str).str[:limit]
        
        return data
