"""
Configuration Management for Account Log Cleaner

Handles all configuration settings from environment variables and provides
type-safe configuration access throughout the application.
"""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

from app.exceptions import ConfigurationError

# Load environment variables from .env file
load_dotenv()


class Config:
    """
    Configuration class for the Account Log Cleaner application.
    
    All configuration is loaded from environment variables with sensible defaults.
    This ensures:
    - No hardcoded credentials
    - Environment-specific configuration
    - Security best practices
    - Easy deployment across different environments
    """
    
    # File paths
    LOGS_INPUT_PATH: Path = Path(os.getenv("LOGS_INPUT_PATH", "./data/input"))
    LOGS_OUTPUT_PATH: Path = Path(os.getenv("LOGS_OUTPUT_PATH", "./data/processed"))
    PICKLES_PATH: Path = Path(os.getenv("PICKLES_PATH", "./data/pickles"))
    
    # Database Configuration
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "1433"))
    DB_USER: str = os.getenv("DB_USER", "sa")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")
    DB_NAME: str = os.getenv("DB_NAME", "AccountLogs")
    DB_SCHEMA: str = os.getenv("DB_SCHEMA", "dbo")
    DB_DRIVER: str = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    
    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: Path = Path(os.getenv("LOG_FILE", "./logs/account_cleaner.log"))
    LOG_FORMAT: str = os.getenv(
        "LOG_FORMAT",
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Processing Configuration
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "1000"))
    CHUNK_SIZE: int = int(os.getenv("CHUNK_SIZE", "5000"))
    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "4"))
    
    # Feature Flags
    ENABLE_PICKLE: bool = os.getenv("ENABLE_PICKLE", "True").lower() == "true"
    ENABLE_DB_INSERT: bool = os.getenv("ENABLE_DB_INSERT", "True").lower() == "true"
    DRY_RUN: bool = os.getenv("DRY_RUN", "False").lower() == "true"
    
    def __init__(self) -> None:
        """
        Initialize configuration and validate required settings.
        
        Raises:
            ConfigurationError: If required configuration is missing or invalid.
        """
        self._validate()
        self._create_directories()
    
    def _validate(self) -> None:
        """
        Validate configuration settings.
        
        Raises:
            ConfigurationError: If configuration validation fails.
        """
        errors = []
        
        # Validate paths
        if not self.LOGS_INPUT_PATH:
            errors.append("LOGS_INPUT_PATH must be configured")
        
        if not self.DB_HOST:
            errors.append("DB_HOST must be configured")
        
        if not self.DB_NAME:
            errors.append("DB_NAME must be configured")
        
        if self.BATCH_SIZE <= 0:
            errors.append("BATCH_SIZE must be greater than 0")
        
        if self.MAX_WORKERS <= 0:
            errors.append("MAX_WORKERS must be greater than 0")
        
        if errors:
            raise ConfigurationError("Configuration validation failed:\n" + "\n".join(errors))
    
    def _create_directories(self) -> None:
        """
        Create required directories if they don't exist.
        
        Creates:
            - LOGS_OUTPUT_PATH
            - PICKLES_PATH
            - LOG_FILE parent directory
        """
        for path in [self.LOGS_OUTPUT_PATH, self.PICKLES_PATH, self.LOG_FILE.parent]:
            path.mkdir(parents=True, exist_ok=True)
    
    def get_database_connection_string(self) -> str:
        """
        Generate database connection string from configuration.
        
        Returns:
            str: SQLAlchemy connection string for SQL Server.
            
        Note:
            Uses ODBC driver for SQL Server connection.
            Connection string format: mssql+pyodbc://user:password@host/db?driver=...
        """
        # Use URL-safe encoding for special characters
        from urllib.parse import quote_plus
        
        password = quote_plus(self.DB_PASSWORD) if self.DB_PASSWORD else ""
        
        if password:
            connection_string = (
                f"mssql+pyodbc://{self.DB_USER}:{password}@{self.DB_HOST}:"
                f"{self.DB_PORT}/{self.DB_NAME}?driver={self.DB_DRIVER}"
            )
        else:
            connection_string = (
                f"mssql+pyodbc://{self.DB_USER}@{self.DB_HOST}:"
                f"{self.DB_PORT}/{self.DB_NAME}?driver={self.DB_DRIVER}"
            )
        
        return connection_string
    
    def __str__(self) -> str:
        """
        String representation of configuration (without sensitive data).
        
        Returns:
            str: Configuration summary.
        """
        return (
            f"Config(host={self.DB_HOST}, db={self.DB_NAME}, "
            f"log_level={self.LOG_LEVEL}, batch_size={self.BATCH_SIZE})"
        )
    
    def __repr__(self) -> str:
        """
        Detailed representation of configuration.
        
        Returns:
            str: Detailed configuration information.
        """
        return self.__str__()
