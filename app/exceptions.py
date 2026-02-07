"""
Custom Exceptions for Account Log Cleaner

Defines domain-specific exceptions for the Account Log Cleaner application.
"""


class AccountLogCleanerError(Exception):
    """
    Base exception class for Account Log Cleaner application.
    
    All domain-specific exceptions should inherit from this class.
    """
    pass


class ConfigurationError(AccountLogCleanerError):
    """
    Raised when configuration is invalid or incomplete.
    
    Examples:
        - Missing required environment variables
        - Invalid path configuration
        - Invalid database connection string
    """
    pass


class FileProcessingError(AccountLogCleanerError):
    """
    Raised when file processing fails.
    
    Examples:
        - File not found
        - Unable to read file
        - Invalid file format
    """
    pass


class DataValidationError(AccountLogCleanerError):
    """
    Raised when data validation fails.
    
    Examples:
        - Empty dataframe
        - Missing required columns
        - Invalid data types
    """
    pass


class DatabaseError(AccountLogCleanerError):
    """
    Raised when database operations fail.
    
    Examples:
        - Connection failure
        - Insert failure
        - Schema issues
    """
    pass


class DataEnrichmentError(AccountLogCleanerError):
    """
    Raised when data enrichment operations fail.
    
    Examples:
        - Cannot parse date
        - Cannot extract required fields
    """
    pass
