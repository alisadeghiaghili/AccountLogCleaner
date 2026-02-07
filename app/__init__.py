"""
Account Log Cleaner Package

Production-ready application for processing and cleaning account validation logs.

Version: 2.0.0
"""

__version__ = "2.0.0"
__author__ = "Ali Sadeghi Aghili"
__license__ = "MIT"

from app.exceptions import AccountLogCleanerError
from app.config import Config
from app.processor import LogProcessor

__all__ = [
    "AccountLogCleanerError",
    "Config",
    "LogProcessor",
    "__version__",
]
