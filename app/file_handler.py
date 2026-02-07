"""
File Handling Operations for Account Log Cleaner

Handles:
- File discovery and validation
- File type detection
- File loading with error recovery
- File archiving and movement
"""

import logging
import re
import codecs
import shutil
from pathlib import Path
from typing import List, Tuple
from datetime import datetime

import pandas as pd

from app.config import Config
from app.exceptions import FileProcessingError


class FileHandler:
    """
    Handles all file-related operations.
    """
    
    # File type patterns
    FILE_PATTERN = r"(ValidationError|ValidationOk)\d*\.txt"
    
    # Expected file types
    VALIDATION_OK = "ValidationOk"
    VALIDATION_ERROR = "ValidationError"
    VALID_TYPES = {VALIDATION_OK, VALIDATION_ERROR}
    
    def __init__(self, config: Config, logger: logging.Logger) -> None:
        """
        Initialize file handler.
        
        Args:
            config: Configuration instance.
            logger: Logger instance.
        """
        self.config = config
        self.logger = logger
    
    def extract_wanted_files(self) -> List[Path]:
        """
        Extract all valid log files from input directory.
        
        Returns:
            List[Path]: List of valid file paths.
            
        Raises:
            FileProcessingError: If input directory doesn't exist.
        """
        try:
            if not self.config.LOGS_INPUT_PATH.exists():
                raise FileProcessingError(
                    f"Input path does not exist: {self.config.LOGS_INPUT_PATH}"
                )
            
            files = []
            for file in self.config.LOGS_INPUT_PATH.iterdir():
                if file.is_file() and re.match(self.FILE_PATTERN, file.name):
                    files.append(file)
            
            return sorted(files)
        
        except FileProcessingError:
            raise
        except Exception as e:
            raise FileProcessingError(f"Error extracting files: {str(e)}")
    
    def detect_file_type(self, file_path: Path) -> str:
        """
        Detect file type based on filename.
        
        Args:
            file_path: Path to the file.
            
        Returns:
            str: File type (ValidationOk or ValidationError).
            
        Raises:
            FileProcessingError: If file type cannot be detected.
        """
        try:
            matches = re.findall(self.FILE_PATTERN, file_path.name)
            if not matches:
                raise FileProcessingError(
                    f"Cannot detect file type for: {file_path.name}"
                )
            
            file_type = matches[0]
            if file_type not in self.VALID_TYPES:
                raise FileProcessingError(
                    f"Unknown file type: {file_type}"
                )
            
            return file_type
        
        except FileProcessingError:
            raise
        except Exception as e:
            raise FileProcessingError(f"Error detecting file type: {str(e)}")
    
    def load_text_files(self, file_path: Path, file_type: str) -> pd.DataFrame:
        """
        Load text file into DataFrame with error recovery.
        
        Args:
            file_path: Path to the file.
            file_type: Type of the file.
            
        Returns:
            pd.DataFrame: Loaded and processed data.
            
        Raises:
            FileProcessingError: If file cannot be loaded.
        """
        try:
            if not file_path.exists():
                raise FileProcessingError(f"File not found: {file_path}")
            
            # Load main data
            df = pd.read_csv(
                file_path,
                delimiter="\t",
                header=None,
                on_bad_lines="skip",
                engine="python",
                encoding="utf-8"
            ).dropna(axis=1, how="all")
            
            # Rename columns
            df = self._rename_columns(df, file_type)
            
            # For ValidationError files, try to recover additional lines
            if file_type == self.VALIDATION_ERROR:
                recovered_df = self._read_erroneous_lines(file_path)
                if not recovered_df.empty:
                    df = pd.concat([df, recovered_df], ignore_index=True)
                    self.logger.debug(f"Recovered {len(recovered_df)} additional lines")
            
            return df
        
        except FileProcessingError:
            raise
        except Exception as e:
            raise FileProcessingError(
                f"Error loading file {file_path.name}: {str(e)}"
            )
    
    def _rename_columns(self, df: pd.DataFrame, file_type: str) -> pd.DataFrame:
        """
        Rename columns based on file type.
        
        Args:
            df: DataFrame to rename.
            file_type: Type of the file.
            
        Returns:
            pd.DataFrame: DataFrame with renamed columns.
        """
        if file_type == self.VALIDATION_OK:
            expected_cols = [
                "BankName", "AccountNumber", "ShebaNumber",
                "NationalCode", "TransactionTime", "Status"
            ]
        elif file_type == self.VALIDATION_ERROR:
            expected_cols = [
                "BankName", "AccountNumber", "ShebaNumber",
                "NationalCode", "TransactionTime", "ErrorCode", "Status"
            ]
        
        # Rename only the columns we have
        cols_to_rename = {i: col for i, col in enumerate(expected_cols) if i < len(df.columns)}
        df = df.rename(columns=cols_to_rename)
        
        return df
    
    def _read_erroneous_lines(self, file_path: Path) -> pd.DataFrame:
        """
        Recover lines with JSON data from erroneous files.
        
        Args:
            file_path: Path to the file.
            
        Returns:
            pd.DataFrame: Recovered data.
        """
        try:
            with codecs.open(file_path, "r", "UTF-8") as file:
                lines = file.readlines()
            
            # Extract lines containing JSON
            json_lines = [
                re.findall(r".*{.*", line)[0].replace("\t\r", "")
                for line in lines if re.findall(r"{", line)
            ]
            
            if not json_lines:
                return pd.DataFrame()
            
            # Split and process lines
            lines_split = [line.split("\t") for line in json_lines]
            lines_corrected = []
            
            for line in lines_split:
                if len(line) == 10:
                    # Merge JSON parts
                    lines_corrected.append(line[0:6] + [line[6] + line[7] + line[8] + line[9]])
                elif len(line) == 7:
                    lines_corrected.append(line)
            
            df = pd.DataFrame(lines_corrected)
            df = self._rename_columns(df, self.VALIDATION_ERROR)
            
            return df
        
        except Exception as e:
            self.logger.warning(f"Error reading erroneous lines: {str(e)}")
            return pd.DataFrame()
    
    def move_logs(self, files: List[Path]) -> None:
        """
        Move processed files to archive folder.
        
        Args:
            files: List of file paths to archive.
            
        Raises:
            FileProcessingError: If archiving fails.
        """
        try:
            # Create archive folder with timestamp
            folder_name = datetime.now().strftime("%Y-%m-%d_%H%M%S")
            archive_path = self.config.LOGS_INPUT_PATH / folder_name
            archive_path.mkdir(parents=True, exist_ok=True)
            
            # Move files
            for file in files:
                if file.exists():
                    shutil.move(str(file), str(archive_path / file.name))
            
            self.logger.info(f"Archived {len(files)} files to {archive_path}")
        
        except Exception as e:
            raise FileProcessingError(f"Error archiving files: {str(e)}")
