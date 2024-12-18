"""Utility functions for SEC EDGAR Bulker."""

import logging
import logging.handlers
from datetime import datetime
from pathlib import Path
from typing import Optional, Union
import os
import stat

from .models import Config, LoggingSettings
from .exceptions import ConfigError

def setup_logging(config: Union[Config, LoggingSettings], log_file: Optional[str] = None) -> logging.Logger:
    """Set up logging configuration.
    
    Args:
        config: Either a Config object or LoggingSettings object
        log_file: Optional override for log file path
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger("sec_edgar_bulker")
    logger.handlers.clear()  # Remove any existing handlers
    
    try:
        # Get logging settings
        settings = config.logging if isinstance(config, Config) else config
        
        # Set level
        level = settings.level.upper()
        if not hasattr(logging, level):
            raise ValueError(f"Invalid logging level: {level}")
        logger.setLevel(getattr(logging, level))
        
        # Create formatter
        formatter = logging.Formatter(settings.format)
        
        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Add file handler if enabled
        if settings.file_enabled:
            file_path = log_file or settings.file
            if file_path:
                # Create log directory with proper permissions
                log_dir = os.path.dirname(file_path)
                if log_dir:
                    os.makedirs(log_dir, exist_ok=True)
                    # Set directory permissions to 755 (rwxr-xr-x)
                    os.chmod(log_dir, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
                    
                # Create or check log file
                Path(file_path).touch(exist_ok=True)
                # Set file permissions to 644 (rw-r--r--)
                os.chmod(file_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
                    
                file_handler = logging.handlers.RotatingFileHandler(
                    file_path,
                    maxBytes=settings.max_size,
                    backupCount=settings.backup_count,
                    encoding='utf-8'
                )
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)
                
                # Set permissions for rotated log files
                base_dir = os.path.dirname(file_path)
                base_name = os.path.basename(file_path)
                for rotated_file in Path(base_dir).glob(f"{base_name}.*"):
                    os.chmod(rotated_file, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
        
        return logger
        
    except Exception as e:
        # Ensure we have at least console logging in case of errors
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        logger.addHandler(console_handler)
        logger.setLevel(logging.ERROR)
        logger.error(f"Failed to setup logging: {e}")
        raise

def validate_config(config: Config) -> None:
    """Validate configuration settings"""
    errors = []
    
    # Years validation
    if not config.years:
        errors.append("`years` must be specified")
    
    # Quarters validation
    if not config.quarters:
        errors.append("`quarters` must be specified")
    
    # Company filter validation
    if config.filtering.company_filter_enabled:
        if not config.filtering.ciks and not config.filtering.company_names:
            errors.append("At least one of `ciks` or `company_names` must be specified when company filter is enabled")
    
    # Date range validation
    if config.filtering.date_range_filter_enabled:
        if not config.filtering.date_range_start or not config.filtering.date_range_end:
            errors.append("Both `date_range_start` and `date_range_end` must be specified when date range filter is enabled")
        else:
            try:
                start = datetime.strptime(config.filtering.date_range_start, "%Y-%m-%d")
                end = datetime.strptime(config.filtering.date_range_end, "%Y-%m-%d")
                if start > end:
                    errors.append("date_range_start must be before date_range_end")
            except ValueError:
                errors.append("Invalid date format. Use YYYY-MM-DD")
                
    if errors:
        raise ConfigError("\n".join(errors))

def generate_batch_id(format_str: str) -> str:
    """Generate a batch ID using the specified format.
    
    Args:
        format_str: strftime format string for generating the batch ID
        
    Returns:
        Generated batch ID string
        
    Raises:
        ValueError: If the format string is invalid
    """
    if not format_str or not isinstance(format_str, str):
        raise ValueError("Format string must be a non-empty string")
        
    try:
        # Validate format string with test datetime
        test_datetime = datetime(2024, 1, 1, 12, 0, 0, 123456)
        try:
            test_str = test_datetime.strftime(format_str)
            # Verify we can parse it back
            parsed = datetime.strptime(test_str, format_str)
            if parsed.year != test_datetime.year:
                raise ValueError("Format string must include year")
        except ValueError as e:
            raise ValueError(f"Invalid datetime format string: {str(e)}")
            
        # Generate actual batch ID
        now = datetime.now()
        batch_id = now.strftime(format_str)
        
        # Validate the generated ID
        if not batch_id or len(batch_id) < 8:  # Minimum length for a meaningful ID
            raise ValueError("Generated batch ID is too short")
            
        return batch_id
        
    except Exception as e:
        if isinstance(e, ValueError):
            raise
        raise ValueError(f"Error generating batch ID: {str(e)}")

def monitor_resources() -> None:
    """Monitor system resource usage.
    
    This function is a placeholder for future implementation.
    It will monitor CPU, memory, disk usage, and network activity.
    """
    pass  # pragma: no cover