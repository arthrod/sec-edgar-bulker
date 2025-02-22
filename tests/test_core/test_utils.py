"""Tests for the utils module."""

import pytest
import logging
from pathlib import Path
from datetime import datetime
from sec_edgar_bulker.core.utils import setup_logging, validate_config, generate_batch_id
from sec_edgar_bulker.core.exceptions import ConfigError
from sec_edgar_bulker.core.models import Config, LoggingSettings

def test_setup_logging_with_file(tmp_path):
    """Test setting up logging with file output."""
    log_file = tmp_path / "test.log"
    config = LoggingSettings(
        level="DEBUG",
        format="%(asctime)s - %(levelname)s - %(message)s",
        file_enabled=True,
        batch_specific=True
    )
    
    logger = setup_logging(config, log_file)
    
    assert isinstance(logger, logging.Logger)
    assert logger.level == logging.DEBUG
    assert len(logger.handlers) == 2  # File and stream handlers
    assert any(isinstance(h, logging.FileHandler) for h in logger.handlers)
    assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)
    
    # Test logging
    test_message = "Test log message"
    logger.info(test_message)
    
    # Verify message was written to file
    assert log_file.exists()
    content = log_file.read_text()
    assert test_message in content

def test_setup_logging_without_file():
    """Test setting up logging without file output."""
    config = LoggingSettings(
        level="INFO",
        format="%(levelname)s - %(message)s",
        file_enabled=False,
        batch_specific=False
    )
    
    logger = setup_logging(config)
    
    assert isinstance(logger, logging.Logger)
    assert logger.level == logging.INFO
    assert len(logger.handlers) == 1  # Only stream handler
    assert all(isinstance(h, logging.StreamHandler) for h in logger.handlers)

@pytest.mark.parametrize("level", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
def test_setup_logging_levels(level):
    """Test setting up logging with different levels."""
    config = LoggingSettings(
        level=level,
        format="%(levelname)s - %(message)s",
        file_enabled=False,
        batch_specific=False
    )
    
    logger = setup_logging(config)
    assert logger.level == getattr(logging, level)

def test_setup_logging_custom_format():
    """Test setting up logging with custom format."""
    custom_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    config = LoggingSettings(
        level="DEBUG",
        format=custom_format,
        file_enabled=False,
        batch_specific=False
    )
    
    logger = setup_logging(config)
    handler = logger.handlers[0]
    assert handler.formatter._fmt == custom_format

def test_validate_config_valid(sample_config):
    """Test validating a valid configuration."""
    validate_config(sample_config)  # Should not raise

def test_validate_config_no_years(sample_config):
    """Test validating config with no years."""
    sample_config.years = []
    with pytest.raises(ConfigError, match="`years` must be specified"):
        validate_config(sample_config)

def test_validate_config_no_quarters(sample_config):
    """Test validating config with no quarters."""
    sample_config.quarters = []
    with pytest.raises(ConfigError, match="`quarters` must be specified"):
        validate_config(sample_config)

def test_validate_config_company_filter_no_identifiers(sample_config):
    """Test validating config with company filter but no identifiers."""
    sample_config.filtering.company_filter_enabled = True
    sample_config.filtering.ciks = None
    sample_config.filtering.company_names = None
    with pytest.raises(ConfigError, match="At least one of `ciks` or `company_names` must be specified"):
        validate_config(sample_config)

def test_validate_config_date_filter_no_range(sample_config):
    """Test validating config with date filter but no range."""
    sample_config.filtering.date_range_filter_enabled = True
    sample_config.filtering.date_range_start = None
    sample_config.filtering.date_range_end = None
    with pytest.raises(ConfigError, match="Both `date_range_start` and `date_range_end` must be specified"):
        validate_config(sample_config)

def test_generate_batch_id():
    """Test generating batch ID."""
    format_str = "%Y%m%d_%H%M%S"
    batch_id = generate_batch_id(format_str)
    
    # Verify format
    assert len(batch_id) == len(datetime.now().strftime(format_str))
    
    # Should be parseable as datetime
    try:
        datetime.strptime(batch_id, format_str)
    except ValueError:
        pytest.fail("Generated batch ID does not match expected format")

@pytest.mark.parametrize("format_str", [
    "%Y%m%d",
    "%Y-%m-%d_%H-%M-%S",
    "%Y%m%d_%H%M%S_%f"
])
def test_generate_batch_id_formats(format_str):
    """Test generating batch ID with different formats."""
    batch_id = generate_batch_id(format_str)
    try:
        datetime.strptime(batch_id, format_str)
    except ValueError:
        pytest.fail(f"Generated batch ID does not match format {format_str}")

def test_setup_logging_file_creation(tmp_path):
    """Test that log file is created with correct permissions."""
    log_file = tmp_path / "test.log"
    config = LoggingSettings(
        level="DEBUG",
        format="%(message)s",
        file_enabled=True,
        batch_specific=True
    )
    
    logger = setup_logging(config, log_file)
    logger.info("Test message")
    
    assert log_file.exists()
    assert log_file.stat().st_mode & 0o777 == 0o644  # Check permissions

def test_setup_logging_directory_creation(tmp_path):
    """Test that log directory is created if it doesn't exist."""
    log_dir = tmp_path / "logs"
    log_file = log_dir / "test.log"
    config = LoggingSettings(
        level="DEBUG",
        format="%(message)s",
        file_enabled=True,
        batch_specific=True
    )
    
    logger = setup_logging(config, log_file)
    logger.info("Test message")
    
    assert log_dir.exists()
    assert log_dir.is_dir()

def test_setup_logging_rotation(tmp_path):
    """Test log file rotation settings."""
    log_file = tmp_path / "test.log"
    config = LoggingSettings(
        level="DEBUG",
        format="%(message)s",
        file_enabled=True,
        batch_specific=True
    )
    
    logger = setup_logging(config, log_file)
    handler = next(h for h in logger.handlers if isinstance(h, logging.FileHandler))
    
    # Write a large message
    large_message = "x" * 1024 * 1024  # 1MB
    logger.info(large_message)
    
    # Check that the log was written
    assert log_file.exists()
    assert log_file.stat().st_size > 0

@pytest.mark.parametrize("invalid_format", [
    "",  # Empty string
    "invalid",  # Invalid format string
    "%Y%m%d%invalid"  # Invalid format specifier
])
def test_generate_batch_id_invalid_format(invalid_format):
    """Test generating batch ID with invalid format strings."""
    with pytest.raises(ValueError):
        generate_batch_id(invalid_format)

def test_validate_config_filtering_dates(sample_config):
    """Test validating config with filtering dates."""
    # Valid dates
    sample_config.filtering.date_range_filter_enabled = True
    sample_config.filtering.date_range_start = "2023-01-01"
    sample_config.filtering.date_range_end = "2023-12-31"
    validate_config(sample_config)  # Should not raise
    
    # Invalid start date
    sample_config.filtering.date_range_start = "invalid"
    with pytest.raises(ConfigError):
        validate_config(sample_config) 