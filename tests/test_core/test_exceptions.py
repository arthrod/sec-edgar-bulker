"""Tests for the exceptions module."""

import pytest
from sec_edgar_bulker.core.exceptions import (
    TimeoutException,
    NoProxiesAvailableError,
    InvalidProxyFormatError,
    ProxyFileError,
    CacheError,
    ConfigError,
    ParsingError,
    DownloadModeError,
    InvalidFormTypeError,
    MissingMetadataError,
    FileIntegrityError
)

def test_timeout_exception():
    """Test TimeoutException."""
    message = "Request timed out"
    exc = TimeoutException(message)
    assert str(exc) == message
    assert isinstance(exc, Exception)

def test_no_proxies_available_error():
    """Test NoProxiesAvailableError."""
    message = "No proxies available"
    exc = NoProxiesAvailableError(message)
    assert str(exc) == message
    assert isinstance(exc, Exception)

def test_invalid_proxy_format_error():
    """Test InvalidProxyFormatError."""
    message = "Invalid proxy format"
    exc = InvalidProxyFormatError(message)
    assert str(exc) == message
    assert isinstance(exc, Exception)

def test_proxy_file_error():
    """Test ProxyFileError."""
    message = "Error reading proxy file"
    exc = ProxyFileError(message)
    assert str(exc) == message
    assert isinstance(exc, Exception)

def test_cache_error():
    """Test CacheError."""
    message = "Cache error occurred"
    exc = CacheError(message)
    assert str(exc) == message
    assert isinstance(exc, Exception)

def test_config_error():
    """Test ConfigError."""
    message = "Invalid configuration"
    exc = ConfigError(message)
    assert str(exc) == message
    assert isinstance(exc, Exception)

def test_parsing_error():
    """Test ParsingError."""
    message = "Failed to parse file"
    exc = ParsingError(message)
    assert str(exc) == message
    assert isinstance(exc, Exception)

def test_download_mode_error():
    """Test DownloadModeError."""
    message = "Invalid download mode"
    exc = DownloadModeError(message)
    assert str(exc) == message
    assert isinstance(exc, Exception)

def test_invalid_form_type_error():
    """Test InvalidFormTypeError."""
    message = "Invalid form type"
    exc = InvalidFormTypeError(message)
    assert str(exc) == message
    assert isinstance(exc, Exception)

def test_missing_metadata_error():
    """Test MissingMetadataError."""
    message = "Missing required metadata"
    exc = MissingMetadataError(message)
    assert str(exc) == message
    assert isinstance(exc, Exception)

def test_file_integrity_error():
    """Test FileIntegrityError."""
    message = "File integrity check failed"
    exc = FileIntegrityError(message)
    assert str(exc) == message
    assert isinstance(exc, Exception)

def test_exception_inheritance():
    """Test that all exceptions inherit from Exception."""
    exceptions = [
        TimeoutException,
        NoProxiesAvailableError,
        InvalidProxyFormatError,
        ProxyFileError,
        CacheError,
        ConfigError,
        ParsingError,
        DownloadModeError,
        InvalidFormTypeError,
        MissingMetadataError,
        FileIntegrityError
    ]
    
    for exc_class in exceptions:
        assert issubclass(exc_class, Exception)

def test_exception_with_nested_exception():
    """Test exceptions with nested exceptions."""
    try:
        try:
            raise ValueError("Inner error")
        except ValueError as e:
            raise ParsingError("Parsing failed") from e
    except ParsingError as e:
        assert isinstance(e.__cause__, ValueError)
        assert str(e.__cause__) == "Inner error"

def test_exception_without_message():
    """Test exceptions without a message."""
    exc = TimeoutException()
    assert str(exc) == ""

@pytest.mark.parametrize("exception_class", [
    TimeoutException,
    NoProxiesAvailableError,
    InvalidProxyFormatError,
    ProxyFileError,
    CacheError,
    ConfigError,
    ParsingError,
    DownloadModeError,
    InvalidFormTypeError,
    MissingMetadataError,
    FileIntegrityError
])
def test_exception_creation(exception_class):
    """Test creation of all exception types."""
    message = f"Test {exception_class.__name__}"
    exc = exception_class(message)
    assert isinstance(exc, exception_class)
    assert str(exc) == message

def test_exception_with_details():
    """Test exceptions with additional details."""
    details = {"url": "http://example.com", "status": 404}
    exc = DownloadModeError("Download failed", details=details)
    assert hasattr(exc, "details")
    assert exc.details == details

def test_chained_exceptions():
    """Test chaining multiple exceptions."""
    try:
        try:
            try:
                raise ValueError("Root cause")
            except ValueError as e:
                raise ConfigError("Config validation failed") from e
        except ConfigError as e:
            raise ParsingError("Parsing failed") from e
    except ParsingError as e:
        assert isinstance(e.__cause__, ConfigError)
        assert isinstance(e.__cause__.__cause__, ValueError)
        assert str(e.__cause__.__cause__) == "Root cause" 