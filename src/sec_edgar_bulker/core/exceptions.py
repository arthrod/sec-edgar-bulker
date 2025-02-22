"""Custom exceptions for SEC EDGAR Bulker."""

class TimeoutException(Exception):
    """Raised when a request times out."""
    pass

class NoProxiesAvailableError(Exception):
    """Raised when no proxies are available."""
    pass

class InvalidProxyFormatError(Exception):
    """Raised when a proxy is not in the correct format."""
    pass

class ProxyFileError(Exception):
    """Raised when there is an error reading the proxy file."""
    pass

class CacheError(Exception):
    """Raised when there is an error with the cache."""
    pass

class ConfigError(Exception):
    """Raised when there is an error with the configuration."""
    pass

class ParsingError(Exception):
    """Raised when there is an error parsing a file."""
    pass

class DownloadModeError(Exception):
    """Raised when there is an error with the download mode."""
    def __init__(self, message, details=None):
        super().__init__(message)
        self.details = details

class InvalidFormTypeError(Exception):
    """Raised when an invalid form type is encountered."""
    pass

class MissingMetadataError(Exception):
    """Raised when required metadata is missing."""
    pass

class FileIntegrityError(Exception):
    """Raised when a file fails integrity checks."""
    pass 