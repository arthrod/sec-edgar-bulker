"""Core functionality for SEC EDGAR Bulker."""

from .downloader import EdgarDownloader
from .idx_downloader import IdxDownloader
from .header_generator import HeaderGenerator
from .proxy_manager import ProxyManager
from .models import (
    Config, 
    HeaderGeneratorSettings,
    StaticHeaders,
    ProxySettings,
    DownloadSettings,
    LoggingSettings,
    DirectorySettings,
    FilteringSettings
)
from .exceptions import (
    ConfigError,
    DownloadModeError,
    InvalidProxyFormatError,
    ProxyFileError
)
from .utils import setup_logging, validate_config, generate_batch_id

__all__ = [
    'EdgarDownloader',
    'IdxDownloader',
    'HeaderGenerator',
    'ProxyManager',
    'Config',
    'HeaderGeneratorSettings',
    'StaticHeaders',
    'ProxySettings',
    'DownloadSettings',
    'LoggingSettings',
    'DirectorySettings',
    'FilteringSettings',
    'ConfigError',
    'DownloadModeError',
    'InvalidProxyFormatError',
    'ProxyFileError',
    'setup_logging',
    'validate_config',
    'generate_batch_id'
] 