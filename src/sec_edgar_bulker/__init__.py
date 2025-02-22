"""SEC EDGAR Bulker package."""

from .core import (
    EdgarDownloader,
    IdxDownloader,
    HeaderGenerator,
    ProxyManager,
    Config,
    setup_logging,
    validate_config
)

__version__ = "0.1.0"

__all__ = [
    'EdgarDownloader',
    'IdxDownloader',
    'HeaderGenerator',
    'ProxyManager',
    'Config',
    'setup_logging',
    'validate_config'
] 