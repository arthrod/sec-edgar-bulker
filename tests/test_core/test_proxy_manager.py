"""Tests for the proxy manager module."""

import pytest
import aiohttp
from pathlib import Path
from sec_edgar_bulker.core.proxy_manager import ProxyManager
from sec_edgar_bulker.core.exceptions import (
    NoProxiesAvailableError,
    InvalidProxyFormatError,
    ProxyFileError
)

@pytest.fixture
def proxy_manager(sample_config):
    """Create a proxy manager instance."""
    return ProxyManager(sample_config)

def test_proxy_manager_init(proxy_manager, sample_config):
    """Test proxy manager initialization."""
    assert proxy_manager.config == sample_config
    assert proxy_manager.settings == sample_config.proxy
    assert proxy_manager.mode == sample_config.proxy.mode
    assert proxy_manager.proxies == []
    assert proxy_manager.proxy_usage_count == {}

def test_proxy_manager_disabled(sample_config):
    """Test proxy manager when disabled."""
    sample_config.proxy.enabled = False
    manager = ProxyManager(sample_config)
    with pytest.raises(NoProxiesAvailableError):
        manager.get_random_proxy()

def test_load_proxies_valid(sample_config, sample_proxy_file):
    """Test loading valid proxies."""
    sample_config.proxy.enabled = True
    sample_config.proxy.file = sample_proxy_file
    manager = ProxyManager(sample_config)
    manager.load_proxies()
    assert len(manager.proxies) == 3
    assert all(isinstance(proxy, tuple) for proxy in manager.proxies)
    assert all(len(proxy) == 2 for proxy in manager.proxies)

def test_load_proxies_invalid_format(sample_config, tmp_path):
    """Test loading proxies with invalid format."""
    proxy_file = tmp_path / "invalid_proxies.txt"
    proxy_file.write_text("invalid:format")
    sample_config.proxy.enabled = True
    sample_config.proxy.file = str(proxy_file)
    manager = ProxyManager(sample_config)
    with pytest.raises(InvalidProxyFormatError):
        manager.load_proxies()

def test_load_proxies_missing_file(sample_config):
    """Test loading proxies with missing file."""
    sample_config.proxy.enabled = True
    sample_config.proxy.file = "nonexistent.txt"
    manager = ProxyManager(sample_config)
    with pytest.raises(ProxyFileError):
        manager.load_proxies()

def test_get_random_proxy_random_mode(sample_config, sample_proxy_file):
    """Test getting random proxy in random mode."""
    sample_config.proxy.enabled = True
    sample_config.proxy.file = sample_proxy_file
    sample_config.proxy.mode = "random"
    manager = ProxyManager(sample_config)
    manager.load_proxies()
    proxy, auth = manager.get_random_proxy()
    assert isinstance(proxy, str)
    assert isinstance(auth, aiohttp.BasicAuth)
    assert proxy.startswith("http://")

def test_get_random_proxy_lru_mode(sample_config, sample_proxy_file):
    """Test getting random proxy in LRU mode."""
    sample_config.proxy.enabled = True
    sample_config.proxy.file = sample_proxy_file
    sample_config.proxy.mode = "lru"
    manager = ProxyManager(sample_config)
    manager.load_proxies()
    first_proxy = manager.get_random_proxy()
    second_proxy = manager.get_random_proxy()
    assert first_proxy != second_proxy

def test_proxy_usage_limit(sample_config, sample_proxy_file):
    """Test proxy usage limit enforcement."""
    sample_config.proxy.enabled = True
    sample_config.proxy.file = sample_proxy_file
    sample_config.proxy.usage_limit = 2
    manager = ProxyManager(sample_config)
    manager.load_proxies()
    
    # Use the first proxy twice
    proxy1 = manager.get_random_proxy()[0]
    proxy2 = manager.get_random_proxy()[0]
    assert proxy1 == proxy2
    assert manager.proxy_usage_count[proxy1] == 2
    
    # Third request should get a different proxy
    proxy3 = manager.get_random_proxy()[0]
    assert proxy3 != proxy1

@pytest.mark.security
def test_proxy_auth_security(sample_config, sample_proxy_file):
    """Test that proxy authentication is handled securely."""
    sample_config.proxy.enabled = True
    sample_config.proxy.file = sample_proxy_file
    manager = ProxyManager(sample_config)
    manager.load_proxies()
    
    # Check that credentials are not exposed in string representation
    manager_str = str(manager)
    assert "user1" not in manager_str
    assert "pass1" not in manager_str
    
    # Check that auth object is properly created
    _, auth = manager.get_random_proxy()
    assert isinstance(auth, aiohttp.BasicAuth)
    assert str(auth.login) not in str(manager.proxies)
    assert str(auth.password) not in str(manager.proxies)

def test_proxy_rotation(sample_config, sample_proxy_file):
    """Test that proxies are properly rotated."""
    sample_config.proxy.enabled = True
    sample_config.proxy.file = sample_proxy_file
    manager = ProxyManager(sample_config)
    manager.load_proxies()
    
    used_proxies = set()
    for _ in range(len(manager.proxies) * 2):
        proxy, _ = manager.get_random_proxy()
        used_proxies.add(proxy)
    
    assert len(used_proxies) == len(manager.proxies)

def test_proxy_reset_after_limit(sample_config, sample_proxy_file):
    """Test that usage counts are reset when all proxies reach limit."""
    sample_config.proxy.enabled = True
    sample_config.proxy.file = sample_proxy_file
    sample_config.proxy.usage_limit = 1
    manager = ProxyManager(sample_config)
    manager.load_proxies()
    
    # Use each proxy once to reach limit
    proxies = set()
    for _ in range(len(manager.proxies)):
        proxy, _ = manager.get_random_proxy()
        proxies.add(proxy)
    
    # All proxies should have been used
    assert len(proxies) == len(manager.proxies)
    assert all(count == 1 for count in manager.proxy_usage_count.values())
    
    # Next request should reset counts
    manager.get_random_proxy()
    assert all(count <= 1 for count in manager.proxy_usage_count.values())

@pytest.mark.parametrize("proxy_line", [
    "192.168.1.1:8080",  # No auth
    "192.168.1.1:8080:user:pass",  # With auth
])
def test_proxy_format_variations(sample_config, tmp_path, proxy_line):
    """Test different proxy format variations."""
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text(proxy_line)
    sample_config.proxy.enabled = True
    sample_config.proxy.file = str(proxy_file)
    manager = ProxyManager(sample_config)
    manager.load_proxies()
    proxy, auth = manager.get_random_proxy()
    assert proxy.startswith("http://")
    if ":user:" in proxy_line:
        assert auth is not None
    else:
        assert auth is None 