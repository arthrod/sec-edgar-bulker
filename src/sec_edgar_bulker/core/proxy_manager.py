"""Proxy manager for SEC EDGAR requests."""

import random
from typing import List, Dict, Optional, Tuple, Set
import aiohttp
import re
import logging
import os
from pathlib import Path
import ipaddress

from .models import Config
from .exceptions import NoProxiesAvailableError, InvalidProxyFormatError, ProxyFileError

class ProxyManager:
    """Proxy manager for handling proxy rotation and authentication."""
    
    def __init__(self, config: Config):
        """Initialize the proxy manager.
        
        Args:
            config: Configuration object containing proxy settings
            
        Raises:
            NoProxiesAvailableError: If proxies are required but not available
        """
        self.config = config
        self.settings = config.proxy
        self.mode = self.settings.mode
        self.proxies: List[Tuple[str, Optional[aiohttp.BasicAuth]]] = []
        self.proxy_usage_count: Dict[str, int] = {}
        self.used_proxies: Set[str] = set()  # Track used proxies for rotation
        self.logger = logging.getLogger("sec_edgar_bulker.proxy")
        
        # Always check required state first
        if self.settings.required:
            if not self.settings.enabled:
                self.logger.error("Proxy is required but proxy settings are disabled")
                raise NoProxiesAvailableError("Proxy is required but proxy settings are disabled")
            
        if self.settings.enabled:
            self.load_proxies()
            
    def load_proxies(self) -> None:
        """Load proxies from file.
        
        Raises:
            NoProxiesAvailableError: If required proxies are not available
            InvalidProxyFormatError: If proxy format is invalid
            ProxyFileError: If proxy file cannot be read
        """
        try:
            proxy_file = Path(self.settings.file)
            if not proxy_file.exists():
                self.logger.error(f"Proxy file not found: {proxy_file}")
                if self.settings.required:
                    raise NoProxiesAvailableError("Required proxy file not found")
                raise ProxyFileError(f"Failed to read proxy file: {proxy_file} not found")
                
            with open(proxy_file, "r", encoding='utf-8') as f:
                lines = [line.strip() for line in f.readlines() if line.strip() and not line.startswith("#")]
                
            if not lines:
                msg = "Empty proxy file"
                if self.settings.required:
                    raise NoProxiesAvailableError(msg)
                raise ProxyFileError(msg)
                
            # Improved proxy pattern to handle both formats:
            # - IP:PORT
            # - IP:PORT:USER:PASS
            ip_pattern = r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
            port_pattern = r'^([1-9][0-9]{0,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$'
                
            for line in lines:
                parts = line.split(":")
                if len(parts) not in [2, 4]:
                    self.logger.error(f"Invalid proxy format (wrong number of parts): {line}")
                    raise InvalidProxyFormatError(f"Invalid proxy format: {line} - must be IP:PORT or IP:PORT:USER:PASS")
                    
                # Validate IP
                try:
                    ipaddress.ip_address(parts[0])
                except ValueError:
                    self.logger.error(f"Invalid IP address: {parts[0]}")
                    raise InvalidProxyFormatError(f"Invalid IP address: {parts[0]}")
                    
                # Validate port
                if not re.match(port_pattern, parts[1]):
                    self.logger.error(f"Invalid port number: {parts[1]}")
                    raise InvalidProxyFormatError(f"Invalid port number: {parts[1]} - must be between 1 and 65535")
                    
                proxy = f"http://{parts[0]}:{parts[1]}"
                auth = None
                
                if len(parts) == 4:
                    # Validate username and password
                    if not parts[2] or not parts[3]:
                        self.logger.error("Empty username or password")
                        raise InvalidProxyFormatError("Username and password cannot be empty")
                        
                    # Create auth with proper encoding
                    auth = aiohttp.BasicAuth(
                        login=parts[2],
                        password=parts[3],
                        encoding='utf-8'
                    )
                    # Log masked credentials
                    masked_user = '*' * len(parts[2])
                    masked_pass = '*' * len(parts[3])
                    self.logger.debug(f"Adding proxy with auth - user: {masked_user}, pass: {masked_pass}")
                else:
                    self.logger.debug(f"Adding proxy without auth: {parts[0]}:****")
                    
                # Only add unique proxies
                if proxy not in self.proxy_usage_count:
                    self.proxies.append((proxy, auth))
                    self.proxy_usage_count[proxy] = 0
                    
            if not self.proxies and self.settings.required:
                raise NoProxiesAvailableError("No valid proxies found in proxy file")
                
        except FileNotFoundError:
            self.logger.error(f"Proxy file not found: {self.settings.file}")
            if self.settings.required:
                raise NoProxiesAvailableError("Required proxy file not found")
            raise ProxyFileError(f"Failed to read proxy file: {self.settings.file} not found")
        except InvalidProxyFormatError:
            if self.settings.required:
                raise NoProxiesAvailableError("No valid proxies available")
            raise
        except Exception as e:
            self.logger.error(f"Error parsing proxy file: {e}")
            if self.settings.required:
                raise NoProxiesAvailableError("Failed to load required proxies")
            raise InvalidProxyFormatError(f"Error parsing proxy file: {e}")
            
    def _reset_usage_counts(self) -> None:
        """Reset proxy usage counts and tracking."""
        self.proxy_usage_count = {proxy[0]: 0 for proxy in self.proxies}
        self.used_proxies.clear()
            
    def get_random_proxy(self) -> Optional[Tuple[str, Optional[aiohttp.BasicAuth]]]:
        """Get a proxy based on random selection.
        
        Returns:
            Tuple of (proxy_url, auth) or None if no proxies available
            
        Raises:
            NoProxiesAvailableError: If proxies are required but none are available
        """
        if not self.settings.enabled or not self.proxies:
            if self.settings.required:
                raise NoProxiesAvailableError("No proxies available and proxy is required")
            return None
            
        # Get proxies under usage limit
        available_proxies = [
            p for p in self.proxies 
            if self.proxy_usage_count[p[0]] < self.settings.usage_limit
        ]
        
        if not available_proxies:
            if self.settings.required:
                raise NoProxiesAvailableError("No proxies available within usage limit")
            # Reset usage counts if all proxies are at limit
            self.logger.info("All proxies at usage limit, resetting counts")
            self._reset_usage_counts()
            available_proxies = self.proxies
            
        # Prefer unused proxies for better rotation
        unused_proxies = [p for p in available_proxies if p[0] not in self.used_proxies]
        if unused_proxies:
            proxy = random.choice(unused_proxies)
        else:
            proxy = random.choice(available_proxies)
            
        self.proxy_usage_count[proxy[0]] = self.proxy_usage_count.get(proxy[0], 0) + 1
        self.used_proxies.add(proxy[0])
        
        # Reset used_proxies if all proxies have been used
        if len(self.used_proxies) == len(self.proxies):
            self.used_proxies.clear()
            
        # Log masked proxy info
        host = proxy[0].split("//")[-1].split(":")[0]
        port = proxy[0].split(":")[-1]
        auth_str = " (with auth)" if proxy[1] else ""
        self.logger.debug(f"Selected random proxy: {host}:****{auth_str} (usage: {self.proxy_usage_count[proxy[0]]})")
        return proxy
            
    def get_proxy(self) -> Optional[Tuple[str, Optional[aiohttp.BasicAuth]]]:
        """Get a proxy based on the configured mode.
        
        Returns:
            Tuple of (proxy_url, auth) or None if no proxies available
            
        Raises:
            NoProxiesAvailableError: If proxies are required but none are available
        """
        if not self.settings.enabled or not self.proxies:
            if self.settings.required:
                raise NoProxiesAvailableError("No proxies available and proxy is required")
            return None
            
        if self.mode == "random":
            return self.get_random_proxy()
            
        # LRU mode
        available_proxies = [
            p for p in self.proxies 
            if self.proxy_usage_count.get(p[0], 0) < self.settings.usage_limit
        ]
        
        if not available_proxies:
            if self.settings.required:
                raise NoProxiesAvailableError("No proxies available within usage limit")
            # Reset usage counts if all proxies are at limit
            self.logger.info("All proxies at usage limit, resetting counts")
            self._reset_usage_counts()
            available_proxies = self.proxies
            
        # Get proxy with lowest usage count
        proxy = min(available_proxies, key=lambda p: self.proxy_usage_count.get(p[0], 0))
        self.proxy_usage_count[proxy[0]] = self.proxy_usage_count.get(proxy[0], 0) + 1
        self.used_proxies.add(proxy[0])
        
        # Reset used_proxies if all proxies have been used
        if len(self.used_proxies) == len(self.proxies):
            self.used_proxies.clear()
            
        # Log masked proxy info
        host = proxy[0].split("//")[-1].split(":")[0]
        port = proxy[0].split(":")[-1]
        auth_str = " (with auth)" if proxy[1] else ""
        self.logger.debug(f"Selected LRU proxy: {host}:****{auth_str} (usage: {self.proxy_usage_count[proxy[0]]})")
        return proxy
            
    def __repr__(self) -> str:
        """Debug representation with masked credentials."""
        return self.__str__()
            
    def __str__(self) -> str:
        """String representation without exposing credentials."""
        if not self.proxies:
            return "ProxyManager(disabled)"
            
        masked_proxies = []
        for proxy, auth in self.proxies:
            host = proxy.split("//")[-1].split(":")[0]
            port = proxy.split(":")[-1]
            if auth:
                masked_proxies.append(f"{host}:****")  # Don't expose port number
            else:
                masked_proxies.append(f"{host}:****")  # Don't expose port number
        return f"ProxyManager({', '.join(masked_proxies)})"