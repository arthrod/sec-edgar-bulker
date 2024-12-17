"""Header generator for SEC EDGAR requests."""

import random
import re
from typing import Dict

from .models import Config, HeaderGeneratorSettings, StaticHeaders


class HeaderGenerator:
    """Generates headers for SEC EDGAR requests.
    
    Attributes:
        config: The configuration object.
        settings: Header generator settings from config.
        _static_headers: Base headers used for all requests.
    """
    
    def __init__(self, config: Config) -> None:
        """Initialize the header generator.
        
        Args:
            config: Configuration object containing header settings.
        """
        self.config = config
        self.settings = config.header_generator
        #base_headers = self.settings.settings.to_dict()
        #self._static_headers = {
         #   **base_headers,
          #  "Connection": "keep-alive",
           # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
         #   "Accept-Language": "en-US,en;q=0.5"
        #}
        self._static_headers = self.settings.settings.to_dict()
        
    def _validate_email_domain(self, domain: str) -> bool:
        """Validate email domain format.
        
        Args:
            domain: Email domain to validate.
            
        Returns:
            bool: True if domain is valid, False otherwise.
        """
        cleaned_domain = domain.strip('() \t\n\r')
        domain_pattern = re.compile(
            r'^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$'
        )
        return bool(domain_pattern.match(cleaned_domain))
    
    def get_random_headers(self) -> Dict[str, str]:
        """Generate random headers based on settings.
        
        Returns:
            Dict[str, str]: Dictionary of HTTP headers.
            
        Raises:
            ValueError: If email domain format is invalid.
        """
        if not self.settings.use_generator:
            return self._static_headers.copy()
            
        headers = self._static_headers.copy()
        
        if self.settings.random_settings:
            domain = random.choice(self.settings.random_settings.email_domains)
            cleaned_domain = domain.strip('() \t\n\r')
            if not self._validate_email_domain(cleaned_domain):
                raise ValueError(f"Invalid email domain format: {domain}")
                
            email = f"user{random.randint(1000,9999)}@{cleaned_domain}"
            headers.update({
                "From": email,
                "User-Agent": f"{self.settings.random_settings.navigator}/SEC-EDGAR-Bulker {email}"
            })
            
        return headers
        
    @property
    def static_headers(self) -> Dict[str, str]:
        """Get static headers.
        
        Returns:
            Dict[str, str]: Copy of static headers dictionary.
        """
        return self._static_headers.copy()
        
    @static_headers.setter
    def static_headers(self, headers: Dict[str, str]) -> None:
        """Set static headers.
        
        Args:
            headers: Dictionary of headers to set.
        """
        self._static_headers = headers.copy()