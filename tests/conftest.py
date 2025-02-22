"""Test fixtures for SEC EDGAR Bulker."""

import pytest
from pathlib import Path
from sec_edgar_bulker.core.models import Config

@pytest.fixture
def sample_config_dict():
    """Create a sample configuration dictionary."""
    return {
        "years": [2023],
        "quarters": [1, 2],
        "directories": {
            "output": "output",
            "exhibits": "exhibits",
            "filings": "filings",
            "logs": "logs",
            "master_idx": "idx_files",
            "progress": "progress",
            "results": "results"
        },
        "proxy": {
            "enabled": False,
            "file": "proxies.txt",
            "mode": "random",
            "required": False,
            "timeout": 600,
            "usage_limit": 5
        },
        "download": {
            "pdfs": True,
            "metadata_only": False,
            "max_workers": 50,
            "worker_timeout": 4000,
            "resume_support": True,
            "mode": "both"
        },
        "processing": {
            "batch_id_format": "%Y%m%d_%H%M%S",
            "cache_downloads": True,
            "cache_by_year": True
        },
        "header_generator": {
            "use_generator": True,
            "settings": {
                "User_Agent": "Test Agent",
                "Accept_Encoding": "gzip",
                "Host": "www.sec.gov"
            },
            "random_settings": {
                "email_domains": ["example.com"],
                "navigator": "Test Navigator"
            }
        },
        "logging": {
            "level": "DEBUG",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "file_enabled": True,
            "file": "logs/test.log",
            "max_size": 1048576,
            "backup_count": 3
        },
        "filtering": {
            "company_filter_enabled": False,
            "date_range_filter_enabled": False,
            "ciks": None,
            "company_names": None,
            "date_range_start": None,
            "date_range_end": None
        },
        "metadata": {
            "default_fields": [
                "cik",
                "company_name",
                "form_type",
                "date_filed",
                "accession_number",
                "master_file"
            ]
        },
        "filing_header": {
            "use_config": False,
            "patterns": {
                "sec_document": "DOCUMENT",
                "filing_form_type": "TYPE",
                "conformed_submission_type": "CONFORMED SUBMISSION TYPE",
                "standard_industrial_classification": "STANDARD INDUSTRIAL CLASSIFICATION",
                "acceptance_datetime": "ACCEPTANCE DATETIME",
                "public_document_count": "PUBLIC DOCUMENT COUNT",
                "conformed_period_of_report": "CONFORMED PERIOD OF REPORT",
                "filed_as_of_date": "FILED AS OF DATE"
            }
        }
    }

@pytest.fixture
def sample_config(sample_config_dict):
    """Create a sample configuration object."""
    return Config.model_validate(sample_config_dict)

@pytest.fixture
def sample_proxy_file(tmp_path):
    """Create a sample proxy file."""
    proxy_file = tmp_path / "proxies.txt"
    proxy_file.write_text("""
192.168.1.1:8080
192.168.1.2:8080:user1:pass1
192.168.1.3:8080:user2:pass2
    """.strip())
    return proxy_file

@pytest.fixture
def mock_aioresponse():
    """Create a mock aiohttp response."""
    class MockResponse:
        def __init__(self, status=200, body=""):
            self.status = status
            self._body = body
            
        async def __aenter__(self):
            return self
            
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass
            
        async def text(self):
            return self._body
            
        async def read(self):
            return self._body.encode() if isinstance(self._body, str) else self._body
    
    class MockAioResponse:
        def __init__(self):
            self.responses = {}
            
        def get(self, url, status=200, body="", exception=None):
            if exception:
                self.responses[url] = exception
            else:
                self.responses[url] = MockResponse(status, body)
            
        async def __call__(self, method, url, **kwargs):
            if url in self.responses:
                if isinstance(self.responses[url], Exception):
                    raise self.responses[url]
                return self.responses[url]
            return MockResponse(404)
    
    return MockAioResponse() 