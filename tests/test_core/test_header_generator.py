"""Tests for the header generator module."""

import pytest
from sec_edgar_bulker.core.header_generator import HeaderGenerator
from sec_edgar_bulker.core.models import Config

@pytest.fixture
def header_generator(sample_config):
    """Create a header generator instance."""
    return HeaderGenerator(sample_config)

def test_header_generator_init(header_generator, sample_config):
    """Test header generator initialization."""
    assert header_generator.config == sample_config
    assert header_generator.settings == sample_config.header_generator

def test_get_random_headers_with_generator(header_generator):
    """Test getting random headers with generator enabled."""
    headers = header_generator.get_random_headers()
    assert "User-Agent" in headers
    assert "Accept-Encoding" in headers
    assert "Host" in headers
    assert "Connection" in headers
    assert "Accept" in headers
    assert "@" in headers["User-Agent"]  # Should contain email
    assert "SEC-EDGAR-Bulker" in headers["User-Agent"]

def test_get_random_headers_without_generator(sample_config):
    """Test getting headers with generator disabled."""
    sample_config.header_generator.use_generator = False
    generator = HeaderGenerator(sample_config)
    headers = generator.get_random_headers()
    assert headers == dict(sample_config.header_generator.settings.static_headers)

def test_get_random_headers_without_random_settings(sample_config):
    """Test getting headers without random settings."""
    sample_config.header_generator.random_settings = None
    generator = HeaderGenerator(sample_config)
    headers = generator.get_random_headers()
    assert headers["User-Agent"] == sample_config.header_generator.settings.User_Agent

@pytest.mark.security
def test_header_generator_no_sensitive_data(header_generator):
    """Test that no sensitive data is included in headers."""
    headers = header_generator.get_random_headers()
    header_str = str(headers)
    assert "password" not in header_str.lower()
    assert "secret" not in header_str.lower()
    assert "token" not in header_str.lower()
    assert "api_key" not in header_str.lower()

def test_header_generator_unique_headers(header_generator):
    """Test that multiple calls generate different headers."""
    headers1 = header_generator.get_random_headers()
    headers2 = header_generator.get_random_headers()
    assert headers1["User-Agent"] != headers2["User-Agent"]

def test_header_generator_required_fields(header_generator):
    """Test that all required header fields are present."""
    headers = header_generator.get_random_headers()
    required_fields = {
        "User-Agent",
        "Accept-Encoding",
        "Host",
        "Connection",
        "Accept"
    }
    assert all(field in headers for field in required_fields)

def test_header_generator_valid_email_format(header_generator):
    """Test that generated email addresses are valid."""
    headers = header_generator.get_random_headers()
    user_agent = headers["User-Agent"]
    email_part = user_agent.split()[-1]
    assert "@" in email_part
    username, domain = email_part.split("@")
    assert username
    assert domain in header_generator.settings.random_settings.email_domains

def test_header_generator_consistent_host(header_generator):
    """Test that Host header is consistent."""
    headers = header_generator.get_random_headers()
    assert headers["Host"] == "www.sec.gov"

@pytest.mark.parametrize("encoding", ["gzip", "deflate"])
def test_header_generator_encoding_options(header_generator, encoding):
    """Test that Accept-Encoding header contains valid options."""
    headers = header_generator.get_random_headers()
    assert encoding in headers["Accept-Encoding"] 