"""Tests for the models module."""

import pytest
from datetime import datetime
from pathlib import Path
from pydantic import ValidationError
from sec_edgar_bulker.core.models import (
    Config,
    ProxySettings,
    DownloadSettings,
    ProcessingSettings,
    HeaderGeneratorSettings,
    LoggingSettings,
    DirectorySettings,
    FilteringSettings,
    Submission,
    Header,
    DocumentMetadata,
    Document,
    ExcludedDocument,
    Progress,
    MasterProgress
)

def test_proxy_settings_validation():
    """Test ProxySettings validation."""
    # Valid settings
    settings = ProxySettings(
        enabled=True,
        file="proxies.txt",
        mode="random",
        required=False,
        timeout=600,
        usage_limit=5
    )
    assert settings.enabled is True
    assert settings.file == "proxies.txt"
    
    # Invalid mode
    with pytest.raises(ValidationError):
        ProxySettings(mode="invalid")

def test_download_settings_validation():
    """Test DownloadSettings validation."""
    settings = DownloadSettings(
        pdfs=True,
        metadata_only=False,
        max_workers=50,
        worker_timeout=4000,
        resume_support=True,
        mode="both"
    )
    assert settings.pdfs is True
    assert settings.max_workers == 50
    
    # Invalid worker count
    with pytest.raises(ValidationError):
        DownloadSettings(max_workers=-1)

def test_processing_settings_validation():
    """Test ProcessingSettings validation."""
    settings = ProcessingSettings(
        batch_id_format="%Y%m%d_%H%M%S",
        cache_downloads=True,
        cache_by_year=True
    )
    assert settings.batch_id_format == "%Y%m%d_%H%M%S"
    
    # Test batch ID format
    batch_id = datetime.now().strftime(settings.batch_id_format)
    assert len(batch_id) > 0

def test_header_generator_settings_validation():
    """Test HeaderGeneratorSettings validation."""
    settings = HeaderGeneratorSettings(
        use_generator=True,
        settings={
            "User_Agent": "Test Agent",
            "Accept_Encoding": "gzip",
            "Host": "www.sec.gov"
        }
    )
    assert settings.use_generator is True
    assert settings.settings.Host == "www.sec.gov"

def test_logging_settings_validation():
    """Test LoggingSettings validation."""
    settings = LoggingSettings(
        level="DEBUG",
        format="%(asctime)s - %(levelname)s - %(message)s",
        file_enabled=True,
        batch_specific=True
    )
    assert settings.level == "DEBUG"
    
    # Invalid log level
    with pytest.raises(ValidationError):
        LoggingSettings(level="INVALID")

def test_directory_settings_validation():
    """Test DirectorySettings validation."""
    settings = DirectorySettings(
        output="output",
        exhibits="exhibits",
        filings="filings",
        logs="logs",
        master_idx="idx_files",
        progress="progress",
        results="results"
    )
    assert settings.output == "output"
    assert settings.exhibits == "exhibits"

def test_filtering_settings_validation():
    """Test FilteringSettings validation."""
    settings = FilteringSettings(
        company_filter_enabled=True,
        date_range_filter_enabled=True,
        ciks=["0000320193"],
        company_names=["Apple Inc."],
        date_range_start="2023-01-01",
        date_range_end="2023-12-31"
    )
    assert settings.company_filter_enabled is True
    assert "0000320193" in settings.ciks
    
    # Invalid date format
    with pytest.raises(ValidationError):
        FilteringSettings(
            date_range_filter_enabled=True,
            date_range_start="invalid",
            date_range_end="invalid"
        )

def test_submission_model():
    """Test Submission model."""
    submission = Submission(
        cik="0000320193",
        company_name="Apple Inc.",
        form_type="10-K",
        date_filed="2023-12-31",
        submission_filename="file.txt",
        accession_number="000123",
        master_file=Path("master.idx"),
        url="http://example.com"
    )
    assert submission.cik == "0000320193"
    assert submission.company_name == "Apple Inc."
    assert isinstance(submission.master_file, Path)

def test_header_model():
    """Test Header model."""
    header = Header(
        accession_number="000123",
        company_name="Apple Inc.",
        cik="0000320193",
        form_type="10-K",
        date_filed=datetime.now(),
        document_count=10
    )
    assert header.accession_number == "000123"
    assert header.document_count == 10

def test_document_metadata_model():
    """Test DocumentMetadata model."""
    metadata = DocumentMetadata(
        document_type="EX-10.1",
        sequence="1",
        filename="exhibit.txt",
        description="Exhibit 10.1",
        url="http://example.com"
    )
    assert metadata.document_type == "EX-10.1"
    assert metadata.sequence == "1"

def test_document_model():
    """Test Document model."""
    metadata = DocumentMetadata(
        document_type="EX-10.1",
        sequence="1",
        filename="exhibit.txt",
        description="Exhibit 10.1",
        url="http://example.com"
    )
    document = Document(
        metadata=metadata,
        content="Test content"
    )
    assert document.metadata.document_type == "EX-10.1"
    assert document.content == "Test content"

def test_excluded_document_model():
    """Test ExcludedDocument model."""
    metadata = DocumentMetadata(
        document_type="EX-10.1",
        sequence="1",
        filename="exhibit.txt",
        description="Exhibit 10.1",
        url="http://example.com"
    )
    excluded = ExcludedDocument(
        metadata=metadata,
        reason="Test reason"
    )
    assert excluded.metadata.document_type == "EX-10.1"
    assert excluded.reason == "Test reason"

def test_progress_model():
    """Test Progress model."""
    progress = Progress(
        idx_file="master.idx",
        last_processed="http://example.com",
        timestamp=datetime.now()
    )
    assert progress.idx_file == "master.idx"
    assert progress.last_processed == "http://example.com"

def test_master_progress_model():
    """Test MasterProgress model."""
    progress = MasterProgress(
        batch_id="20231231_235959",
        idx_files=["master.idx"],
        start_time=datetime.now(),
        status="in_progress"
    )
    assert progress.batch_id == "20231231_235959"
    assert progress.status == "in_progress"
    assert progress.end_time is None

def test_config_model_full(sample_config_dict):
    """Test full Config model validation."""
    config = Config.model_validate(sample_config_dict)
    assert config.years == [2023]
    assert config.quarters == [1, 2]
    assert config.proxy.enabled is False
    assert config.download.pdfs is True
    assert config.header_generator.use_generator is True

@pytest.mark.parametrize("field,value,should_raise", [
    ("years", [], True),  # Empty years list
    ("quarters", [5], True),  # Invalid quarter
    ("proxy.mode", "invalid", True),  # Invalid proxy mode
    ("download.max_workers", 50, False),  # Valid worker count
    ("download.max_workers", -1, True),  # Invalid worker count
])
def test_config_validation_cases(sample_config_dict, field, value, should_raise):
    """Test various Config validation cases."""
    # Modify the config dict based on the field path
    current = sample_config_dict
    field_parts = field.split(".")
    for part in field_parts[:-1]:
        current = current[part]
    current[field_parts[-1]] = value
    
    if should_raise:
        with pytest.raises(ValidationError):
            Config.model_validate(sample_config_dict)
    else:
        config = Config.model_validate(sample_config_dict)
        assert config is not None 