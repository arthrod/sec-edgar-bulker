"""Tests for the IDX downloader module."""

import pytest
from pathlib import Path
import aiohttp
import asyncio
from unittest.mock import Mock, patch

from sec_edgar_bulker.core.idx_downloader import download_idx_file, download_and_process_idx_files, IdxDownloader
from sec_edgar_bulker.core.models import Config

@pytest.mark.asyncio
async def test_download_idx_file_success(sample_config, mock_aioresponse):
    """Test successful index file download."""
    year = 2023
    quarter = 1
    idx_type = "master"
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/{idx_type}.idx"
    
    # Mock successful response
    mock_aioresponse.get(url, status=200, body="CIK|Company Name|Form Type|Date Filed|Filename")
    
    async with aiohttp.ClientSession() as session:
        success = await download_idx_file(year, quarter, idx_type, session, sample_config)
    
    assert success is True
    output_file = Path(sample_config.directories.master_idx) / f"{idx_type}{quarter}{year}.idx"
    assert output_file.exists()
    assert output_file.read_text() == "CIK|Company Name|Form Type|Date Filed|Filename"

@pytest.mark.asyncio
async def test_download_idx_file_failure(sample_config, mock_aioresponse):
    """Test failed index file download."""
    year = 2023
    quarter = 1
    idx_type = "master"
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/{idx_type}.idx"
    
    # Mock failed response
    mock_aioresponse.get(url, status=404)
    
    async with aiohttp.ClientSession() as session:
        success = await download_idx_file(year, quarter, idx_type, session, sample_config)
    
    assert success is False
    output_file = Path(sample_config.directories.master_idx) / f"{idx_type}{quarter}{year}.idx"
    assert not output_file.exists()

@pytest.mark.asyncio
async def test_download_idx_file_yearly(sample_config, mock_aioresponse):
    """Test yearly index file download."""
    year = 2023
    quarter = None
    idx_type = "master"
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/{idx_type}.idx"
    
    # Mock successful response
    mock_aioresponse.get(url, status=200, body="CIK|Company Name|Form Type|Date Filed|Filename")
    
    async with aiohttp.ClientSession() as session:
        success = await download_idx_file(year, quarter, idx_type, session, sample_config)
    
    assert success is True
    output_file = Path(sample_config.directories.master_idx) / f"{idx_type}{year}.idx"
    assert output_file.exists()
    assert output_file.read_text() == "CIK|Company Name|Form Type|Date Filed|Filename"

@pytest.mark.asyncio
async def test_download_idx_file_network_error(sample_config, mock_aioresponse):
    """Test index file download with network error."""
    year = 2023
    quarter = 1
    idx_type = "master"
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/{idx_type}.idx"
    
    # Mock network error
    mock_aioresponse.get(url, exception=aiohttp.ClientError())
    
    async with aiohttp.ClientSession() as session:
        success = await download_idx_file(year, quarter, idx_type, session, sample_config)
    
    assert success is False

@pytest.mark.asyncio
async def test_download_and_process_idx_files_success(sample_config, mock_aioresponse):
    """Test successful download and processing of all index files."""
    # Mock responses for all quarters
    for year in sample_config.years:
        for quarter in sample_config.quarters:
            url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"
            mock_aioresponse.get(url, status=200, body=f"Content for {year} Q{quarter}")
    
    await download_and_process_idx_files(sample_config)
    
    # Verify all files were downloaded
    idx_dir = Path(sample_config.directories.master_idx)
    for year in sample_config.years:
        for quarter in sample_config.quarters:
            idx_file = idx_dir / f"{idx_type}{quarter}{year}.idx"
            assert idx_file.exists()
            assert idx_file.read_text() == f"Content for {year} Q{quarter}"

@pytest.mark.asyncio
async def test_download_and_process_idx_files_partial_failure(sample_config, mock_aioresponse):
    """Test partial failure in downloading index files."""
    # Mock mixed responses (success and failure)
    year = sample_config.years[0]
    quarter1, quarter2 = sample_config.quarters
    
    url1 = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter1}/master.idx"
    url2 = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter2}/master.idx"
    
    mock_aioresponse.get(url1, status=200, body=f"Content for {year} Q{quarter1}")
    mock_aioresponse.get(url2, status=404)
    
    await download_and_process_idx_files(sample_config)
    
    # Verify successful download
    idx_dir = Path(sample_config.directories.master_idx)
    success_file = idx_dir / f"{idx_type}{quarter1}{year}.idx"
    assert success_file.exists()
    assert success_file.read_text() == f"Content for {year} Q{quarter1}"
    
    # Verify failed download
    failed_file = idx_dir / f"{idx_type}{quarter2}{year}.idx"
    assert not failed_file.exists()

@pytest.mark.asyncio
async def test_download_idx_file_creates_directory(sample_config, mock_aioresponse):
    """Test that the output directory is created if it doesn't exist."""
    year = 2023
    quarter = 1
    idx_type = "master"
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/{idx_type}.idx"
    
    # Remove the directory if it exists
    idx_dir = Path(sample_config.directories.master_idx)
    if idx_dir.exists():
        idx_dir.rmdir()
    
    # Mock successful response
    mock_aioresponse.get(url, status=200, body="Test content")
    
    async with aiohttp.ClientSession() as session:
        success = await download_idx_file(year, quarter, idx_type, session, sample_config)
    
    assert success is True
    assert idx_dir.exists()
    assert idx_dir.is_dir()

@pytest.mark.asyncio
async def test_download_idx_file_overwrites_existing(sample_config, mock_aioresponse):
    """Test that existing index files are overwritten."""
    year = 2023
    quarter = 1
    idx_type = "master"
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/{idx_type}.idx"
    
    # Create existing file with old content
    idx_dir = Path(sample_config.directories.master_idx)
    idx_dir.mkdir(parents=True, exist_ok=True)
    output_file = idx_dir / f"{idx_type}{quarter}{year}.idx"
    output_file.write_text("Old content")
    
    # Mock successful response
    mock_aioresponse.get(url, status=200, body="New content")
    
    async with aiohttp.ClientSession() as session:
        success = await download_idx_file(year, quarter, idx_type, session, sample_config)
    
    assert success is True
    assert output_file.read_text() == "New content"

@pytest.mark.asyncio
async def test_download_idx_file_large_content(sample_config, mock_aioresponse):
    """Test downloading a large index file."""
    year = 2023
    quarter = 1
    idx_type = "master"
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/{idx_type}.idx"
    
    # Generate large content (1MB)
    large_content = "x" * (1024 * 1024)
    
    # Mock successful response
    mock_aioresponse.get(url, status=200, body=large_content)
    
    async with aiohttp.ClientSession() as session:
        success = await download_idx_file(year, quarter, idx_type, session, sample_config)
    
    assert success is True
    output_file = Path(sample_config.directories.master_idx) / f"{idx_type}{quarter}{year}.idx"
    assert output_file.exists()
    assert output_file.stat().st_size >= len(large_content)

@pytest.mark.asyncio
async def test_download_idx_file_unicode_content(sample_config, mock_aioresponse):
    """Test downloading index file with Unicode content."""
    year = 2023
    quarter = 1
    idx_type = "master"
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/{idx_type}.idx"
    
    # Content with Unicode characters
    unicode_content = "CIK|Company Name 株式会社|Form Type|Date Filed|Filename"
    
    # Mock successful response
    mock_aioresponse.get(url, status=200, body=unicode_content)
    
    async with aiohttp.ClientSession() as session:
        success = await download_idx_file(year, quarter, idx_type, session, sample_config)
    
    assert success is True
    output_file = Path(sample_config.directories.master_idx) / f"{idx_type}{quarter}{year}.idx"
    assert output_file.exists()
    assert output_file.read_text() == unicode_content 