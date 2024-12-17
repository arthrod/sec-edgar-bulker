"""Index file downloader for SEC EDGAR Bulker."""

import asyncio
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiofiles
import aiohttp

from .exceptions import (
    DownloadModeError,
    FileIntegrityError,
    InvalidProxyFormatError,
    NoProxiesAvailableError,
    ProxyFileError,
    TimeoutException,
)
from .header_generator import HeaderGenerator
from .models import Config
from .proxy_manager import ProxyManager

# Old implementation commented out
# async def download_idx_file(year: int, quarter: Optional[int], idx_type: str, session: aiohttp.ClientSession, config: Config) -> bool:
#     """Download an idx file for a specific year and quarter"""
#     downloader = IdxDownloader(config)
#     return await downloader._download_idx_file(year, quarter, idx_type, session)

async def download_idx_file(year: int, quarter: Optional[int], idx_type: str, session: aiohttp.ClientSession, config: Config) -> bool:
    """Download an idx file for a specific year and quarter.
    
    Args:
        year: Year to download
        quarter: Optional quarter to download
        idx_type: Type of index file (e.g., "master", "company", etc.)
        session: aiohttp session to use
        config: Configuration object
        
    Returns:
        bool: True if download was successful, False otherwise
    """
    downloader = IdxDownloader(config)
    try:
        return await downloader._download_idx_file(year, quarter, idx_type, session)
    except Exception as e:
        downloader.logger.error(f"Failed to download idx file for {year} Q{quarter}: {e}")
        return False


class IdxDownloader:
    """IDX file downloader class."""
    
    def __init__(self, config: Config) -> None:
        """Initialize the IDX downloader.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.logger = logging.getLogger("sec_edgar_bulker.idx_downloader")
        self.header_generator = HeaderGenerator(config)
        self.proxy_manager = ProxyManager(config) if config.proxy.enabled else None
        self.temp_files: Dict[str, Path] = {}  # Track temp files for cleanup
        
    async def _download_idx_file(
        self,
        year: int,
        quarter: Optional[int],
        idx_type: str = "master",
        session: Optional[aiohttp.ClientSession] = None,
    ) -> bool:
        """Internal method to download an idx file.
        
        Args:
            year: Year to download
            quarter: Optional quarter to download
            idx_type: Type of index file (e.g., "master", "company", etc.)
            session: Optional aiohttp session to use
            
        Returns:
            bool: True if download was successful, False otherwise
        """
        valid_idx_types = ["master", "company", "form"]
        if idx_type not in valid_idx_types:
            self.logger.error(
                f"Invalid idx_type: {idx_type}. Must be one of {valid_idx_types}"
            )
            return False
            
        url = f"https://www.sec.gov/Archives/edgar/full-index/{year}"
        if quarter:
            url += f"/QTR{quarter}/{idx_type}.idx"
            filename = f"{idx_type}{quarter}{year}.idx"
        else:
            url += f"/{idx_type}.idx"
            filename = f"{idx_type}{year}.idx"
            
        output_dir = Path(self.config.directories.master_idx)
        if not output_dir.exists():
            try:
                output_dir.mkdir(parents=True, exist_ok=True)
                os.chmod(output_dir, 0o755)  # rwxr-xr-x
                self.logger.debug(f"Created directory: {output_dir}")
            except Exception as e:
                self.logger.error(f"Failed to create directory {output_dir}: {e}")
                return False
                
        output_file = output_dir / filename
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file = Path(temp_dir) / f"{filename}.tmp"
            self.temp_files[filename] = temp_file
            
            try:
                if session is None:
                    async with aiohttp.ClientSession() as new_session:
                        success = await self._download_file(
                            new_session, url, output_file, temp_file
                        )
                else:
                    success = await self._download_file(
                        session, url, output_file, temp_file
                    )
                    
                if not success:
                    await self._cleanup_temp_file(temp_file)
                return success
                    
            except Exception as e:
                self.logger.error(f"Failed to download {url}: {e}")
                await self._cleanup_temp_file(temp_file)
                return False
            finally:
                if filename in self.temp_files:
                    del self.temp_files[filename]
            
    async def _cleanup_temp_file(self, temp_file: Path) -> None:
        """Clean up a temporary file.
        
        Args:
            temp_file: Path to temporary file to clean up
        """
        if temp_file.exists():
            try:
                temp_file.unlink()
            except Exception as e:
                self.logger.warning(f"Failed to clean up temp file {temp_file}: {e}")
            
    async def _download_file(
        self,
        session: aiohttp.ClientSession,
        url: str,
        output_file: Path,
        temp_file: Path,
    ) -> bool:
        """Helper method to handle the actual file download.
        
        Args:
            session: aiohttp session to use
            url: URL to download from
            output_file: Final output file path
            temp_file: Temporary file path
            
        Returns:
            bool: True if download was successful, False otherwise
        """
        max_retries = 5  # Increased retries for 403 errors
        retry_delay = 2  # Base delay in seconds
        max_file_size = 100 * 1024 * 1024  # 100MB max file size
        
        for attempt in range(max_retries):
            try:
                # Get proxy and headers
                #proxy = None
                auth = None
                if self.proxy_manager:
                    try:
                        proxy, auth = self.proxy_manager.get_random_proxy()
                    except Exception as e:
                        self.logger.warning(f"Failed to get proxy: {e}")
                
                # Get fresh headers for each attempt
                headers = self.header_generator.get_random_headers()
                
                # Make request with proper headers and proxy
                async with session.get(
                    url,
                    headers=headers,
                    proxy=proxy,
                    proxy_auth=auth,
                    timeout=300,
                    allow_redirects=True,
                ) as response:
                    if response.status == 403:
                        delay = retry_delay * (2 ** attempt)  # Exponential backoff
                        self.logger.warning(
                            f"HTTP 403 error for {url}, retrying in {delay}s..."
                        )
                        await asyncio.sleep(delay)
                        continue
                        
                    if response.status != 200:
                        self.logger.error(f"HTTP {response.status} error for {url}")
                        return False
                        
                    content_length = response.headers.get("Content-Length")
                    if content_length and int(content_length) > max_file_size:
                        self.logger.error(
                            f"File too large: {content_length} bytes "
                            f"(max {max_file_size})"
                        )
                        return False
                        
                    encodings = ["utf-8", "latin1", "ascii", "iso-8859-1"]
                    content = None
                    
                    for encoding in encodings:
                        try:
                            content = await response.text(encoding=encoding)
                            break
                        except UnicodeDecodeError:
                            continue
                            
                    if content is None:
                        self.logger.error(
                            f"Failed to decode content with any encoding: {encodings}"
                        )
                        return False
                    
                    if not content.strip():
                        self.logger.error(f"Empty content received from {url}")
                        return False
                        
                    # Write to temp file first
                    async with aiofiles.open(temp_file, 'w', encoding='utf-8') as f:
                        await f.write(content)
                        
                    # Verify file size and content
                    file_size = temp_file.stat().st_size
                    if 0 < file_size <= max_file_size:
                        # Remove existing file if it exists
                        if output_file.exists():
                            try:
                                output_file.unlink()
                            except Exception as e:
                                self.logger.error(
                                    f"Failed to remove existing file {output_file}: {e}"
                                )
                                return False
                                
                        # Move temp file to final location
                        try:
                            # Ensure parent directory exists and is empty
                            output_file.parent.mkdir(parents=True, exist_ok=True)
                            
                            # Copy file and set permissions
                            shutil.copy2(temp_file, output_file)
                            os.chmod(output_file, 0o644)  # rw-r--r--
                            
                            self.logger.debug(
                                f"Successfully downloaded {url} to {output_file}"
                            )
                            return True
                        except Exception as e:
                            self.logger.error(
                                f"Failed to move temp file to {output_file}: {e}"
                            )
                            return False
                    else:
                        self.logger.error(f"Invalid file size: {file_size} bytes")
                        return False
                        
            except asyncio.TimeoutError:
                delay = retry_delay * (2 ** attempt)
                self.logger.warning(f"Timeout downloading {url}, attempt {attempt + 1}/{max_retries}, retrying in {delay}s")
                if attempt == max_retries - 1:
                    raise TimeoutException(f"Download timed out after {max_retries} attempts")
                await asyncio.sleep(delay)
            except Exception as e:
                self.logger.error(f"Error downloading {url}: {e}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(retry_delay * (2 ** attempt))
            
        return False
            
    async def download_and_process_idx_files(self, years: List[int], quarters: Optional[List[int]] = None, idx_type: str = "master") -> Tuple[int, int]:
        """Download and process multiple idx files.
        
        Args:
            years: List of years to download
            quarters: Optional list of quarters to download
            idx_type: Type of index file (e.g., "master", "company", etc.)
            
        Returns:
            Tuple of (successful downloads, total attempted downloads)
        """
        # Validate idx_type
        valid_idx_types = ["master", "company", "form"]
        if idx_type not in valid_idx_types:
            self.logger.error(f"Invalid idx_type: {idx_type}. Must be one of {valid_idx_types}")
            return 0, 0
            
        total = 0
        successful = 0
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for year in years:
                if quarters:
                    tasks.extend([
                        self._download_idx_file(year, quarter, idx_type, session)
                        for quarter in quarters
                    ])
                    total += len(quarters)
                else:
                    tasks.append(self._download_idx_file(year, None, idx_type, session))
                    total += 1
                    
            results = await asyncio.gather(*tasks, return_exceptions=True)
            successful = sum(1 for result in results if result is True)
            
            # Log summary
            self.logger.info(f"Downloaded {successful} of {total} idx files")
                    
        return successful, total
            
    async def cleanup(self) -> None:
        """Clean up any temporary files."""
        # Clean up tracked temp files
        for filename, temp_file in self.temp_files.items():
            await self._cleanup_temp_file(temp_file)
            
        # Clean up temp files in master_idx directory
        idx_dir = Path(self.config.directories.master_idx)
        if idx_dir.exists():
            try:
                # Only remove .tmp files
                for temp_file in idx_dir.glob("*.tmp"):
                    try:
                        temp_file.unlink()
                    except Exception as e:
                        self.logger.warning(f"Failed to remove temp file {temp_file}: {e}")
            except Exception as e:
                self.logger.error(f"Failed to clean up directory {idx_dir}: {e}")

async def download_and_process_idx_files(config: Config, years: Optional[List[int]] = None, quarters: Optional[List[int]] = None, idx_type: str = "master") -> Tuple[int, int]:
    """Helper function to download and process idx files without creating an instance.
    
    Args:
        config: Configuration object
        years: Optional list of years to download (defaults to config.years)
        quarters: Optional list of quarters to download (defaults to config.quarters)
        idx_type: Type of index file (e.g., "master", "company", etc.)
        
    Returns:
        Tuple of (successful downloads, total attempted downloads)
    """
    downloader = IdxDownloader(config)
    try:
        years = years or config.years
        quarters = quarters or config.quarters
        return await downloader.download_and_process_idx_files(years, quarters, idx_type)
    finally:
        await downloader.cleanup()

__all__ = [
    'IdxDownloader',
    'download_idx_file',
    'download_and_process_idx_files'
]