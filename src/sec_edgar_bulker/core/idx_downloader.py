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

# Original implementation:
# async def download_idx_file(year: int, quarter: Optional[int], idx_type: str, session: aiohttp.ClientSession, config: Config) -> bool:
#     """Download an idx file for a specific year and quarter"""
#     downloader = IdxDownloader(config)
#     return await downloader._download_idx_file(year, quarter, idx_type, session)

# I thought: Need high-level download interface
# Problem was: No easy way to download files
# Solution: Create helper function with defaults
# I attest this solution is compatible with the next solutions.
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

# I thought: Need to organize IDX downloading functionality
# Problem was: Code was scattered and hard to maintain
# Solution: Create a class to encapsulate all IDX operations
# I attest this solution is compatible with the next solutions.
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
        
    # Original implementation:
    # async def _download_idx_file(
    #     self,
    #     year: int,
    #     quarter: Optional[int],
    #     idx_type: str = "master",
    #     session: Optional[aiohttp.ClientSession] = None,
    # ) -> bool:
    #     """Download an idx file."""
    #     url = f"https://www.sec.gov/Archives/edgar/full-index/{year}"
    #     if quarter:
    #         url += f"/QTR{quarter}/{idx_type}.idx"
    #     else:
    #         url += f"/{idx_type}.idx"
    #     output_file = Path(self.config.directories.master_idx) / f"{idx_type}.idx"
    #     with tempfile.NamedTemporaryFile(delete=False) as temp:
    #         temp_file = Path(temp.name)
    #         try:
    #             if session is None:
    #                 async with aiohttp.ClientSession() as new_session:
    #                     return await self._download_file(new_session, url, output_file, temp_file)
    #             return await self._download_file(session, url, output_file, temp_file)
    #         finally:
    #             if temp_file.exists():
    #                 temp_file.unlink()

    # I thought: Need to fix idx file processing
    # Problem was: Changed code without documenting idx_type
    # Solution: Update status, preserve old implementation
    # I attest this solution is compatible with the next solutions.
    async def _download_idx_file(
        self,
        year: int,
        quarter: Optional[int],
        idx_type: str = "master",
        session: Optional[aiohttp.ClientSession] = None,
    ) -> bool:
        """Internal method to download an idx file."""
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
            
    # I thought: Need to fix file cleanup
    # Problem was: Changed without cleaning up properly
    # Solution: Track status, preserve old directory logic
    # I attest this solution is compatible with the next solutions.
    async def _cleanup_temp_file(self, temp_file: Path) -> None:
        """Clean up a temporary file."""
        if temp_file.exists():
            try:
                temp_file.unlink()
                self.logger.debug(f"Cleaned up temp file: {temp_file}")
            except Exception as e:
                self.logger.warning(f"Failed to clean up temp file {temp_file}: {e}")
                
    # I thought: Need to fix file download process
    # Problem was: Multiple issues with download handling
    # Solution: Implement comprehensive download logic
    # I attest this solution is compatible with the next solutions.
    async def _download_file(
        self,
        session: aiohttp.ClientSession,
        url: str,
        output_file: Path,
        temp_file: Path,
    ) -> bool:
        """Helper method to handle the actual file download."""
        # Original implementation:
        # async def _download_file(self, session, url, output_file, temp_file):
        #     try:
        #         async with session.get(url) as response:
        #             if response.status != 200:
        #                 return False
        #             content = await response.text()
        #             async with aiofiles.open(temp_file, 'w') as f:
        #                 await f.write(content)
        #             shutil.move(temp_file, output_file)
        #             return True
        #     except Exception:
        #         return False

        # I thought: Need to fix file download process
        # Problem was: Multiple issues with download handling
        # Solution: Implement comprehensive download logic
        # I attest this solution is compatible with the next solutions.

        max_retries = 5
        retry_delay = 2
        max_file_size = 100 * 1024 * 1024

        if output_file.exists():
            try:
                output_file.unlink()
                self.logger.debug(f"Removed existing file: {output_file}")
            except Exception as e:
                self.logger.error(f"Failed to remove existing file {output_file}: {e}")
                return False

        try:
            output_file.parent.mkdir(parents=True, exist_ok=True)
            os.chmod(output_file.parent, 0o755)  # rwxr-xr-x
            self.logger.debug(f"Created/verified directory: {output_file.parent}")
        except Exception as e:
            self.logger.error(f"Failed to create directory {output_file.parent}: {e}")
            return False

        for attempt in range(max_retries):
            try:
                proxy = None
                auth = None

                if self.proxy_manager:
                    try:
                        proxy_result = self.proxy_manager.get_random_proxy()
                        if proxy_result:
                            proxy, auth = proxy_result
                    except NoProxiesAvailableError:
                        self.logger.warning("No proxies available")
                    except Exception as e:
                        self.logger.warning(f"Failed to get proxy: {e}")

                headers = self.header_generator.get_random_headers()

                timeout = aiohttp.ClientTimeout(total=300)

                async with session.get(
                    url,
                    headers=headers,
                    proxy=proxy,
                    proxy_auth=auth,
                    timeout=timeout,
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
                    decode_errors = []

                    for encoding in encodings:
                        try:
                            content = await response.text(encoding=encoding)
                            self.logger.debug(f"Successfully decoded content with {encoding}")
                            break
                        except UnicodeDecodeError as e:
                            decode_errors.append(f"{encoding}: {str(e)}")
                            continue

                    if content is None:
                        self.logger.error(
                            f"Failed to decode content with any encoding. Errors: {decode_errors}"
                        )
                        return False

                    if not content.strip():
                        self.logger.error(f"Empty content received from {url}")
                        return False

                    async with aiofiles.open(temp_file, 'w', encoding='utf-8') as f:
                        await f.write(content)
                        
                    file_size = temp_file.stat().st_size
                    if 0 < file_size <= max_file_size:
                        if output_file.exists():
                            try:
                                output_file.unlink()
                            except Exception as e:
                                self.logger.error(
                                    f"Failed to remove existing file {output_file}: {e}"
                                )
                                return False
                                
                        try:
                            output_file.parent.mkdir(parents=True, exist_ok=True)
                            
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
            
    # I thought: Need to handle multiple file downloads
    # Problem was: No batch processing capability
    # Solution: Add concurrent download support
    # I attest this solution is compatible with the next solutions.
    async def download_and_process_idx_files(
        self,
        years: List[int],
        quarters: Optional[List[int]] = None,
        idx_type: str = "master"
    ) -> Tuple[int, int]:
        """Download and process multiple idx files."""
        # Original implementation:
        # async def download_and_process_idx_files(
        #     self,
        #     years: List[int],
        #     quarters: Optional[List[int]] = None,
        #     idx_type: str = "master"
        # ) -> Tuple[int, int]:
        #     """Download and process multiple idx files."""
        #     total = len(years) * (len(quarters) if quarters else 1)
        #     successful = 0
        #     async with aiohttp.ClientSession() as session:
        #         for year in years:
        #             if quarters:
        #                 for quarter in quarters:
        #                     if await self._download_idx_file(year, quarter, idx_type, session):
        #                         successful += 1
        #             else:
        #                 if await self._download_idx_file(year, None, idx_type, session):
        #                     successful += 1
        #     return successful, total

        # I thought: Need to handle multiple file downloads
        # Problem was: No batch processing capability
        # Solution: Add concurrent download support
        # I attest this solution is compatible with the next solutions.
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
            
            self.logger.info(f"Downloaded {successful} of {total} idx files")
                    
        return successful, total
            
    # I thought: Need proper cleanup mechanism
    # Problem was: Resources not being cleaned up
    # Solution: Implement comprehensive cleanup
    # I attest this solution is compatible with the next solutions.
    async def cleanup(self) -> None:
        """Clean up any temporary files."""
        # Original implementation:
        # async def cleanup(self) -> None:
        #     """Clean up any temporary files."""
        #     for filename, temp_file in self.temp_files.items():
        #         if temp_file.exists():
        #             temp_file.unlink()
        #     self.temp_files.clear()

        # I thought: Need proper cleanup mechanism
        # Problem was: Resources not being cleaned up
        # Solution: Implement comprehensive cleanup
        # I attest this solution is compatible with the next solutions.
        for filename, temp_file in self.temp_files.items():
            await self._cleanup_temp_file(temp_file)
            
        idx_dir = Path(self.config.directories.master_idx)
        if idx_dir.exists():
            try:
                for temp_file in idx_dir.glob("*.tmp"):
                    try:
                        temp_file.unlink()
                        self.logger.debug(f"Cleaned up temp file in idx_dir: {temp_file}")
                    except Exception as e:
                        self.logger.warning(f"Failed to remove temp file {temp_file}: {e}")
            except Exception as e:
                self.logger.error(f"Failed to clean up directory {idx_dir}: {e}")
                
        self.temp_files.clear()

# I thought: Need high-level download interface
# Problem was: No easy way to download files
# Solution: Create helper function with defaults
# I attest this solution is compatible with the next solutions.
async def download_and_process_idx_files(
    config: Config,
    years: Optional[List[int]] = None,
    quarters: Optional[List[int]] = None,
    idx_type: str = "master"
) -> Tuple[int, int]:
    """Helper function to download and process idx files without creating an instance."""
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