"""Main downloader for SEC EDGAR Bulker."""

import asyncio
import aiohttp
import aiofiles
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from tqdm import tqdm

from .models import Config, Submission, Document, DocumentMetadata, ExcludedDocument
from .exceptions import (
    TimeoutException,
    ParsingError,
    DownloadModeError,
    InvalidFormTypeError,
    MissingMetadataError,
    FileIntegrityError,
)
from .proxy_manager import ProxyManager
from .header_generator import HeaderGenerator
from .utils import setup_logging

class EdgarDownloader:
    """Downloads and processes SEC EDGAR filings."""
    
    def __init__(self, config: Config):
        """Initialize the downloader.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.base_url = "http://www.sec.gov"
        self.batch_id = datetime.now().strftime(config.processing.batch_id_format)
        
        # Set up directories
        self.output_dir = Path(config.directories.output)
        self.progress_dir = Path(config.directories.progress)
        self.results_dir = Path(config.directories.results)
        self.exhibits_dir = Path(config.directories.exhibits)
        
        # Create directories if they don't exist
        for directory in [self.output_dir, self.progress_dir, self.results_dir, self.exhibits_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.proxy_manager = ProxyManager(config) if config.proxy.enabled else None
        self.header_generator = HeaderGenerator(config)
        
        # Set up session
        self.session = None
        self.download_pdfs = config.download.pdfs
        self.worker_timeout = config.download.worker_timeout
        
        # Initialize state
        self.downloaded_links = self._load_downloaded_links()
        self.files_to_process = []
    
    async def cleanup(self) -> None:
        """Clean up resources."""
        if self.session:
            await self.session.close()
    
    async def __aenter__(self):
        """Enter async context."""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit async context."""
        await self.cleanup()
    
    def __del__(self):
        """Clean up on deletion."""
        if self.session and not self.session.closed:
            asyncio.create_task(self.cleanup())
    
    def setup_batch_logging(self) -> None:
        """Set up batch-specific logging."""
        log_file = self.output_dir / f"batch_{self.batch_id}.log"
        self.logger = setup_logging(self.config.logging, log_file)
    
    async def run(self) -> None:
        """Run the downloader with the current configuration."""
        try:
            # Set up logging
            self.setup_batch_logging()
            self.logger.info(f"Starting batch {self.batch_id}")
            
            # Process submissions
            await self.process_submissions()
            
            self.logger.info(f"Completed batch {self.batch_id}")
        except Exception as e:
            self.logger.error(f"Error in batch {self.batch_id}: {e}")
            raise
        finally:
            await self.cleanup()
    
    async def get_starting_point(self, idx_file: str) -> List[Submission]:
        """Get the starting point for processing based on progress.
        
        Args:
            idx_file: Path to the index file
            
        Returns:
            List of submissions to process
        """
        # Parse the index file
        submissions = await self.parse_master_idx(idx_file)
        
        # Check progress file
        progress_file = self.progress_dir / f"progress_{Path(idx_file).stem}.txt"
        if progress_file.exists():
            async with aiofiles.open(progress_file, "r") as f:
                processed_urls = set()
                async for line in f:
                    _, url = line.strip().split("\t")
                    processed_urls.add(url)
            
            # Filter out processed submissions
            submissions = [s for s in submissions if s.url not in processed_urls]
        
        return submissions
    
    async def save_progress(self, idx_file: str, last_processed: str) -> None:
        """Save progress information.
        
        Args:
            idx_file: Path to the index file
            last_processed: Last processed URL
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
        line = f"{timestamp}\t{last_processed}\n"
        
        # Save to batch-specific progress file
        batch_progress_file = self.output_dir / f"progress_{Path(idx_file).stem}.txt"
        async with aiofiles.open(batch_progress_file, "a") as f:
            await f.write(line)
        
        # Save to global progress file
        global_progress_file = self.progress_dir / f"progress_{Path(idx_file).stem}.txt"
        async with aiofiles.open(global_progress_file, "a") as f:
            await f.write(line)
        
        # Update master progress file
        master_progress_file = self.progress_dir / "master_progress.txt"
        async with aiofiles.open(master_progress_file, "a") as f:
            await f.write(f"{self.batch_id}\t{idx_file}\t{timestamp}\n")
    
    async def parse_master_idx(self, idx_file: str) -> List[Submission]:
        """Parse a master.idx file.
        
        Args:
            idx_file: Path to the index file
            
        Returns:
            List of parsed submissions
            
        Raises:
            ParsingError: If the file cannot be parsed
        """
        submissions = []
        
        try:
            async with aiofiles.open(idx_file, "r") as f:
                lines = await f.readlines()
            
            # Skip header lines
            data_lines = [line for line in lines if not line.startswith("---")]
            
            for line in data_lines:
                if not line.strip():
                    continue
                
                try:
                    cik, company_name, form_type, date_filed, filename = line.strip().split("|")
                    
                    # Extract accession number from filename
                    accession_number = filename.split("/")[-1].replace(".txt", "")
                    
                    submission = Submission(
                        cik=cik,
                        company_name=company_name,
                        form_type=form_type,
                        date_filed=date_filed,
                        submission_filename=filename,
                        accession_number=accession_number,
                        master_file=Path(idx_file),
                        url=f"{self.base_url}/Archives/{filename}"
                    )
                    
                    submissions.append(submission)
                except Exception as e:
                    self.logger.warning(f"Failed to parse line in {idx_file}: {e}")
                    continue
            
            return submissions
        except Exception as e:
            raise ParsingError(f"Failed to parse master.idx file {idx_file}: {e}")
    
    async def process_submissions(self) -> None:
        """Process all submissions."""
        for idx_file in self.files_to_process:
            submissions = await self.get_starting_point(idx_file)
            
            with tqdm(total=len(submissions), desc=f"Processing {idx_file}") as pbar:
                for submission in submissions:
                    await self._process_submission_with_progress(submission, pbar)
    
    async def _process_submission_with_progress(
        self,
        submission: Dict[str, Any],
        pbar: tqdm
    ) -> None:
        """Process a submission and update progress.
        
        Args:
            submission: Submission data
            pbar: Progress bar to update
        """
        try:
            documents = await self._process_submission(submission)
            if documents:
                # Save results
                results_file = self.results_dir / f"results_{submission['master_file'].stem}.jsonl"
                async with aiofiles.open(results_file, "a") as f:
                    for doc in documents:
                        await f.write(json.dumps(doc) + "\n")
        except Exception as e:
            self.logger.error(f"Failed to process submission {submission['url']}: {e}")
        finally:
            await self.save_progress(submission["master_file"], submission["url"])
            pbar.update(1)
    
    async def _process_submission(
        self,
        submission: Dict[str, Any]
    ) -> Optional[List[Dict[str, Any]]]:
        """Process a single submission.
        
        Args:
            submission: Submission data
            
        Returns:
            List of processed documents or None if processing failed
        """
        # Download the filing
        content = await self._make_request(submission["url"])
        if not content:
            return None
        
        # Parse the filing
        try:
            documents = await self._parse_filings(content, submission)
            return documents
        except Exception as e:
            self.logger.error(f"Failed to parse filing {submission['url']}: {e}")
            return None
    
    async def _parse_filings(
        self,
        raw_filing: str,
        submission: Dict[str, str]
    ) -> Optional[List[Dict[str, Any]]]:
        """Parse documents from a filing.
        
        Args:
            raw_filing: Raw filing content
            submission: Submission data
            
        Returns:
            List of parsed documents or None if parsing failed
        """
        if not raw_filing:
            return []
        
        documents = []
        excluded_docs = []
        
        # Split into document sections
        sections = raw_filing.split("<DOCUMENT>")
        
        for section in sections[1:]:  # Skip header section
            try:
                metadata = await self._parse_documents(section, submission)
                if not metadata:
                    continue
                
                # Check if we should download this document
                if await self._apply_document_rules(
                    metadata["document_type"],
                    metadata["document_filename"]
                ):
                    # Download the document
                    document_url = f"{self.base_url}/Archives/{submission['submission_filename']}"
                    content = await self._get_exhibit(document_url, metadata["document_filename"])
                    
                    if content:
                        doc = {
                            "document_metadata": metadata,
                            "document_content": content,
                            **{k: submission[k] for k in self.config.metadata["default_fields"]}
                        }
                        documents.append(doc)
                else:
                    excluded_docs.append({
                        "document_metadata": metadata,
                        "reason": "Document type or filename excluded by rules"
                    })
            except Exception as e:
                self.logger.error(f"Failed to parse document section: {e}")
                continue
        
        # Save excluded documents
        if excluded_docs:
            await self._save_exclusions(excluded_docs)
        
        return documents
    
    async def _get_exhibit(
        self,
        url: str,
        document_filename: Optional[str]
    ) -> Optional[Union[str, bytes]]:
        """Download an exhibit.
        
        Args:
            url: URL of the exhibit
            document_filename: Filename of the document
            
        Returns:
            Exhibit content as string or bytes, or None if download failed
        """
        if not document_filename:
            return None
        
        is_pdf = document_filename.lower().endswith(".pdf")
        
        if is_pdf and not self.download_pdfs:
            return None
        
        content = await self._make_request(url, is_binary=is_pdf)
        
        if content and is_pdf:
            # Save PDF to file
            output_file = self.exhibits_dir / f"{self.batch_id}_{document_filename}"
            async with aiofiles.open(output_file, "wb") as f:
                await f.write(content)
            return None
        
        return content
    
    async def _parse_documents(
        self,
        doc_content: str,
        submission: Dict[str, str]
    ) -> Optional[Dict[str, Any]]:
        """Parse metadata from a document section.
        
        Args:
            doc_content: Document section content
            submission: Submission data
            
        Returns:
            Document metadata or None if parsing failed
        """
        try:
            # Extract metadata fields
            type_match = doc_content.split("<TYPE>")[1].split("\n")[0].strip()
            sequence_match = doc_content.split("<SEQUENCE>")[1].split("\n")[0].strip()
            filename_match = doc_content.split("<FILENAME>")[1].split("\n")[0].strip()
            description_match = doc_content.split("<DESCRIPTION>")[1].split("\n")[0].strip()
            
            return {
                "document_type": type_match,
                "sequence": sequence_match,
                "document_filename": filename_match,
                "description": description_match,
                "title": ""  # Optional field
            }
        except Exception as e:
            self.logger.error(f"Failed to parse document metadata: {e}")
            return None
    
    async def _apply_document_rules(
        self,
        doc_type: str,
        document_filename: str
    ) -> bool:
        """Apply rules to determine if a document should be downloaded.
        
        Args:
            doc_type: Document type
            document_filename: Document filename
            
        Returns:
            True if the document should be downloaded
        """
        # Check if document type matches exhibit types
        for exhibit_type in self.config.exhibit_types:
            if exhibit_type in doc_type:
                # Check if document type is excluded
                for exclusion in self.config.exhibit_exclusions:
                    if exclusion in doc_type:
                        return False
                return True
        
        return False
    
    async def _save_exclusions(self, excluded_docs: List[Dict[str, Any]]) -> None:
        """Save information about excluded documents.
        
        Args:
            excluded_docs: List of excluded documents
        """
        async with aiofiles.open("excluded_exhibits.jsonl", "a") as f:
            for doc in excluded_docs:
                await f.write(json.dumps(doc) + "\n")
    
    async def _make_request(
        self,
        url: str,
        max_retries: int = 5,
        is_binary: bool = False
    ) -> Optional[Union[str, bytes]]:
        """Make an HTTP request with retries and proxy support.
        
        Args:
            url: URL to request
            max_retries: Maximum number of retries
            is_binary: Whether to return binary content
            
        Returns:
            Response content or None if request failed
        """
        for attempt in range(max_retries):
            try:
                # Get proxy and headers
                proxy = None
                auth = None
                if self.proxy_manager:
                    try:
                        proxy, auth = self.proxy_manager.get_random_proxy()
                    except Exception as e:
                        self.logger.warning(f"Failed to get proxy: {e}")
                
                headers = self.header_generator.get_random_headers()
                
                # Make request
                async with self.session.get(
                    url,
                    proxy=proxy,
                    proxy_auth=auth,
                    headers=headers,
                    timeout=self.worker_timeout
                ) as response:
                    if response.status == 404:
                        return None
                    
                    if response.status != 200:
                        raise aiohttp.ClientError(f"Status {response.status}")
                    
                    if is_binary:
                        return await response.read()
                    return await response.text()
            
            except asyncio.TimeoutError:
                if attempt == max_retries - 1:
                    raise TimeoutException(f"Request to {url} timed out after {max_retries} attempts")
            except Exception as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"Request to {url} failed: {e}")
                    return None
            
            # Wait before retrying
            await asyncio.sleep(2 ** attempt)
        
        return None
    
    def _load_downloaded_links(self) -> dict:
        """Load previously downloaded links from progress files.
        
        Returns:
            Dictionary of downloaded links
        """
        downloaded = {}
        
        try:
            for progress_file in self.progress_dir.glob("progress_*.txt"):
                with open(progress_file) as f:
                    for line in f:
                        _, url = line.strip().split("\t")
                        downloaded[url] = True
        except Exception as e:
            self.logger.warning(f"Failed to load downloaded links: {e}")
        
        return downloaded 