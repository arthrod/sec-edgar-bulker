"""SEC filing processor and downloader.

This module provides functionality to download and process SEC filings,
extracting relevant documents and metadata while managing rate limits
and proxy rotation.

Version: 1.0.0
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import signal
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import argparse
import aiofiles
import aiohttp
from aiohttp import ClientTimeout, BasicAuth
from yarl import URL
import pytz
from tqdm import tqdm


# Ignore SIGHUP (hangup signal)
signal.signal(signal.SIGHUP, signal.SIG_IGN)

class TimeoutException(Exception):
    """Exception raised when an operation times out."""
    pass


def timeout_handler(signum, frame):
    raise TimeoutException("API call timed out")


# Set the signal handler and a 10-minute alarm
signal.signal(signal.SIGALRM, timeout_handler)


class ProxyManager:
    def __init__(self, proxy_file: str):
        self.proxy_file = proxy_file
        self.proxies = []
        self.load_proxies()
        self.proxy_usage_count = {proxy: 0 for proxy in self.proxies}  # Initialize usage count

    def load_proxies(self):
        """Load proxies from file."""
        with open(self.proxy_file, 'r') as f:
            for line in f:
                if line.strip():
                    # Format: ip:port:username:password
                    self.proxies.append(line.strip())

    def get_random_proxy(self) -> Optional[tuple[str, Optional[BasicAuth]]]:
        """Get a random proxy with usage tracking.
        
        Returns:
            Tuple of (proxy_url, auth) or None if no proxies available.
        """
        if not self.proxies:
            return None
        
        # Filter out proxies that have reached the usage limit
        available_proxies = [proxy for proxy, count in self.proxy_usage_count.items() if count < 10]
        if not available_proxies:
            self.proxy_usage_count = {proxy: 0 for proxy in self.proxies}
            available_proxies = self.proxies
        
        # Select and update proxy usage
        proxy = random.choice(available_proxies)
        self.proxy_usage_count[proxy] += 1
        
        # Format proxy settings with full authentication
        ip, port, username, password = proxy.split(':')
        full_url = URL(f'http://{username}:{password}@{ip}:{port}')
        proxy_url = str(f'http://{ip}:{port}')  # URL without auth
        proxy_auth = BasicAuth.from_url(full_url)
        return proxy_url, proxy_auth


class Header:
    def __init__(self):
        pass

    def get_fixed_headers(self) -> Dict[str, str]:
        """Return fixed headers for SEC requests.
        
        Returns:
            Dictionary containing User-Agent and other required headers.
        """
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.sec.gov'
        }

# Notes for users:
# The Header class has been updated to use fixed headers.
# Please use the get_fixed_headers method to retrieve the headers for SEC requests.
# This change enhances reliability by ensuring consistent header usage across requests.


class SECDownloader:
    def __init__(self):
        self.base_url = "http://www.sec.gov"
        self.logger = logging.getLogger(__name__)
        self.proxy_manager = ProxyManager('proxies.txt')
        self.header = Header()
        self.files_to_process = []
        self.batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Set up output directory structure
        self.output_dir = Path("output") / self.batch_id
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.progress_dir = Path('progress')
        self.progress_dir.mkdir(exist_ok=True)
        
        self.results_dir = self.output_dir / 'results'
        self.results_dir.mkdir(parents=True, exist_ok=True)

        self.exhibits_dir = Path("exhibits_download")
        self.exhibits_dir.mkdir(parents=True, exist_ok=True)

        self.download_pdfs = True
        self.worker_status = {}  # Track worker status
        self.available_workers = asyncio.Queue()  # Queue of available worker IDs
        self.sessions = {}  # Store sessions by proxy
        self.connectors = {}  # Store connectors by proxy
        self.worker_timeout = 4000  # Worker timeout in seconds
        self.downloaded_files_cache = {}  # Cache of downloaded files by year

        # Paths for downloaded links and logs
        self.downloaded_links_file = Path("downloaded_links.jsonl")
        self.downloaded_links = self._load_downloaded_links()

        # Initialize worker queue
        for i in range(400):
            self.available_workers.put_nowait(i)

        # Setup batch logging
        self.setup_batch_logging()

    # In get_starting_point, later you can do:
    # progress_file = self.progress_dir / f"progress_{Path(idx_file).stem}.txt"

    # In _parse_filings, when you know filename_stem:
    # results_file = self.results_dir / f'results_{filename_stem}.jsonl'

    # Other methods remain the same...
        

        

    async def cleanup(self) -> None:
        """Clean up all sessions and connectors."""
        for session in self.sessions.values():
            if not session.closed:
                await session.close()
        for connector in self.connectors.values():
            if not connector.closed:
                await connector.close()
        self.sessions.clear()
        self.connectors.clear()

    def __del__(self) -> None:
        """Ensure cleanup runs on deletion."""
        if self.sessions or self.connectors:
            asyncio.create_task(self.cleanup())

    def setup_batch_logging(self) -> None:
        """Setup logging for this specific batch."""
        self.stats = {
            'total_processed': 0,
            'ex10_matches': 0,
            'not_ex10_matches': 0
        }
        log_file = self.output_dir / f"batch_{self.batch_id}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        self.logger.addHandler(file_handler)

    async def get_starting_point(self, idx_file: str) -> List[Dict[str, str]]:
        """Compare master.idx entries with processed submissions to determine pending submissions."""
        try:
            # Get processed submissions from progress file
            processed_urls = set()
            progress_file = self.progress_dir / f"progress_{Path(idx_file).stem}.txt"
            if progress_file.exists():
                async with aiofiles.open(progress_file, 'r') as f:
                    async for line in f:
                        if line.strip():
                            processed_urls.add(line.strip().split('\t')[1])
                self.logger.info(f"Found {len(processed_urls)} processed submissions")
            else:
                self.logger.info("No progress file found")

            # Get all submissions from master.idx
            try:
                all_submissions = await self.parse_master_idx(idx_file)
                self.logger.info(f"Found {len(all_submissions)} total submissions")
                
                # Filter out processed submissions
                pending_submissions = []
                for submission in all_submissions:
                    if submission['accession_number']:  # Only if we have a valid accession number
                        raw_txt_url = f"{self.base_url}/Archives/edgar/data/{submission['cik']}/{submission['accession_number']}/{Path(submission['submission_filename']).name}"
                        self.logger.debug(f"Processing {raw_txt_url}")
                        if raw_txt_url not in processed_urls:
                            pending_submissions.append(submission)
                
                self.logger.info(f"Found {len(pending_submissions)} pending submissions")
                return pending_submissions
                
            except Exception as e:
                self.logger.error(f"No pending submissions in {idx_file}, skipping to next file. Error {e}.")
                return []

        except Exception as e:
            self.logger.error(f"Error processing {idx_file}: {str(e)}")
            return []
    
    async def save_progress(self, idx_file: str, last_processed: str):
        """Save progress to the progress file with timestamp."""
        try:
            # Get current time in EST
            est_ = pytz.timezone('US/Eastern')
            timestamp_collection = datetime.now(timezone.utc).astimezone(est_).strftime("%Y-%m-%d %H:%M:%S %Z")

            # Save to batch-specific progress file - always use .txt extension
            batch_progress = self.output_dir / f"progress_{Path(idx_file).stem}.txt"
            async with aiofiles.open(batch_progress, 'a') as f:
                await f.write(f"{timestamp_collection}\t{last_processed}\n")
                await f.flush()
            
            # Save to global progress file - always use .txt extension
            global_progress = self.progress_dir / f"progress_{Path(idx_file).stem}.txt"
            async with aiofiles.open(global_progress, 'a') as f:
                await f.write(f"{timestamp_collection}\t{last_processed}\n")
                await f.flush()
                
            # Save to master progress file
            master_progress = self.progress_dir / "master_progress.txt"
            async with aiofiles.open(master_progress, 'a') as f:
                await f.write(f"{timestamp_collection}\t{idx_file}\t{last_processed}\t{self.batch_id}\n")
                await f.flush()
                
        except Exception as e:
            self.logger.error(f"Error saving progress: {e}")
            
    async def parse_master_idx(self, idx_file: str) -> List[Dict[str, str]]:
        """Parse master.idx content into list of submissions."""
        try:
            async with aiofiles.open(idx_file, mode='r') as f:
                idx_content = await f.read()
                
            submissions = []
            lines = idx_content.split('\n')
            
            # Find the header line and start processing after the dashes
            start_idx = next((i for i, line in enumerate(lines) if 'CIK|Company Name|Form Type|Date Filed|Filename' in line), 0) +2
            if not start_idx:
                self.logger.error(f"Could not find header in {idx_file}")
                return []
                
            
            for line in lines[start_idx:]:
                if not line.strip():
                    continue
                    
                try:
                    parts = line.strip().split('|')
                    if len(parts) == 5:
                        submission_filename = parts[4].strip()
                        accession_match = re.search(r'/(\d{10}-\d{2}-\d{6})', submission_filename)
                        accession_number = accession_match.group(1).replace('-', '') if accession_match else None
                    

                        submissions.append({
                            'cik': parts[0].strip(),
                            'company_name': parts[1].strip(),
                            'form_type': parts[2].strip(),
                            'date_filed': parts[3].strip(),
                            'submission_filename': submission_filename,
                            'accession_number': accession_number,
                            'master_file': Path(idx_file)
                            })
                    
                    else:
                        self.logger.error(f"Invalid line in {idx_file}: {line}")
                        continue

                except Exception as e:
                    self.logger.error(f"Error parsing line in {idx_file}: {str(e)}")
                    continue
                    
            self.logger.debug(f"Found {len(submissions)} submissions in {idx_file}")
            return submissions

        except Exception as e:
            self.logger.error(f"Error reading/parsing master.idx file {idx_file}: {str(e)}.")
            return []

    
    async def process_submissions(self):
        """Process all master.idx files in files_to_process list with improved worker handling."""
        self.logger.debug(f"Starting batch {self.batch_id}")
        
        try:
            
            for idx_file in self.files_to_process:
                self.logger.debug(f"\n{'='*50}\nProcessing {idx_file}\n{'='*50}")
                
                start_from = await self.get_starting_point(idx_file)
                if not start_from:
                    self.logger.info(f"No pending submissions in {idx_file}, skipping to next file")
                    continue  # Skip to next file in self.files_to_process
                self.logger.info(f"Resuming from {start_from[0]}")
                try:
                    valid_submissions = start_from
                    total_submissions = len(valid_submissions)
                    
                    self.logger.info(f"\nProcessing {total_submissions:,} submissions")
                    
                    pbar = tqdm(total=total_submissions, initial=0, desc=f"Processing {idx_file}")

                    async def worker(queue):
                        while True:
                            try:
                                item = await queue.get()
                                if item is None:
                                    break
                                
                                _, submission = item
                                try:
                                    await self._process_submission(submission)
                                    self.logger.debug(f"Submission {submission['submission_filename']} completed.")
                                    pbar.update(1)
                                except Exception as e:
                                    self.logger.error(f"Error processing submission {submission['submission_filename']}: {str(e)}")
                                finally:
                                    queue.task_done()
                            except Exception as e:
                                self.logger.error(f"Worker error: {str(e)}")
                                continue

                    queue = asyncio.Queue()
                    workers = [asyncio.create_task(worker(queue)) for _ in range(400)]

                    # Feed all submissions to queue first
                    for i, submission in enumerate(valid_submissions):
                        await queue.put((i, submission))

                    # Add sentinel values to signal workers to exit
                    for _ in range(400):
                        await queue.put(None)
                    
                    # Wait for all workers to complete
                    await asyncio.gather(*workers)
                    pbar.close()
                    self.logger.info(f"Finished processing file {idx_file}.")
                    
                except Exception as e:
                    self.logger.error(f"Error processing batch in {idx_file}: {str(e)}.")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error in process_filings: {str(e)}.")
            raise
        finally:
            # Clean up all sessions and connectors
            for session in self.sessions.values():
                if not session.closed:
                    await session.close()
            for connector in self.connectors.values():
                if not connector.closed:
                    await connector.close()
            self.sessions.clear()
            self.connectors.clear()
    
                        
    async def _process_submission(self, submission: Dict[str, str]) -> Optional[List[Dict[str, Any]]]:
        """Process a single submission asynchronously."""
        try:
            # Extract accession number from filename
            accession_match = re.search(r'/(\d{10}-\d{2}-\d{6})', submission['submission_filename'])
            if not accession_match:
                self.logger.debug(f"Could not extract accession number from {submission['submission_filename']}")
                return []
                
            accession_number = accession_match.group(1).replace('-', '')
            
            # Get the raw text URL using accession number
            raw_txt_url = f"{self.base_url}/Archives/edgar/data/{submission['cik']}/{accession_number}/{Path(submission['submission_filename']).name}"
            submission = {**submission, 'url': raw_txt_url}
            # Get raw content with timeout
            try:
                self.logger.debug(f"Processing {raw_txt_url}")
                filing_raw_content_ = await asyncio.wait_for(
                    self._make_request(raw_txt_url),
                    timeout=1200
                )
                filing_raw_content = str(filing_raw_content_)
                if not filing_raw_content:
                    self.logger.error(f"Failed to get raw content from {raw_txt_url}")
                    return None
            except asyncio.TimeoutError:
                self.logger.error(f"Timeout getting raw content from {raw_txt_url}")
                return None
            
            # Process exhibits with timeout
            for attempt in range(3):
                try:
                    parsed_documents = await asyncio.wait_for(
                        self._parse_filings(filing_raw_content, submission),
                        timeout=2200
                    )
                    self.logger.debug(f"Parsed documents for {raw_txt_url}")
                    if parsed_documents is None:
                        self.logger.debug(f"No parsed documents for {raw_txt_url}")
                        return None
                    await self.save_progress(submission['master_file'], raw_txt_url)
                    return parsed_documents
                except asyncio.TimeoutError:
                    if attempt == 2:
                        self.logger.error(f"Timeout processing parsed documents for {raw_txt_url}")
                        return None
                    await asyncio.sleep(1)

        except Exception as e:
            self.logger.error(f"Error processing submission: {str(e)}")
            return None

    async def _parse_filings(self, raw_filing: str, submission: Dict[str, str]) -> Optional[List[Dict[str, Any]]]:
        """Parse a filing and its documents from raw content."""
        try:
            if not raw_filing:
                return []

            
            # Level 1: Submission info (from master file)
            submission_info = {
                'cik': submission['cik'],
                'company_name': submission.get('company_name', ''),
                'form_type': submission.get('form_type', ''),
                'date_filed': submission.get('date_filed', ''),
                'master_file': str(submission.get('master_file', '')),  # Convert to string
                'submission_filename': submission.get('submission_filename', ''),
                'submission_url': submission.get('url', ''),  # Changed to use 'url' which contains the main filing URL
                'accession_number': submission.get('accession_number', ''),
            }
            self.logger.debug("Error C.")
            
            # Try original format first, then fallback to SEC-HEADER format
            sec_document = re.search(r'<(?i:SEC-DOCUMENT)>\s*(.*?)\s*(?:</[^>]*>|$)', raw_filing)
            if not sec_document:
                sec_document = re.search(r'SEC-DOCUMENT:\s*(.*?)(?:\s*$|\n)', raw_filing, re.IGNORECASE)
                if not sec_document:
                    sec_document = re.search(r'<SEC-DOCUMENT>\s*(.*?)\s*(?:$|\n)', raw_filing)

            acceptance_datetime = re.search(r'<(?i:ACCEPTANCE-DATETIME)>\s*(.*?)\s*(?:</[^>]*>|$)', raw_filing)
            if not acceptance_datetime:
                acceptance_datetime = re.search(r'ACCEPTANCE-DATETIME:\s*(.*?)(?:\s*$|\n)', raw_filing, re.IGNORECASE)
                if not acceptance_datetime:
                    acceptance_datetime = re.search(r'ACCEPTANCE-DATETIME[:>\s]*(.*?)\s*(?:$|\n)', raw_filing)

            public_document_count = re.search(r'<(?i:PUBLIC-DOCUMENT-COUNT)>\s*(.*?)\s*(?:</[^>]*>|$)', raw_filing)
            if not public_document_count:
                public_document_count = re.search(r'PUBLIC DOCUMENT COUNT:\s*(.*?)(?:\s*$|\n)', raw_filing, re.IGNORECASE)
                if not public_document_count:
                    public_document_count = re.search(r'PUBLIC DOCUMENT COUNT:\s*(.*?)\s*(?:$|\n)', raw_filing)

            company_name = re.search(r'<(?i:COMPANY-NAME)>\s*(.*?)\s*(?:</[^>]*>|$)', raw_filing)
            if not company_name:
                company_name = re.search(r'COMPANY CONFORMED NAME:\s*(.*?)(?:\s*$|\n)', raw_filing, re.IGNORECASE)

            sec_header = re.search(r'<(?i:SEC-HEADER)>\s*(.*?)\s*(?:</[^>]*>|$)', raw_filing)
            if not sec_header:
                sec_header = re.search(r'SEC-HEADER:\s*(.*?)(?:\s*$|\n)', raw_filing, re.IGNORECASE)
                if not sec_header:
                    sec_header = re.search(r'SEC-HEADER\s*(.*?)\s*(?:$|\n)', raw_filing)

            filing_date = re.search(r'<(?i:DATE)>\s*(.*?)\s*(?:</[^>]*>|$)', raw_filing)
            if not filing_date:
                filing_date = re.search(r'FILED AS OF DATE:\s*(.*?)(?:\s*$|\n)', raw_filing, re.IGNORECASE)

            filing_form_type = re.search(r'FORM TYPE:\s*(.*?)\s*(?:$|\n)', raw_filing, re.IGNORECASE)
            if not filing_form_type:
                filing_form_type = re.search(r'FORM TYPE:\s*(.*?)(?:\s*$|\n)', raw_filing, re.IGNORECASE)

            submission_type = re.search(r'SUBMISSION TYPE:\s*(.*?)\s*(?:$|\n)', raw_filing, re.IGNORECASE)
            if not submission_type:
                submission_type = re.search(r'CONFORMED SUBMISSION TYPE:\s*(.*?)(?:\s*$|\n)', raw_filing, re.IGNORECASE)

            conformed_submission_type = re.search(r'CONFORMED SUBMISSION TYPE:\s*(.*?)\s*(?:$|\n)', raw_filing, re.IGNORECASE)
            if not conformed_submission_type:
                conformed_submission_type = re.search(r'CONFORMED SUBMISSION TYPE:\s*(.*?)(?:\s*$|\n)', raw_filing, re.IGNORECASE)

            period_of_report = re.search(r'PERIOD OF REPORT:\s*(.*?)\s*(?:$|\n)', raw_filing, re.IGNORECASE)
            if not period_of_report:
                period_of_report = re.search(r'CONFORMED PERIOD OF REPORT:\s*(.*?)(?:\s*$|\n)', raw_filing, re.IGNORECASE)

            conformed_period_of_report = re.search(r'CONFORMED PERIOD OF REPORT:\s*(.*?)\s*(?:$|\n)', raw_filing, re.IGNORECASE)
            if not conformed_period_of_report:
                conformed_period_of_report = re.search(r'CONFORMED PERIOD OF REPORT:\s*(.*?)(?:\s*$|\n)', raw_filing, re.IGNORECASE)
            sec_header_complete = ''          # Initialize with empty string
            try:
                header_start = raw_filing.find('<SEC-HEADER>')
                header_end = raw_filing.find('</SEC-HEADER>')
                if header_start != -1 and header_end != -1:
                    sec_header_complete = str(raw_filing[header_start:header_end + len('</SEC-HEADER>')])
                else:
                    header_match = re.search(r'SEC-HEADER:\s*(.*?)\s*(?:$|\n)', raw_filing, re.IGNORECASE)
                    if header_match:
                        sec_header_complete = header_match.group(1).strip()
            except Exception as e:
                self.logger.debug(f"Error extracting SEC header: {e}")

            # Special handling for SIC which has two groups
            sic_match = re.search(r'STANDARD INDUSTRIAL CLASSIFICATION:\s*(.*?)\s*\[(\d+)\]', raw_filing, re.IGNORECASE)
            if sic_match and len(sic_match.groups()) >= 2:
                standard_industrial_classification = sic_match.group(1).strip()
                classification_number = sic_match.group(2).strip()
            else:
                standard_industrial_classification = ''
                classification_number = ''

            # Get accession number
            accession_match = re.search(r'/(\d{10}-\d{2}-\d{6})', submission['submission_filename'])
            if accession_match:
                accession_number = accession_match.group(1).replace('-', '')
            else:
                accession_number = ''
                self.logger.warning(f"Could not extract accession number from {submission['submission_filename']}")

            self.logger.debug("Error E.")
            # Process each regex match properly
            sec_document = sec_document.group(1).strip() if sec_document else ''
            acceptance_datetime = acceptance_datetime.group(1).strip() if acceptance_datetime else ''
            filing_form_type = filing_form_type.group(1).strip() if filing_form_type else ''
            submission_type = submission_type.group(1).strip() if submission_type else ''
            conformed_submission_type = conformed_submission_type.group(1).strip() if conformed_submission_type else ''
            period_of_report = period_of_report.group(1).strip() if period_of_report else ''
            conformed_period_of_report = conformed_period_of_report.group(1).strip() if conformed_period_of_report else ''
            public_document_count = public_document_count.group(1).strip() if public_document_count else ''
            company_name = company_name.group(1).strip() if company_name else ''
            sec_header = sec_header.group(1).strip() if sec_header else ''
            filing_date = filing_date.group(1).strip() if filing_date else ''
            
            header_info = {
                'sec_document': sec_document,
                'acceptance_datetime': acceptance_datetime,
                'filing_form_type': filing_form_type,
                'submission_type': submission_type,
                'conformed_submission_type': conformed_submission_type,
                'period_of_report': period_of_report,
                'conformed_period_of_report': conformed_period_of_report,
                'standard_industrial_classification': standard_industrial_classification.strip() if standard_industrial_classification else '',
                'classification_number': classification_number.strip() if classification_number else '',
                'accession_number': accession_number,
                'public_document_count': public_document_count,
                'company_name': company_name,
                'sec_header': sec_header,
                'filing_date': filing_date,
                'sec-header-complete': sec_header_complete
            }
            
            # Split into documents
                    # Split into documents
            try:
                documents = []
                current_pos = 0
                
                # Extract SEC header first
                header_start = raw_filing.find('<SEC-HEADER>')
                header_end = raw_filing.find('</SEC-HEADER>')
                if header_start != -1 and header_end != -1:
                    submission['sec-header-complete'] = raw_filing[header_start:header_end + len('</SEC-HEADER>')]
                
                doc_count = 0  # Add counter
                self.logger.debug("Starting to split documents...")
                
                while True:
                    doc_start = raw_filing.find('<DOCUMENT>', current_pos)
                    if doc_start == -1:
                        break
                        
                    next_start = raw_filing.find('<DOCUMENT>', doc_start + 10)
                    doc_end = raw_filing.find('</DOCUMENT>', doc_start)
                    
                    if doc_end != -1 and (next_start == -1 or doc_end < next_start):
                        documents.append(raw_filing[doc_start:doc_end + 11])
                        current_pos = doc_end + 11
                    else:
                        end_pos = next_start if next_start != -1 else len(raw_filing)
                        documents.append(raw_filing[doc_start:end_pos])
                        current_pos = end_pos
                    
                    doc_count += 1  # Increment counter
                    self.logger.debug(f"Found document {doc_count} at position {doc_start}")

                self.logger.debug(f"Total documents found: {len(documents)}")

                results = []
                excluded = []
                clean_filename = str(submission.get('master_file'))
                filename_stem = str(Path(clean_filename).stem)
                results_file = self.results_dir / f'results_{filename_stem}.jsonl'

                est_ = pytz.timezone('US/Eastern')
                timestamp_collection = datetime.now(timezone.utc).astimezone(est_).strftime("%Y-%m-%d_%H:%M:%S_%Z")

                for raw_doc in documents:
                    try:
                        document_metadata = await self._parse_documents(raw_doc, submission)
                        if not document_metadata:
                            continue
                            
                        complete_doc = {
                            'submission': submission_info,
                            'header': header_info,
                            'document_from_text': raw_doc,
                            'document_metadata': document_metadata
                        }
                        type_for_path = str(complete_doc['document_metadata']['document_type'])
                        doc_for_path = str(complete_doc['document_metadata']['document_filename'])

                        allowed_extensions = ('.htm', '.html', '.txt')  
                        if not any(doc_for_path.endswith(ext) for ext in allowed_extensions):
                            if doc_for_path.endswith('.pdf'):
                                # Handle PDFs - download and save to exhibits_download
                                doc_url = f"{self.base_url}/Archives/edgar/data/{submission['cik']}/{accession_number}/{doc_for_path}"
                                pdf_content = await self._get_exhibit(doc_url, doc_for_path)
                                if pdf_content:
                                    pdf_path = self.exhibits_dir / doc_for_path
                                    async with aiofiles.open(pdf_path, 'wb') as f:
                                        await f.write(pdf_content)
                            else:
                                self.logger.debug(f"Skipping non-text file: {doc_for_path}")
                            continue

                        should_download = await self._apply_document_rules(type_for_path, doc_for_path)

                        if should_download:
                            doc_url = f"{self.base_url}/Archives/edgar/data/{submission['cik']}/{accession_number}/{doc_for_path}"
                            raw_document_content = await self._get_exhibit(
                                doc_url,
                                document_filename=str(complete_doc['document_metadata']['document_filename'])
                            )
                            complete_doc.update({
                                '_id': str(uuid.uuid4()),
                                'timestamp_collection': timestamp_collection,
                                'doc_url': doc_url,
                                'raw_document_content': raw_document_content
                            })
                            results.append(complete_doc)

                            async with aiofiles.open(results_file, 'a') as f:
                                await f.write(json.dumps(complete_doc) + '\n')
                                await f.flush()

                        else:
                            excluded.append({
                                '_id': str(uuid.uuid4()),
                                'timestamp_collection': timestamp_collection,
                                'submission_url': submission.get('url'),
                                'master_file': str(submission.get('master_file')),
                                'document_type': complete_doc.get('document_metadata', {}).get('document_type'),
                                'submission_filename': filename_stem,
                                'document_filename': complete_doc.get('document_metadata', {}).get('document_filename'),
                            })
                            await self._save_exclusions(excluded)

                    except Exception as e:
                        self.logger.error(f"Error extracting metadata (first): {str(e)}")
                        return results

                return results

            except Exception as e:
                self.logger.error(f"Error splitting documents: {str(e)}")
                return []

        except Exception as e:
            self.logger.error(f"Error extracting metadata (second): {str(e)}")
            return []

    async def _get_exhibit(self, url: str, document_filename: str | None) -> Optional[str]:
        """Process an exhibit and return its content."""
        try:
            self.logger.debug(f"Checking file: {document_filename}")
            # Determine if the file is a PDF
            is_pdf = document_filename.endswith('.pdf') if document_filename else False
            
            for attempt in range(3):
                try:
                    content = await self._make_request(url, is_binary=is_pdf)
                    if not content:
                        if attempt == 2:
                            return None
                        continue
                    
                    if is_pdf and isinstance(content, bytes):
                        if not self.download_pdfs:
                            self.logger.debug(f"Skipping PDF download (download_pdfs=False): {document_filename}")
                            return None
                        
                        # Verify PDF content
                        try:
                            import io
                            from PyPDF2 import PdfReader
                            pdf = PdfReader(io.BytesIO(content))
                            if len(pdf.pages) == 0:
                                self.logger.error(f"Invalid PDF content for {document_filename}")
                                return None
                        except Exception as e:
                            self.logger.error(f"Error validating PDF content for {document_filename}: {str(e)}")
                            return None

                        # Save PDF to exhibits directory
                        pdf_path = self.exhibits_dir / f"{self.batch_id}_{document_filename}"
                        async with aiofiles.open(pdf_path, 'wb') as f:
                            await f.write(content)
                            await f.flush()
                        # Return None since we don't keep binary content in memory
                        return None
                    else:
                        return str(content)
            
                except Exception as e:
                    self.logger.error(f"Error processing exhibit {url}: {str(e)}")
                    return None
        except Exception as e:
            self.logger.error(f"Error processing exhibit {url}: {str(e)}")
            return None

    async def _parse_documents(self, doc_content: str, submission: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Extract metadata and content from a single document section."""
        try:
            if not doc_content:
                return None
            
            doc_type = re.search(r'<TYPE>\s*([^<\r\n]+)', doc_content, re.IGNORECASE)
            doc_type = doc_type.group(1).strip() if doc_type else ''
            sequence = re.search(r'<SEQUENCE>\s*([^<\r\n]+)', doc_content, re.IGNORECASE)
            sequence = sequence.group(1).strip() if sequence else ''
            document_filename = re.search(r'<FILENAME>\s*([^<\r\n]+)', doc_content, re.IGNORECASE)
            document_filename = document_filename.group(1).strip() if document_filename else ''
            description = re.search(r'<DESCRIPTION>\s*([^<\r\n]+)', doc_content, re.IGNORECASE)
            if not description:
                description = re.search(r'<(?i:DESCRIPTION)>\s*(.*?)\s*(?:</[^>]*>|$)', doc_content)
            elif not description:
                description = re.search(r'DESCRIPTION[:>\s]*(.*?)(?:\s*$|\n)', doc_content, re.IGNORECASE)
            description = description.group(1).strip() if description else ''
            title = re.search(r'<TITLE>\s*([^<\r\n]+)', doc_content, re.IGNORECASE)
            title = title.group(1).strip() if title else ''
            document_metadata = {
                'document_type': doc_type,
                'sequence': sequence,
                'document_filename': document_filename,
                'description': description,
                'title': title,
            }
            
            return document_metadata
                
        except Exception as e:
            self.logger.error(f"Error extracting document metadata: {str(e)}")
            return None

    async def _apply_document_rules(self, doc_type: str, document_filename: str) -> bool:
        """
        Check if a document should be downloaded based on its type and filename.
        Returns True if the document should be downloaded.
        """
        try:
            doc_type_lower = doc_type.lower()
            filename_lower = document_filename.lower()

            # Step 1: Block any obvious non-10 exhibit numbers
            non_10_pattern = r'(?:exhibit|ex)[\s\-_\.]*(\d+)'
            number_match = re.search(non_10_pattern, doc_type_lower)
            if number_match:
                num_str = number_match.group(1)
                if not num_str.startswith('10'):
                    self.logger.debug(f"Blocked by non-10 number check: {doc_type}")
                    return False

            # Step 2: Check for standard EX-10 patterns
            standard_patterns = [
                r'(?:exhibit|ex)[\s\-_\.]*10(?:\.\d+)?(?![0-9])',  # ex-10, ex10.1, etc.
                r'10(?:\.\d+)?[\s\-_\.]*(?:exhibit|ex)',  # 10-ex, 10.1-exhibit, etc.
                r'(?:^|\D)10(?:\.\d+)?(?![0-9]).*(?:exhibit|ex)',  # Numbers before exhibit
            ]

            for pattern in standard_patterns:
                if re.search(pattern, doc_type_lower):
                    self.logger.debug(f"Matched standard pattern in doc_type: {doc_type}")
                    self.stats['ex10_matches'] += 1
                    return True

            # Step 3: Check filename as backup
            filename_patterns = [
                r'ex[\s\-_\.]*10(?:\.\d+)?(?![0-9])',
                r'10[\s\-_\.]*ex(?:\.\d+)?(?![0-9])'
            ]

            for pattern in filename_patterns:
                if re.search(pattern, filename_lower):
                    self.logger.debug(f"Matched filename pattern: {document_filename}")
                    self.stats['ex10_matches'] += 1
                    return True

            return False

        except Exception as e:
            self.logger.error(f"Error in _apply_document_rules: {str(e)}")
            return False

    async def _save_exclusions(self, excluded_docs: List[Dict[str, Any]]):
        """Save excluded documents to a separate JSONL file"""
        try:
            async with aiofiles.open('excluded_exhibits.jsonl', 'a') as f:
                for doc in excluded_docs:
                    await f.write(json.dumps(doc) + '\n')
        except Exception as e:
            self.logger.error(f"Error saving exclusions: {e}")


    async def _make_request(self, url: str, max_retries: int = 400, is_binary: bool = False) -> Optional[Union[str, bytes]]:
        attempts = 0
        last_error = None
                  
        while attempts < max_retries:
            try:
                proxy_result = self.proxy_manager.get_random_proxy()
                if not proxy_result:
                    self.logger.error("No valid proxies available")
                    return None
                
                proxy_url, proxy_auth = proxy_result
                proxy_key = proxy_url  # Use full proxy URL as key
                self.logger.debug(f"Using proxy {proxy_url} and {proxy_auth}")
                try:
                    if proxy_key not in self.sessions or self.sessions[proxy_key].closed:
                        headers = self.header.get_fixed_headers()
                        connector = aiohttp.TCPConnector(ssl=False, force_close=False, limit_per_host=100)
                        self.sessions[proxy_key] = aiohttp.ClientSession(
                            headers=headers,
                            connector=connector,
                            proxy=proxy_url,
                            proxy_auth=proxy_auth
                        )
                        self.logger.debug(f"Creating new session with proxy {proxy_url}")
                    
                    session = self.sessions[proxy_key]
                    
                    async with session.get(url, proxy=proxy_url, proxy_auth=proxy_auth, timeout=ClientTimeout(total=300)) as response:
                        if response.status == 200:
                            self.logger.debug(f"Got 200 for {url} with proxy {proxy_url}")
                            if is_binary:
                                return await response.read()
                            return await response.text()
                         
                        elif response.status == 403:
                            # Close and remove this session
                            await session.close()
                            self.sessions.pop(proxy_key, None)
                            attempts += 1
                            self.logger.warning(f"Got 403 on {url} with proxy {proxy_key} (attempt {attempts}/{max_retries})")
                            await asyncio.sleep(0.1)
                            continue
                        
                        elif response.status == 404:
                            self.logger.warning(f"File not found (404) at {url}")
                            return None

                        elif response.status == 407:
                            await session.close()
                            self.sessions.pop(proxy_key, None)
                            self.logger.warning(f"Authentication issue (407) at {url} and {proxy_key}")
                            return None
                        
                        elif response.status == 429:
                            await session.close()
                            self.sessions.pop(proxy_key, None)
                            attempts += 1
                            self.logger.warning(f"Got 429 (Too Many Requests) on {url} with proxy {proxy_key} (attempt {attempts}/{max_retries})")
                            await asyncio.sleep(1)
                            continue
                        
                        else:
                            await session.close()
                            self.sessions.pop(proxy_key, None)
                            attempts += 1
                            self.logger.error(f"Got unexpected status {response.status} for {url} with proxy {proxy_key} (attempt {attempts}/{max_retries})")
                            last_error = f"HTTP {response.status}"
                            continue

                except Exception as e:
                    if proxy_key in self.sessions:
                        await self.sessions[proxy_key].close()
                    if proxy_key in self.connectors:
                        await self.connectors[proxy_key].close()
                    self.logger.error(f"Got unexpected error {e} for {url} with proxy {proxy_key} (attempt {attempts}/{max_retries})")
                    attempts += 1
                    last_error = str(e)
                    await asyncio.sleep(0.1)
            
            except asyncio.TimeoutError:
                attempts += 1
                self.logger.error(f"Timeout error with proxy (attempt {attempts}/{max_retries})")
                last_error = "Timeout"
                await asyncio.sleep(0.1)
            
            except aiohttp.ClientError:
                attempts += 1
                self.logger.error(f"Network error (attempt {attempts}/{max_retries})")
                last_error = "Network error."
                await asyncio.sleep(0.1)
            
            except Exception as e:
                attempts += 1
                self.logger.error(f"Unexpected error (attempt {attempts}/{max_retries}): {str(e)}")
                last_error = str(e)
                await asyncio.sleep(0.1)
        
        self.logger.error(f"Failed to fetch {url} after {max_retries} attempts. Last error: {last_error}")
        return None

    def _load_downloaded_links(self) -> dict:
        """Load downloaded links from JSONL file."""
        downloaded_links = {}
        try:
            if self.downloaded_links_file.exists():
                with open(self.downloaded_links_file, 'r') as f:
                    for line in f:
                        try:
                            entry = json.loads(line.strip())
                            if isinstance(entry, dict) and 'document_url' in entry:
                                downloaded_links[entry['document_url']] = entry
                        except (json.JSONDecodeError, KeyError) as e:
                            self.logger.error(f"Error parsing line in downloaded_links.jsonl: {e}")
                            continue
        except Exception as e:
            self.logger.error(f"Error loading downloaded_links.jsonl: {e}")
        return downloaded_links

    async def _save_downloaded_link(self, url: str, metadata: dict):
        """Save a downloaded link to JSONL file."""
        est_ = pytz.timezone('US/Eastern')
        timestamp_collection = datetime.now(timezone.utc).astimezone(est_).strftime("%Y-%m-%d_%H:%M:%S_%Z")
        try:
            entry = {
                '_id': str(uuid.uuid4()),
                'url': url,
                'timestamp_collection': timestamp_collection,
                'metadata': metadata
            }
            async with aiofiles.open(self.downloaded_links_file, 'a') as f:
                await f.write(json.dumps(entry) + '\n')
            return entry
        except Exception as e:
            self.logger.error(f"Error saving to downloaded_links.jsonl: {e}")
            return None

def setup_logging():
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    est_ = pytz.timezone('US/Eastern')
    timestamp_collection = datetime.now(timezone.utc).astimezone(est_).strftime("%Y-%m-%d_%H:%M:%S_%Z")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / f"sec_download_{timestamp_collection}.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def setup_argparse():
    """Setup argument parser with defaults."""
    parser = argparse.ArgumentParser(description='SEC EDGAR EX-10 Exhibit Downloader')
    
    parser.add_argument(
        '--proxy-file',
        default='/home/arthrod/sec-new/proxies.txt',
        help='Path to proxy file containing proxies in format ip:port:username:password (default: %(default)s)'
    )
    
    parser.add_argument(
        '--start-year',
        type=int,
        default=2024,
        help='Start year for downloading (default: %(default)s)'
    )
    
    parser.add_argument(
        '--end-year',
        type=int,
        default=2024,
        help='End year for downloading (default: %(default)s)'
    )
    
    parser.add_argument(
        '--download-pdfs',
        type=bool,
        default=True,
        help='Whether to download PDF exhibits (default: %(default)s)'
    )
    
    return parser

async def main():
    parser = setup_argparse()
    args = parser.parse_args()
    
    downloader = SECDownloader()
    
    years = range(args.start_year, args.end_year + 1)
    quarters = range(1, 5)
    
    # Process year by year
    for year in years:
        # Load cache for this year only
        downloader.downloaded_files_cache = {}  # Clear previous year's cache
        jsonl_path = Path("output") / f"results-ex-10-{year}.jsonl"
        if jsonl_path.exists():
            try:
                with open(jsonl_path, 'r') as f:
                    for line in f:
                        try:
                            data = json.loads(line.strip())
                            accession = data.get('accession_number', '')
                            if not accession and 'doc_info' in data:
                                accession = data['doc_info'].get('accession_number', '')
                            
                            submission_filename = data.get('submission_filename', '')
                            if not submission_filename and 'doc_info' in data:
                                submission_filename = data['doc_info'].get('submission_filename', '')
                            submission_filename = Path(submission_filename) if submission_filename else None
                            
                            if accession and submission_filename:
                                if year not in downloader.downloaded_files_cache:
                                    downloader.downloaded_files_cache[year] = set()
                                entry = (accession.replace('-', ''), Path(submission_filename).name)
                                downloader.downloaded_files_cache[year].add(entry)
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                logger.error(f"Error reading cache file {jsonl_path}: {str(e)}")
                continue
        
        # Process all quarters for this year
        try:
            downloader.files_to_process = [f"master{q}{year}.idx" for q in quarters]
            await downloader.process_submissions()
        except Exception as e:
            logger.error(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    logger = setup_logging()
    logger.debug("Starting SEC EX-10 exhibit processor")
    asyncio.run(main())
    logger.debug("Processing complete")