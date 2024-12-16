# SEC EDGAR Bulker

A robust Python project for downloading and parsing SEC EDGAR filings with parallel processing capabilities.

## Future Project Structure
```
sec_edgar_bulker/
├── __init__.py              # Package initialization
├── cli.py                   # Command-line interface
├── config.py                # Configuration handling
├── exceptions.py            # Custom exceptions
├── core/                    # Core functionality
│   ├── __init__.py
│   └── downloader.py        # Main downloader implementation
├── models/                  # Data models
│   ├── __init__.py
│   ├── exhibit.py          # Exhibit data models
│   └── filing.py           # Filing data models
├── network/                 # Network handling
│   ├── __init__.py
│   ├── headers.py          # HTTP headers management
│   ├── proxy.py            # Proxy management
│   └── session.py          # Session management
├── parsers/                 # Filing parsers
│   ├── __init__.py
│   ├── parser.py           # Main parser implementation
│   └── saver.py            # File saving utilities
├── utils/                   # Utility functions
│   ├── __init__.py
│   ├── config_generator.py # Config file generation
│   ├── logging.py          # Logging configuration
│   ├── progress.py         # Progress tracking
│   └── validation.py       # Input validation
└── tests/                  # Test suite
    ├── __init__.py
    ├── conftest.py         # Test configuration
    ├── test_filing_parser.py
    ├── test_metadata_download.py
    ├── data/               # Test data
    │   └── proxies.txt
    ├── integration/        # Integration tests
    │   └── test_downloader.py
    ├── network/            # Network tests
    │   ├── test_headers.py
    │   └── test_session.py
    └── parsers/            # Parser tests
        ├── test_filing_header.py
        └── test_filing_processor.py
```

## Key Features and Functionality

### Data Retrieval
1. **Quarterly Bulk Downloads**: Download entire quarters of SEC filings in parallel
2. **Smart Resume**: Automatically continues from last downloaded file if interrupted
3. **Proxy Support**: Rotates through multiple proxies to avoid rate limiting
4. **Configurable Retry Logic**: Attempts up to 10 retries on network failures
5. **Progress Tracking**: Real-time progress bars using tqdm

### Filing Types Support
6. **Form Types**: Downloads various SEC forms including: (coming soon)
   - 10-K (Annual Reports)
   - 10-Q (Quarterly Reports)
   - 8-K (Current Reports)
   - 20-F (Foreign Private Issuer Reports)
   - DEF 14A (Proxy Statements)

### Processing Capabilities
7. **Parallel Processing**: Multi-threaded downloading and parsing
8. **Raw File Storage**: Saves unprocessed files for custom parsing
9. **Incremental Updates**: Only downloads new or modified filings
10. **Company Filtering**: Filter downloads by CIK or company name
11. **Date Range Filtering**: Specify custom date ranges for downloads
12. **Index File Processing**: Parses master.idx files for efficient lookups

### Error Handling
13. **Graceful Error Recovery**: Continues processing despite network issues
14. **Detailed Logging**: Comprehensive error and warning messages
15. **Network Error Handling**: Manages proxy timeouts and connection issues
16. **Corrupt File Detection**: Validates downloaded files for integrity

### Data Organization
17. **Structured Output**: Organizes files by year, quarter, and form type
18. **Metadata Extraction**: Pulls key information from filing headers
19. **CIK Directory Structure**: Maintains company-specific file organization
20. **Index Generation**: Creates searchable indices of downloaded content

### Performance Features
21. **Memory Efficient**: Streams large files instead of loading entirely
22. **Bandwidth Management**: Throttles downloads to respect SEC limits
23. **Cache Management**: Maintains local cache of frequently accessed data
24. **Resource Monitoring**: Tracks CPU and memory usage during processing

### User Interface
25. **Progress Visualization**: Shows download and processing progress
26. **Status Updates**: Displays current operation and estimated completion
27. **Error Reporting**: Clear error messages with suggested solutions
28. **Activity Logs**: Maintains detailed logs of all operations

### Integration Features
29. **API Integration**: Easy integration with data analysis pipelines
30. **Parsing Capabilities**: Exports metadata in JSONL format

## Quick Start

## Configuration

### Minimal Required Configuration
```yaml
# Required configurations
years: [2022, 2023]  # List of years to process
quarters: [1, 2, 3, 4]  # List of quarters to process
http:
  settings:
    static_headers:
      User-Agent: 'Mozilla/5.0 ... SEC-Downloader your.email@example.com'  # SEC requires your email
      Accept-Encoding: 'gzip, deflate'
      Host: 'www.sec.gov'
```

### Proxy Configuration
While proxies are not strictly required, they are recommended for large-scale downloads to avoid rate limiting. Proxies should be provided in a separate file (proxies.txt) in the format:
```
ip:port:username:password
```

Example proxy entry (one per line on proxies.txt):
```
191.96.104.139:5876:username:password
```

Configure proxy usage in config.yaml:
```yaml
proxies:
  enabled: true
  file: "proxies.txt"  # Path to proxy list file
  max_retries: 10      # Retries per proxy
```

### Output Format
The downloader saves files in JSONL format with the following structure:
```jsonl
{"_id": "11d89d83-e718-440b-bca2-d56d3731610f", "url": "https://www.sec.gov/Archives/edgar/data/1821534/000114036121038137/brhc10030255_ex99-1.htm", "sec_document": "0001140361-21-038137.txt : 20211116", "document_type": "ADD EXHB", "content_type": "text", "metadata": {"cik": "1821534", "company_name": "Exodus Movement, Inc.", "form_type": "1-U", "date_filed": "2021-11-16", "accession_number": "000114036121038137", "master_file": "master42021", "sec_document": "0001140361-21-038137.txt : 20211116", "filename": "brhc10030255_ex99-1.htm", "sequence": "2", "description": "EXHIBIT 99.1", "title": "", "filing_form_type": "1-U", "conformed_submission_type": "1-U", "standard_industrial_classification": "FINANCE SERVICES", "classification_number": "6199"}, "document_content": "<DOCUMENT>\n<TYPE>ADD EXHB\n<SEQUENCE>2\n<FILENAME>brhc10030255_ex99-1.htm\n<DESCRIPTION>EXHIBIT 99.1\n<TEXT>\n<html>\n  <head>\n    <title></title>\n    <!-- Licensed to: Broadridge\n         Document created using EDGARfilings PROfile 8.0.0.0\n         Copyright 1995 - 2021 Broadridge -->\n  </head></DOCUMENT>\n"}
```

## Advanced Configuration

### Download Settings
```yaml
download:
  pdfs: true                # Download PDF versions if available
  metadata_only: false      # Only download metadata, skip documents
  max_workers: 400         # Parallel download workers
  worker_timeout: 4000     # Seconds before worker timeout
  batch_size: 100         # Items per batch for processing
```

### Output Structure
```yaml
output:
  filings_to_disk: false   # Save raw filings to disk
  whole_filings: false     # Save entire filing as single JSON
  directories:
    output: "output"       # Main output directory
    exhibits: "exhibits_download"
    filings: "filings_download"
    logs: "logs"
```

### Document Processing
The downloader processes each document with the following structure:
```yaml
document:
  output_format:
    main_keys:             # Primary document fields
      - _id               # Unique document identifier
      - url              # SEC.gov URL
      - sec_document     # SEC document identifier
      - type             # Document type
      - document_content # Actual content
    metadata_keys:        # Metadata fields
      - cik
      - company_name
      - form_type
      - date_filed
      - accession_number
      - master_file
      - content_type
      - filename
      - sequence
      - description
      - title
```

### Metadata Extraction
Available metadata fields:

1. Default Fields (Always Extracted):
   - cik
   - company_name
   - form_type
   - date_filed
   - accession_number
   - master_file
   - sec_document
   - filename
   - sequence
   - description
   - title
   - filing_form_type
   - conformed_submission_type
   - standard_industrial_classification
   - classification_number

2. Optional Header Fields:
   ```yaml
   header:
     enabled: false
     fields:
       - acceptance_datetime
       - public_document_count
       - conformed_period_of_report
       - filed_as_of_date
       - date_as_of_change
       - sec_act
       - sec_file_number
       - film_number
   ```

3. Optional Company Data:
   ```yaml
   company:
     enabled: false
     fields:
       - irs_number
       - state_of_incorporation
       - fiscal_year_end
       - business_address
       - mail_address
   ```

### Header Parsing Configuration
The downloader supports custom header parsing patterns:

```yaml
header_parsing:
  use_config: false  # Use default parsing if false
  default:
    patterns:
      accession_number: "ACCESSION NUMBER:\\s*(\\S+)"
      company_name: "COMPANY CONFORMED NAME:\\s*(.+?)\\n"
      cik: "CENTRAL INDEX KEY:\\s*(\\d+)"

  custom:
    sections:
      - name: "IDENTIFICATION"
        start: "<SEC-HEADER>"
        end: "FILING VALUES"
        fields:
          - name: "accession_number"
            pattern: "ACCESSION NUMBER:\\s*(\\S+)"
          - name: "company_name"
            pattern: "COMPANY CONFORMED NAME:\\s*(.+?)\\n"
```

### SEC.gov Access Configuration
Required headers for SEC.gov access:
```yaml
navigation_headers:
  use_random: false  # Use static headers (recommended)
  settings:
    static_headers:
      User-Agent: 'Mozilla/5.0 ... SEC-Downloader your.email@example.com'
      Accept-Encoding: 'gzip, deflate'
      Host: 'www.sec.gov'
```

### Performance Optimization
1. Worker Configuration:
   - Adjust `max_workers` based on available CPU cores
   - Set `worker_timeout` based on network conditions
   - Tune `batch_size` for memory optimization

2. Network Settings:
   - Configure proxy timeouts (default: 300 seconds)
   - Set retry attempts (default: 10)
   - Adjust request delays to comply with SEC.gov rate limits

3. Storage Options:
   - Enable `filings_to_disk` for raw file storage
   - Use `metadata_only` for quick indexing
   - Enable `whole_filings` for complete filing preservation

### Logging and Monitoring
```yaml
logging:
  level: "INFO"
  format: "%(asctime)s - %(levelname)s - %(message)s"
  file_enabled: true
```

Monitor progress through:
- Download statistics
- Worker status
- Network errors
- Processing completion rates

## What to Expect on Screen

```
[Progress Bars]
Processing master12022.idx: 100%|██████████████| 104259/104259 [27:37<00:00, 62.88it/s]

[Information Messages]
2024-XX-XX XX:XX:XX - INFO - Found 291583 submissions in masterXXXXX.idx
2024-XX-XX XX:XX:XX - INFO - Resuming from last checkpoint

[Warning Messages]
2024-XX-XX XX:XX:XX - ERROR - Network error with proxy XXX.XXX.XXX.XXX:XXXX (attempt X/10)
2024-XX-XX XX:XX:XX - ERROR - Timeout error with proxy XXX.XXX.XXX.XXX:XXXX (attempt X/10)

[Start Messages]
2024-12-08 19:21:08,618 - INFO - No progress file found
2024-12-08 19:21:09,397 - INFO - Found 291583 submissions in master22022.idx
2024-12-08 19:21:09,400 - INFO - Found 291583 total submissions
2024-12-08 19:21:09,950 - INFO - Found 291583 pending submissions
2024-XX-XX XX:XX:XX - INFO - Resuming from {'cik': 'XXXXXXX', 'company_name': 'ACME INC', 'form_type': 'XX-X', 'date_filed': 'YYYY-MM-DD', 'filename': 'edgar/data/XXXXXXX/XXXXXXXXXX-XX-XXXXXX.txt', 'accession_number': 'XXXXXXXXXXXXX', 'master_file': 'masterXXXXX'}
```

## Processing Times
- Typical quarter processing time: ~1 hour
- Processing speed: ~60-70 items per second
- Network dependent: May vary based on proxy performance

## Installation

We recommend using [uv](https://github.com/astral-sh/uv) for fast, reliable Python package management: 

uv ```bash
pip install sec-edgar-downloader
```

## Usage

```python
from sec_edgar_downloader import SECDownloader, DownloaderConfig

# Load configuration from config.yaml
config = DownloaderConfig.from_yaml()

# Initialize downloader
downloader = SECDownloader(config)

# Download filings for a specific year range
await downloader.download_years()

# Check statistics
print(f"Statistics: {downloader.stats}")
```

## Statistics Tracking

The downloader keeps track of the following statistics:

- `total_processed`: Total number of filings processed
- `ex10_matches`: Number of filings containing EX-10 exhibits
- `not_ex10_matches`: Number of filings without EX-10 exhibits

## License

MIT License