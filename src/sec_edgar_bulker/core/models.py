"""Pydantic models for SEC EDGAR Bulker."""

from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, Field, field_validator, model_validator, ValidationInfo
from datetime import datetime
from pathlib import Path

IndexType = Literal["master"]

class ProxySettings(BaseModel):
    """Settings for proxy configuration.
    
    Attributes:
        enabled: Whether to use proxies.
        file: Path to proxy list file.
        mode: Proxy selection mode (random or lru).
        required: Whether proxies are required.
        timeout: Proxy timeout in seconds.
        usage_limit: Maximum uses per proxy.
    """
    enabled: bool = False
    file: str = "proxies.txt"
    mode: str = Field("random", pattern="^(random|lru)$")
    required: bool = False
    timeout: int = Field(600, ge=0)
    usage_limit: int = Field(5, gt=0)

class DownloadSettings(BaseModel):
    """Settings for download configuration.
    
    Attributes:
        pdfs: Whether to download PDFs.
        metadata_only: Whether to only download metadata.
        max_workers: Maximum concurrent workers.
        worker_timeout: Worker timeout in seconds.
        resume_support: Whether to support resuming downloads.
        mode: Download mode (document, exhibit, or both).
    """
    pdfs: bool = True
    metadata_only: bool = False
    max_workers: int = Field(50, gt=0)
    worker_timeout: int = Field(4000, gt=0)
    resume_support: bool = True
    mode: str = Field("both", pattern="^(document|exhibit|both)$")

class ProcessingSettings(BaseModel):
    """Settings for processing configuration.
    
    Attributes:
        batch_id_format: Format string for batch IDs.
        cache_downloads: Whether to cache downloads.
        cache_by_year: Whether to cache by year.
    """
    batch_id_format: str = "%Y%m%d_%H%M%S"
    cache_downloads: bool = True
    cache_by_year: bool = True

class DefaultHeaderPatterns(BaseModel):
    """Default patterns for header parsing.
    
    Attributes:
        accession_number: Pattern for accession number.
        company_name: Pattern for company name.
        cik: Pattern for CIK.
    """
    accession_number: str = r"ACCESSION NUMBER:\s*(\S+)"
    company_name: str = r"COMPANY CONFORMED NAME:\s*(.+?)\n"
    cik: str = r"CENTRAL INDEX KEY:\s*(\d+)"

class CustomHeaderSection(BaseModel):
    """Custom section for header parsing.
    
    Attributes:
        name: Section name.
        start: Start pattern.
        end: End pattern.
        fields: List of field patterns.
    """
    name: str
    start: str
    end: str
    fields: List[Dict[str, str]]

class CustomHeaderParsing(BaseModel):
    """Custom parsing configuration for headers.
    
    Attributes:
        sections: List of custom header sections.
    """
    sections: List[CustomHeaderSection]

class HeaderParsingSettings(BaseModel):
    """Settings for header parsing.
    
    Attributes:
        use_config: Whether to use custom config.
        default: Default header patterns.
        custom: Optional custom header parsing.
    """
    use_config: bool = False
    default: DefaultHeaderPatterns = Field(default_factory=DefaultHeaderPatterns)
    custom: Optional[CustomHeaderParsing] = None

class StaticHeaders(BaseModel):
    """Static headers configuration.
    
    Attributes:
        User_Agent: User agent string.
        Accept_Encoding: Accepted encodings.
        Host: Host header.
        Connection: Connection type.
        Accept: Accepted content types.
        Accept_Language: Accepted languages.
    """
    User_Agent: str
    Accept_Encoding: str = "gzip, deflate"
    Host: str = "www.sec.gov"
    Connection: str = "keep-alive"
    Accept: str = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    Accept_Language: str = "en-US,en;q=0.5"

    def to_dict(self) -> Dict[str, str]:
        """Convert static headers to dictionary with proper HTTP header format.
        
        Returns:
            Dict[str, str]: Dictionary of HTTP headers.
        """
        return {
            "User-Agent": self.User_Agent,
            "Accept-Encoding": self.Accept_Encoding,
            "Host": self.Host,
            "Connection": self.Connection,
            "Accept": self.Accept,
            "Accept-Language": self.Accept_Language
        }

class RandomHeaderSettings(BaseModel):
    """Settings for random header generation.
    
    Attributes:
        email_domains: List of email domains to use.
        navigator: Browser navigator string.
    """
    email_domains: List[str]
    navigator: str

class HeaderGeneratorSettings(BaseModel):
    """Settings for header generation.
    
    Attributes:
        use_generator: Whether to use random generation.
        settings: Static header settings.
        random_settings: Optional random header settings.
    """
    use_generator: bool = True
    settings: StaticHeaders
    random_settings: Optional[RandomHeaderSettings] = None

class FilingHeaderPatterns(BaseModel):
    """Patterns for filing header parsing.
    
    Attributes:
        sec_document: Pattern for SEC document.
        filing_form_type: Pattern for form type.
        conformed_submission_type: Pattern for submission type.
        standard_industrial_classification: Pattern for SIC.
        acceptance_datetime: Pattern for acceptance datetime.
        public_document_count: Pattern for document count.
        conformed_period_of_report: Pattern for report period.
        filed_as_of_date: Pattern for filing date.
    """
    sec_document: str
    filing_form_type: str
    conformed_submission_type: str
    standard_industrial_classification: str
    acceptance_datetime: str
    public_document_count: str
    conformed_period_of_report: str
    filed_as_of_date: str

class FilingHeaderSettings(BaseModel):
    """Settings for filing header parsing.
    
    Attributes:
        use_config: Whether to use custom config.
        patterns: Filing header patterns.
    """
    use_config: bool = False
    patterns: FilingHeaderPatterns

class DocumentStructure(BaseModel):
    """Structure for document parsing."""
    pass

class FilingProcessingSettings(BaseModel):
    """Settings for filing processing.
    
    Attributes:
        parse_filings: Whether to parse filings.
        document_format: Document format settings.
    """
    parse_filings: bool = True
    document_format: Dict[str, List[str]]

class DocumentOutputFormat(BaseModel):
    """Format for document output.
    
    Attributes:
        main_keys: Main document keys.
        metadata_keys: Metadata keys.
    """
    main_keys: List[str]
    metadata_keys: List[str]

class DocumentSettings(BaseModel):
    """Settings for document handling.
    
    Attributes:
        output_format: Document output format.
        structure: Optional document structure.
    """
    output_format: DocumentOutputFormat
    structure: Optional[DocumentStructure] = None

class LoggingSettings(BaseModel):
    """Settings for logging configuration.
    
    Attributes:
        level: Log level.
        format: Log format string.
        file_enabled: Whether to log to file.
        file: Optional log file path.
        max_size: Maximum log file size.
        backup_count: Number of backup files.
    """
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_enabled: bool = False
    file: Optional[str] = None
    max_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: str) -> str:
        """Validate log level.
        
        Args:
            v: Log level string.
            
        Returns:
            str: Validated log level.
            
        Raises:
            ValueError: If log level is invalid.
        """
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v.upper()

    @model_validator(mode="after")
    def validate_file_settings(self, info: ValidationInfo) -> "LoggingSettings":
        """Validate file settings.
        
        Args:
            info: Validation info.
            
        Returns:
            LoggingSettings: Validated settings.
        """
        if self.file_enabled and not self.file:
            self.file = "logs/sec_edgar_bulker.log"  # Set default log file
        return self

class DirectorySettings(BaseModel):
    """Settings for directory paths.
    
    Attributes:
        output: Output directory.
        exhibits: Exhibits directory.
        filings: Filings directory.
        logs: Logs directory.
        master_idx: Master index directory.
        progress: Progress directory.
        results: Results directory.
    """
    output: str = "output"
    exhibits: str = "exhibits"
    filings: str = "filings"
    logs: str = "logs"
    master_idx: str = "idx_files"
    progress: str = "progress"
    results: str = "results"

class FilteringSettings(BaseModel):
    """Settings for filtering.
    
    Attributes:
        company_filter_enabled: Whether to filter by company.
        date_range_filter_enabled: Whether to filter by date.
        ciks: Optional list of CIKs.
        company_names: Optional list of company names.
        date_range_start: Optional start date.
        date_range_end: Optional end date.
    """
    company_filter_enabled: bool = False
    date_range_filter_enabled: bool = False
    ciks: Optional[List[str]] = None
    company_names: Optional[List[str]] = None
    date_range_start: Optional[str] = None
    date_range_end: Optional[str] = None

    @model_validator(mode="after")
    def validate_date_range(self, info: ValidationInfo) -> "FilteringSettings":
        """Validate date range settings.
        
        Args:
            info: Validation info.
            
        Returns:
            FilteringSettings: Validated settings.
            
        Raises:
            ValueError: If date range is invalid.
        """
        if self.date_range_filter_enabled:
            if not (self.date_range_start and self.date_range_end):
                raise ValueError("Both date_range_start and date_range_end must be specified")
            try:
                datetime.strptime(self.date_range_start, "%Y-%m-%d")
                datetime.strptime(self.date_range_end, "%Y-%m-%d")
            except ValueError as e:
                raise ValueError(f"Invalid date format: {e}")
        return self

class Config(BaseModel):
    """Main configuration model.
    
    Attributes:
        years: List of years to process.
        quarters: List of quarters to process.
        directories: Directory settings.
        proxy: Proxy settings.
        download: Download settings.
        processing: Processing settings.
        exhibit_types: List of exhibit types.
        exhibit_exclusions: List of excluded exhibits.
        metadata: Metadata settings.
        header_parsing: Header parsing settings.
        header_generator: Header generator settings.
        filing_header: Filing header settings.
        logging: Logging settings.
        filtering: Filtering settings.
    """
    years: List[int] = Field(default_factory=list, min_length=1)
    quarters: List[int] = Field(default_factory=list, min_length=1)
    directories: DirectorySettings = Field(default_factory=DirectorySettings)
    proxy: ProxySettings = Field(default_factory=lambda: ProxySettings(
        mode="random",
        timeout=600,
        usage_limit=5
    ))
    download: DownloadSettings = Field(default_factory=lambda: DownloadSettings(
        max_workers=50,
        worker_timeout=4000,
        mode="both"
    ))
    processing: ProcessingSettings = Field(default_factory=ProcessingSettings)
    exhibit_types: List[str] = ["EX-10"]
    exhibit_exclusions: List[str] = []
    metadata: Dict[str, List[str]]
    header_parsing: HeaderParsingSettings = Field(default_factory=HeaderParsingSettings)
    header_generator: HeaderGeneratorSettings
    filing_header: FilingHeaderSettings
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    filtering: FilteringSettings = Field(default_factory=FilteringSettings)

    @field_validator("quarters")
    @classmethod
    def validate_quarters(cls, v: List[int]) -> List[int]:
        """Validate quarter values.
        
        Args:
            v: List of quarters.
            
        Returns:
            List[int]: Validated quarters.
            
        Raises:
            ValueError: If quarters are invalid.
        """
        if not all(1 <= q <= 4 for q in v):
            raise ValueError("Quarters must be between 1 and 4")
        return v

    @field_validator("years")
    @classmethod
    def validate_years(cls, v: List[int]) -> List[int]:
        """Validate year values.
        
        Args:
            v: List of years.
            
        Returns:
            List[int]: Validated years.
            
        Raises:
            ValueError: If years are invalid.
        """
        current_year = datetime.now().year
        if not all(1994 <= y <= current_year for y in v):
            raise ValueError(f"Years must be between 1994 and {current_year}")
        return v

class Submission(BaseModel):
    """Model for a submission.
    
    Attributes:
        cik: Company CIK.
        company_name: Company name.
        form_type: Form type.
        date_filed: Filing date.
        submission_filename: Submission filename.
        accession_number: Accession number.
        master_file: Master file path.
        url: Submission URL.
    """
    cik: str
    company_name: str
    form_type: str
    date_filed: str
    submission_filename: str
    accession_number: str
    master_file: Path
    url: str

class Header(BaseModel):
    """Model for a header.
    
    Attributes:
        accession_number: Accession number.
        company_name: Company name.
        cik: Company CIK.
        form_type: Form type.
        date_filed: Filing date.
        document_count: Number of documents.
    """
    accession_number: str
    company_name: str
    cik: str
    form_type: str
    date_filed: datetime
    document_count: int

class DocumentMetadata(BaseModel):
    """Model for document metadata.
    
    Attributes:
        document_type: Document type.
        sequence: Document sequence.
        filename: Document filename.
        description: Document description.
        url: Document URL.
    """
    document_type: str
    sequence: str
    filename: str
    description: str
    url: str

class Document(BaseModel):
    """Model for a document.
    
    Attributes:
        metadata: Document metadata.
        content: Document content.
    """
    metadata: DocumentMetadata
    content: str

class ExcludedDocument(BaseModel):
    """Model for an excluded document."""
    metadata: DocumentMetadata
    reason: str

class Progress(BaseModel):
    """Model for progress tracking."""
    idx_file: str
    last_processed: str
    timestamp: datetime

class MasterProgress(BaseModel):
    """Model for master progress tracking."""
    batch_id: str
    idx_files: List[str]
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "in_progress" 