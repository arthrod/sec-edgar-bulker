"""Command-line interface for SEC EDGAR Bulker."""

import asyncio
import click
import yaml
from pathlib import Path
from typing import Optional
import sys
import logging
from datetime import datetime

from ..core.models import Config
from ..core.downloader import EdgarDownloader
from ..core.utils import setup_logging, validate_config

# Old implementation commented out
# async def _run_downloader(config: Config) -> None:
#     """Run the downloader with the given configuration."""
#     downloader = EdgarDownloader(config)
#     await downloader.run()

# New implementation with context manager for proper cleanup
async def _run_downloader(config: Config) -> None:
    """Run the downloader with the given configuration."""
    async with EdgarDownloader(config) as downloader:
        await downloader.run()

# Old CLI implementation commented out
# @click.command(help="SEC EDGAR Bulker CLI - Download SEC EDGAR filings in bulk")
# @click.argument('config-file', type=click.Path(exists=True, dir_okay=False, resolve_path=True), metavar='CONFIG_FILE')
# @click.option('--download-mode', type=click.Choice(['sequential', 'concurrent']), help='Override download mode')
# @click.option('--years', help='Override years (comma-separated)')
# @click.option('--quarters', help='Override quarters (comma-separated)')
# def main(config_file: str, download_mode: Optional[str], years: Optional[str], quarters: Optional[str]) -> None:
#     """SEC EDGAR Bulker CLI.
#     
#     CONFIG_FILE: Path to the YAML configuration file
#     """
#     logger = logging.getLogger("sec_edgar_bulker")
#     
#     try:
#         # Load and validate config file
#         try:
#             with open(config_file, encoding='utf-8') as f:
#                 config_data = yaml.safe_load(f)
#                 if not config_data:
#                     raise click.UsageError("Empty config file")
#         except FileNotFoundError:
#             raise click.UsageError(f"Config file '{config_file}' does not exist")
#         except yaml.YAMLError as e:
#             raise click.UsageError(f"Invalid YAML format: {str(e)}")
#         except Exception as e:
#             raise click.UsageError(f"Failed to read config file: {str(e)}")
#             
#         # Create config object
#         try:
#             config = Config(**config_data)
#         except Exception as e:
#             raise click.UsageError(f"Invalid config format: {str(e)}")
#             
#         # Apply CLI overrides
#         if download_mode:
#             if download_mode not in ['sequential', 'concurrent']:
#                 raise click.BadParameter(f"Invalid value for '--download-mode': {download_mode}")
#             config.download.mode = download_mode
#             
#         if years:
#             try:
#                 config.years = [int(y.strip()) for y in years.split(',')]
#             except ValueError:
#                 raise click.BadParameter("Invalid years format - must be comma-separated integers", param_hint='--years')
#                 
#         if quarters:
#             try:
#                 quarters_list = [int(q.strip()) for q in quarters.split(',')]
#                 if not all(1 <= q <= 4 for q in quarters_list):
#                     raise click.BadParameter("Quarters must be between 1 and 4", param_hint='--quarters')
#                 config.quarters = quarters_list
#             except ValueError:
#                 raise click.BadParameter("Invalid quarters format - must be comma-separated integers between 1 and 4", param_hint='--quarters')
#                 
#         # Validate final config
#         try:
#             validate_config(config)
#         except Exception as e:
#             raise click.UsageError(f"Invalid config: {str(e)}")
#             
#         # Create directories
#         for dir_path in config.directories.dict().values():
#             try:
#                 Path(dir_path).mkdir(parents=True, exist_ok=True)
#             except Exception as e:
#                 raise click.UsageError(f"Failed to create directory {dir_path}: {str(e)}")
#                 
#         # Setup logging
#         try:
#             logger = setup_logging(config)
#         except Exception as e:
#             raise click.UsageError(f"Failed to setup logging: {str(e)}")
#             
#         # Run downloader
#         try:
#             asyncio.run(_run_downloader(config))
#         except Exception as e:
#             logger.error(f"Download failed: {str(e)}")
#             raise click.UsageError(f"Download failed: {str(e)}")
#             
#     except (click.UsageError, click.BadParameter) as e:
#         logger.error(str(e))
#         click.echo(f"Error: {str(e)}", err=True)
#         sys.exit(1)
#     except Exception as e:
#         logger.error(f"Unexpected error: {str(e)}")
#         click.echo(f"Unexpected error: {str(e)}", err=True)
#         sys.exit(1)

# New CLI implementation with improved error handling and validation
@click.command(help="SEC EDGAR Bulker CLI - Download SEC EDGAR filings in bulk")
@click.argument('config_file', type=click.Path(exists=True, dir_okay=False, resolve_path=True))
@click.option('--download-mode', type=click.Choice(['sequential', 'concurrent']), help='Override download mode')
@click.option('--years', help='Override years (comma-separated)')
@click.option('--quarters', help='Override quarters (comma-separated)')
def main(config_file: str, download_mode: Optional[str], years: Optional[str], quarters: Optional[str]) -> None:
    """SEC EDGAR Bulker CLI.
    
    Args:
        config_file: Path to the YAML configuration file
        download_mode: Optional download mode override
        years: Optional years override (comma-separated)
        quarters: Optional quarters override (comma-separated)
    """
    # Set up basic logging first
    logger = logging.getLogger("sec_edgar_bulker")
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.addHandler(console_handler)
    logger.setLevel(logging.INFO)
    
    success = False  # Track success for proper exit code
    
    try:
        # Load and validate config file
        try:
            # Check if config file exists
            config_path = Path(config_file)
            if not config_path.exists():
                raise click.UsageError(f"Config file '{config_file}' does not exist")
                
            # Load config file
            with open(config_path, encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
                
            # Check for empty config
            if not config_data:
                raise click.UsageError("Empty config file")
                
        except FileNotFoundError:
            # This should never happen due to click.Path(exists=True)
            raise click.UsageError(f"Config file '{config_file}' does not exist")
        except yaml.YAMLError as e:
            raise click.UsageError(f"Invalid YAML format: {str(e)}")
        except Exception as e:
            raise click.UsageError(f"Failed to read config file: {str(e)}")
            
        # Create config object
        try:
            config = Config(**config_data)
        except Exception as e:
            raise click.UsageError(f"Invalid config format: {str(e)}")
            
        # Apply CLI overrides
        if download_mode:
            # Validate download mode
            if download_mode not in ['sequential', 'concurrent']:
                raise click.BadParameter(
                    f"Invalid value for '--download-mode': {download_mode}. "
                    "Must be one of: sequential, concurrent"
                )
            config.download.mode = download_mode
            
        if years:
            try:
                year_list = [int(y.strip()) for y in years.split(',')]
                # Validate years
                current_year = datetime.now().year
                if not all(1994 <= y <= current_year for y in year_list):
                    raise click.BadParameter(
                        f"Years must be between 1994 and {current_year}",
                        param_hint='--years'
                    )
                config.years = year_list
            except ValueError:
                raise click.BadParameter(
                    "Invalid years format - must be comma-separated integers",
                    param_hint='--years'
                )
                
        if quarters:
            try:
                quarters_list = [int(q.strip()) for q in quarters.split(',')]
                if not all(1 <= q <= 4 for q in quarters_list):
                    raise click.BadParameter(
                        "Quarters must be between 1 and 4",
                        param_hint='--quarters'
                    )
                config.quarters = quarters_list
            except ValueError:
                raise click.BadParameter(
                    "Invalid quarters format - must be comma-separated integers between 1 and 4",
                    param_hint='--quarters'
                )
                
        # Validate final config
        try:
            validate_config(config)
        except Exception as e:
            raise click.UsageError(f"Invalid config: {str(e)}")
            
        # Create directories
        for dir_path in config.directories.dict().values():
            try:
                Path(dir_path).mkdir(parents=True, exist_ok=True)
            except Exception as e:
                raise click.UsageError(f"Failed to create directory {dir_path}: {str(e)}")
                
        # Setup logging
        try:
            logger = setup_logging(config.logging)
        except Exception as e:
            raise click.UsageError(f"Failed to setup logging: {str(e)}")
            
        # Run downloader
        try:
            asyncio.run(_run_downloader(config))
            success = True  # Mark success before exiting
        except Exception as e:
            logger.error(f"Download failed: {str(e)}")
            raise click.UsageError(f"Download failed: {str(e)}")
            
    except click.UsageError as e:
        # Handle both UsageError and BadParameter (which is a subclass of UsageError)
        logger.error(str(e))
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(2)  # Exit code 2 for usage/parameter errors
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        click.echo(f"Error: Unexpected error occurred - {str(e)}", err=True)
        sys.exit(1)  # Exit code 1 for other errors
        
    # Only exit with 0 if everything succeeded
    if success:
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main() 