"""Tests for the CLI module."""

import pytest
import yaml
from pathlib import Path
from click.testing import CliRunner
from sec_edgar_bulker.cli.main import main
from sec_edgar_bulker.core.exceptions import ConfigError

@pytest.fixture
def cli_runner():
    """Create a Click CLI runner."""
    return CliRunner()

@pytest.fixture
def config_file(tmp_path, sample_config_dict):
    """Create a temporary config file."""
    config_path = tmp_path / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(sample_config_dict, f)
    return str(config_path)

def test_cli_help(cli_runner):
    """Test CLI help command."""
    result = cli_runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.output
    assert "--config-file" in result.output
    assert "--download-mode" in result.output
    assert "--years" in result.output
    assert "--quarters" in result.output

def test_cli_missing_config(cli_runner):
    """Test CLI with missing config file."""
    result = cli_runner.invoke(main, ["--config-file", "nonexistent.yaml"])
    assert result.exit_code != 0
    assert "Error:" in result.output
    assert "does not exist" in result.output

def test_cli_invalid_config_format(cli_runner, tmp_path):
    """Test CLI with invalid config format."""
    config_path = tmp_path / "invalid.yaml"
    config_path.write_text("invalid: yaml: content")
    
    result = cli_runner.invoke(main, ["--config-file", str(config_path)])
    assert result.exit_code != 0
    assert "Error:" in result.output

def test_cli_valid_config(cli_runner, config_file, mocker):
    """Test CLI with valid config file."""
    # Mock the async function
    mock_run = mocker.patch("sec_edgar_bulker.cli.main._run_downloader")
    
    result = cli_runner.invoke(main, ["--config-file", config_file])
    assert result.exit_code == 0
    mock_run.assert_called_once()

def test_cli_download_mode_override(cli_runner, config_file, mocker):
    """Test CLI with download mode override."""
    mock_run = mocker.patch("sec_edgar_bulker.cli.main._run_downloader")
    
    result = cli_runner.invoke(main, [
        "--config-file", config_file,
        "--download-mode", "document"
    ])
    assert result.exit_code == 0
    
    # Verify the config was updated
    mock_run.assert_called_once()
    config = mock_run.call_args[0][0]
    assert config.download.mode == "document"

def test_cli_years_override(cli_runner, config_file, mocker):
    """Test CLI with years override."""
    mock_run = mocker.patch("sec_edgar_bulker.cli.main._run_downloader")
    
    result = cli_runner.invoke(main, [
        "--config-file", config_file,
        "--years", "2022,2023"
    ])
    assert result.exit_code == 0
    
    # Verify the config was updated
    mock_run.assert_called_once()
    config = mock_run.call_args[0][0]
    assert config.years == [2022, 2023]

def test_cli_quarters_override(cli_runner, config_file, mocker):
    """Test CLI with quarters override."""
    mock_run = mocker.patch("sec_edgar_bulker.cli.main._run_downloader")
    
    result = cli_runner.invoke(main, [
        "--config-file", config_file,
        "--quarters", "1,2,3"
    ])
    assert result.exit_code == 0
    
    # Verify the config was updated
    mock_run.assert_called_once()
    config = mock_run.call_args[0][0]
    assert config.quarters == [1, 2, 3]

def test_cli_invalid_download_mode(cli_runner, config_file):
    """Test CLI with invalid download mode."""
    result = cli_runner.invoke(main, [
        "--config-file", config_file,
        "--download-mode", "invalid"
    ])
    assert result.exit_code != 0
    assert "Error:" in result.output
    assert "Invalid value for '--download-mode'" in result.output

def test_cli_invalid_years_format(cli_runner, config_file):
    """Test CLI with invalid years format."""
    result = cli_runner.invoke(main, [
        "--config-file", config_file,
        "--years", "invalid"
    ])
    assert result.exit_code != 0
    assert "Error:" in result.output

def test_cli_invalid_quarters_format(cli_runner, config_file):
    """Test CLI with invalid quarters format."""
    result = cli_runner.invoke(main, [
        "--config-file", config_file,
        "--quarters", "invalid"
    ])
    assert result.exit_code != 0
    assert "Error:" in result.output

def test_cli_invalid_quarters_range(cli_runner, config_file):
    """Test CLI with quarters out of range."""
    result = cli_runner.invoke(main, [
        "--config-file", config_file,
        "--quarters", "0,5"
    ])
    assert result.exit_code != 0
    assert "Error:" in result.output

def test_cli_directory_creation(cli_runner, config_file, tmp_path, mocker):
    """Test that required directories are created."""
    mock_run = mocker.patch("sec_edgar_bulker.cli.main._run_downloader")
    
    # Update config to use temporary directory
    with open(config_file) as f:
        config_dict = yaml.safe_load(f)
    
    for key in config_dict["directories"]:
        config_dict["directories"][key] = str(tmp_path / key)
    
    new_config = tmp_path / "new_config.yaml"
    with open(new_config, "w") as f:
        yaml.dump(config_dict, f)
    
    result = cli_runner.invoke(main, ["--config-file", str(new_config)])
    assert result.exit_code == 0
    
    # Verify directories were created
    for dir_name in config_dict["directories"].values():
        assert Path(dir_name).exists()
        assert Path(dir_name).is_dir()

def test_cli_logging_setup(cli_runner, config_file, tmp_path, mocker):
    """Test that logging is properly configured."""
    mock_setup_logging = mocker.patch("sec_edgar_bulker.core.utils.setup_logging")
    mock_run = mocker.patch("sec_edgar_bulker.cli.main._run_downloader")
    
    result = cli_runner.invoke(main, ["--config-file", config_file])
    assert result.exit_code == 0
    
    # Verify logging was configured
    mock_setup_logging.assert_called_once()
    log_config = mock_setup_logging.call_args[0][0]
    assert log_config.level == "DEBUG"
    assert log_config.file_enabled is True

@pytest.mark.asyncio
async def test_cli_run_downloader(cli_runner, config_file, mocker):
    """Test the _run_downloader function."""
    mock_download = mocker.patch("sec_edgar_bulker.core.idx_downloader.download_and_process_idx_files")
    mock_process = mocker.patch("sec_edgar_bulker.core.downloader.EdgarDownloader.process_submissions")
    
    result = cli_runner.invoke(main, ["--config-file", config_file])
    assert result.exit_code == 0
    
    # Verify downloader functions were called
    mock_download.assert_called_once()
    mock_process.assert_called_once()

def test_cli_config_validation_error(cli_runner, config_file, mocker):
    """Test CLI with config validation error."""
    mock_validate = mocker.patch(
        "sec_edgar_bulker.core.utils.validate_config",
        side_effect=ConfigError("Invalid config")
    )
    
    result = cli_runner.invoke(main, ["--config-file", config_file])
    assert result.exit_code != 0
    assert "Error: Invalid config" in result.output
    mock_validate.assert_called_once() 