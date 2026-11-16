"""Logging configuration utilities for the pipeline."""

import logging
import sys
from pathlib import Path


def setup_logging(
    log_file: Path | str | None,
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
    log_file_mode: str = "a",
) -> logging.Logger:
    """Configure logging to both console and file.

    Args:
        log_file: Path to the log file
        console_level: Logging level for console output
        file_level: Logging level for file output
        log_file_mode: File open mode for the log file. Use "a" to append
            (default) or "w" to overwrite at the start of each run.

    Returns:
        Configured logger instance
    """
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Replace earlier stream/file handlers (FileHandler is a StreamHandler); keep pytest's caplog
    for h in [
        h
        for h in root_logger.handlers
        if isinstance(h, logging.StreamHandler) and "LogCapture" not in type(h).__name__
    ]:
        root_logger.removeHandler(h)

    # Console handler with Unicode error handling for Windows
    stdout_stream = (
        sys.stdout.reconfigure(errors="replace")  # pyright: ignore[reportAttributeAccessIssue]
        if hasattr(sys.stdout, "reconfigure")
        else sys.stdout
    )
    console_handler = logging.StreamHandler(stream=stdout_stream)
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler
    if log_file is not None:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file, mode=log_file_mode, encoding="utf-8")
        file_handler.setLevel(file_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    return root_logger
