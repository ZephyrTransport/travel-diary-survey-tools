"""Logging configuration utilities for the pipeline."""

import logging
import sys
from pathlib import Path


def setup_logging(
    log_file: Path | str | None,
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
) -> logging.Logger:
    """Configure logging to both console and file.

    Args:
        log_file: Path to the log file
        console_level: Logging level for console output
        file_level: Logging level for file output

    Returns:
        Configured logger instance
    """
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    # Console handler with Unicode error handling for Windows
    stdout_stream = (
        sys.stdout.reconfigure(errors="replace")  # pyright: ignore[reportAttributeAccessIssue]
        if hasattr(sys.stdout, "reconfigure")
        else sys.stdout
    )
    console_handler = logging.StreamHandler(stream=stdout_stream)
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)

    # Configure root logger (idempotent - preserves existing handlers like pytest's caplog)
    root_logger = logging.getLogger()

    # Check if we've already added our console handler
    has_our_console = any(
        isinstance(h, logging.StreamHandler)
        and not isinstance(h, logging.FileHandler)
        and h.level == console_level
        for h in root_logger.handlers
    )

    if not has_our_console:
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(console_handler)

    # File handler
    if log_file is not None:
        # Check if we've already added this file handler
        resolved_path = str(Path(log_file).resolve())
        has_our_file = any(
            isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", None) == resolved_path
            for h in root_logger.handlers
        )
        if not has_our_file:
            file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
            file_handler.setLevel(file_level)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)

    return root_logger
