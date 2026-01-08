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

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Remove only our own StreamHandler and FileHandler instances
    # Preserve pytest's caplog handler (LogCaptureHandler) and other special handlers
    handlers_to_remove = []
    for h in root_logger.handlers:
        handler_class_name = h.__class__.__name__

        # Skip pytest's LogCaptureHandler and LogCaptureFixture
        if "LogCapture" in handler_class_name:
            continue

        # Remove our StreamHandlers and FileHandlers
        if isinstance(h, (logging.FileHandler, logging.StreamHandler)):
            handlers_to_remove.append(h)

    for h in handlers_to_remove:
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
        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setLevel(file_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    return root_logger
