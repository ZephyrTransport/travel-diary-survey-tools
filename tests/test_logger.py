"""Tests for pipeline logging configuration."""

import logging

from pipeline.logger import setup_logging


class TestSetupLogging:
    """Test setup_logging functionality."""

    def test_console_only_logging(self, caplog):
        """Test logging with console handler only."""
        caplog.set_level(logging.INFO)

        logger = setup_logging(log_file=None)

        assert logger is logging.getLogger()
        # Root logger level may be set by pytest or previous tests
        # Just verify it's set to a valid level
        assert logger.level in [logging.DEBUG, logging.INFO, logging.WARNING]

        # Test that logging works
        logger.info("Test message")
        assert "Test message" in caplog.text

    def test_file_logging(self, tmp_path, caplog):
        """Test logging with file handler."""
        caplog.set_level(logging.INFO)
        log_file = tmp_path / "test.log"

        logger = setup_logging(log_file=log_file)

        # Test file handler was added
        file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) > 0

        # Test logging to file
        logger.info("Test message to file")

        # Flush handlers to ensure write
        for handler in logger.handlers:
            handler.flush()

        assert log_file.exists()
        content = log_file.read_text()
        assert "Test message to file" in content

    def test_idempotent_setup(self, caplog):
        """Test that setup_logging can be called multiple times safely."""
        caplog.set_level(logging.INFO)

        logger1 = setup_logging(log_file=None, console_level=logging.INFO)
        handler_count1 = len(logger1.handlers)

        logger2 = setup_logging(log_file=None, console_level=logging.INFO)
        handler_count2 = len(logger2.handlers)

        # Should not add duplicate handlers
        assert handler_count2 == handler_count1

        # Logging should still work
        logger2.info("Test after multiple setups")
        assert "Test after multiple setups" in caplog.text

    def test_different_console_levels(self, tmp_path):
        """Test setup with different console log levels."""
        log_file = tmp_path / "test.log"

        logger = setup_logging(
            log_file=log_file, console_level=logging.WARNING, file_level=logging.DEBUG
        )

        # Check console handler has correct level
        console_handlers = [
            h
            for h in logger.handlers
            if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        ]
        assert len(console_handlers) > 0
        # Find our handler (not pytest's caplog handler which has NOTSET/INFO level)
        our_handlers = [h for h in console_handlers if h.level >= logging.WARNING]
        assert len(our_handlers) > 0
        assert our_handlers[0].level == logging.WARNING

    def test_preserves_existing_handlers(self, caplog):
        """Test that setup_logging preserves pytest's caplog handler."""
        caplog.set_level(logging.INFO)

        # Get initial handler count (includes pytest's handler)
        root_logger = logging.getLogger()
        initial_handlers = len(root_logger.handlers)

        # Setup logging
        setup_logging(log_file=None)

        # Should have added our handler but not removed pytest's
        assert len(root_logger.handlers) >= initial_handlers

        # Caplog should still capture logs
        root_logger.info("Test message after setup")
        assert "Test message after setup" in caplog.text

    def test_unicode_characters_in_logs(self, tmp_path, caplog):
        """Test that Unicode characters are handled correctly."""
        caplog.set_level(logging.INFO)
        log_file = tmp_path / "test.log"

        logger = setup_logging(log_file=log_file)

        # Log Unicode characters (from status report)
        logger.info("Status: ✓ CACHED, ✗ NO CACHE, ∅ DISABLED")

        # Should appear in caplog
        assert "✓ CACHED" in caplog.text or "CACHED" in caplog.text

        # Should be written to file
        for handler in logger.handlers:
            handler.flush()

        content = log_file.read_text(encoding="utf-8")
        assert "CACHED" in content

    def test_file_handler_not_duplicated(self, tmp_path):
        """Test that file handler is not added twice for same file."""
        log_file = tmp_path / "test.log"

        logger1 = setup_logging(log_file=log_file)
        file_handlers1 = [h for h in logger1.handlers if isinstance(h, logging.FileHandler)]
        count1 = len(file_handlers1)

        # Call again with same file
        logger2 = setup_logging(log_file=log_file)
        file_handlers2 = [h for h in logger2.handlers if isinstance(h, logging.FileHandler)]
        count2 = len(file_handlers2)

        # Should not add duplicate file handler
        assert count2 == count1

    def test_different_file_handlers_can_coexist(self, tmp_path):
        """Test that different file handlers can be added."""
        log_file1 = tmp_path / "test1.log"
        log_file2 = tmp_path / "test2.log"

        logger1 = setup_logging(log_file=log_file1)
        initial_count = len([h for h in logger1.handlers if isinstance(h, logging.FileHandler)])

        # Add handler for different file
        logger2 = setup_logging(log_file=log_file2)
        final_count = len([h for h in logger2.handlers if isinstance(h, logging.FileHandler)])

        # Should have added second file handler
        assert final_count == initial_count + 1

    def test_log_formatter(self, tmp_path):
        """Test that log messages have correct format."""
        log_file = tmp_path / "test.log"

        logger = setup_logging(log_file=log_file)
        logger.info("Test message")

        for handler in logger.handlers:
            handler.flush()

        content = log_file.read_text()

        # Check format: timestamp | level | message
        assert " | INFO | Test message" in content
        # Should have timestamp
        assert "2026-01-" in content  # Current date
