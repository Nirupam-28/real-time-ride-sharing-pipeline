
import logging
import os
from pathlib import Path
from logging.handlers import RotatingFileHandler


def setup_logging(service_name: str) -> logging.Logger:
    """
    Configure console and rotating-file logging for a service.
    """

    project_root = Path(__file__).resolve().parent.parent
    log_directory = project_root / "logs"
    log_directory.mkdir(parents=True, exist_ok=True)

    log_file = log_directory / f"{service_name}.log"

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, log_level, logging.INFO)

    logger = logging.getLogger(service_name)
    logger.setLevel(level)
    logger.propagate = False

    # Avoid duplicate handlers if setup_logging is called again.
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # Display logs in the terminal.
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Persist logs to disk and rotate at 5 MB.
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
