from __future__ import annotations

import logging

from app.http import configure_http_client_logging


def test_configure_http_client_logging_sets_dependency_loggers_to_warning() -> None:
    logger_names = ("httpx", "httpcore")
    original_levels = {name: logging.getLogger(name).level for name in logger_names}
    try:
        for name in logger_names:
            logging.getLogger(name).setLevel(logging.INFO)
        configure_http_client_logging()
        assert all(logging.getLogger(name).level == logging.WARNING for name in logger_names)
    finally:
        for name, level in original_levels.items():
            logging.getLogger(name).setLevel(level)
