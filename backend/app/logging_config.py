from __future__ import annotations

import logging
from pathlib import Path

_CONFIGURED = False
_CONFIGURED_LOG_FILE_PATH: Path | None = None


def _log_file_path() -> Path:
    log_dir = Path(__file__).resolve().parents[1] / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / "backend-debug.log"


def configure_logging() -> Path:
    global _CONFIGURED
    global _CONFIGURED_LOG_FILE_PATH

    log_file_path = _log_file_path()
    if _CONFIGURED:
        return _CONFIGURED_LOG_FILE_PATH or log_file_path

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.set_name("listenlabs_console")
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(message)s"))

    file_handler = logging.FileHandler(log_file_path, encoding="utf-8", delay=True)
    file_handler.set_name("listenlabs_file")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )

    existing_handler_names = {handler.get_name() for handler in root_logger.handlers}
    if "listenlabs_console" not in existing_handler_names:
        root_logger.addHandler(console_handler)
    if "listenlabs_file" not in existing_handler_names:
        root_logger.addHandler(file_handler)

    sync_file_logger = logging.getLogger("listenlabs.sync.file")
    sync_file_logger.setLevel(logging.DEBUG)
    sync_file_logger.propagate = False
    sync_file_handler_names = {handler.get_name() for handler in sync_file_logger.handlers}
    root_file_handler = next(
        (handler for handler in root_logger.handlers if handler.get_name() == "listenlabs_file"),
        None,
    )
    if root_file_handler is not None and "listenlabs_file" not in sync_file_handler_names:
        sync_file_logger.addHandler(root_file_handler)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    _CONFIGURED = True
    _CONFIGURED_LOG_FILE_PATH = log_file_path

    return log_file_path


def reset_logging() -> None:
    global _CONFIGURED
    global _CONFIGURED_LOG_FILE_PATH

    root_logger = logging.getLogger()
    owned_handlers = [
        handler
        for handler in root_logger.handlers
        if handler.get_name() in {"listenlabs_console", "listenlabs_file"}
    ]
    for handler in owned_handlers:
        root_logger.removeHandler(handler)
        handler.close()

    sync_file_logger = logging.getLogger("listenlabs.sync.file")
    for handler in list(sync_file_logger.handlers):
        sync_file_logger.removeHandler(handler)
        if handler.get_name() != "listenlabs_file":
            handler.close()
    sync_file_logger.propagate = False
    sync_file_logger.setLevel(logging.NOTSET)

    _CONFIGURED = False
    _CONFIGURED_LOG_FILE_PATH = None
