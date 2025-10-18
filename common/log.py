from __future__ import annotations
import json
import sys
import time
import uuid
from typing import Any, Dict

LEVELS = {"DEBUG": 10, "INFO": 20, "WARN": 30, "WARNING": 30, "ERROR": 40}

class JsonLogger:
    def __init__(self, level: str = "INFO") -> None:
        self.level = LEVELS.get(level.upper(), 20)

    def _emit(self, level: str, msg: str, **kw: Any) -> None:
        rec: Dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "level": level,
            "module": kw.pop("module", "app"),
            "msg": msg,
        }
        # Nhét thêm các trường bổ sung (error_code, hint, correlation_id, extra, ...)
        rec.update({k: v for k, v in kw.items() if v is not None})
        sys.stdout.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        sys.stdout.flush()

    def debug(self, msg: str, **kw: Any) -> None:
        if self.level <= 10:
            self._emit("DEBUG", msg, **kw)

    def info(self, msg: str, **kw: Any) -> None:
        if self.level <= 20:
            self._emit("INFO", msg, **kw)

    def warning(self, msg: str, **kw: Any) -> None:
        if self.level <= 30:
            self._emit("WARNING", msg, **kw)

    def error(self, msg: str, **kw: Any) -> None:
        if self.level <= 40:
            self._emit("ERROR", msg, **kw)

def get_logger(level: str = "INFO") -> JsonLogger:
    return JsonLogger(level)

def new_correlation_id() -> str:
    return uuid.uuid4().hex
