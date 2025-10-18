from __future__ import annotations
from typing import Optional

class AppError(Exception):
    def __init__(
        self,
        message: str,
        *,
        error_code: str,
        hint: Optional[str] = None,
        actionable_next_steps: Optional[str] = None,
        root_cause: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.hint = hint
        self.actionable_next_steps = actionable_next_steps
        self.root_cause = root_cause

class ConfigError(AppError): ...
class EnvError(AppError): ...
class ConnectivityError(AppError): ...
class SchemaError(AppError): ...
class SerializationError(AppError): ...
class DeserializationError(AppError): ...
class DataError(AppError): ...
class Neo4jTxError(AppError): ...
