"""Logging utilities for the transpiler."""

from abc import ABC, abstractmethod
from enum import IntEnum
from typing import Any


class LogLevel(IntEnum):
    """Log levels for the transpiler logger."""

    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3


class ILoggable(ABC):
    """Interface for logging functionality."""

    @abstractmethod
    def log(self, level: LogLevel, message: str, *args: Any) -> None:
        """Log a message at the specified level."""
        ...

    def debug(self, message: str, *args: Any) -> None:
        """Log a debug message."""
        self.log(LogLevel.DEBUG, message, *args)

    def info(self, message: str, *args: Any) -> None:
        """Log an info message."""
        self.log(LogLevel.INFO, message, *args)

    def warning(self, message: str, *args: Any) -> None:
        """Log a warning message."""
        self.log(LogLevel.WARNING, message, *args)

    def error(self, message: str, *args: Any) -> None:
        """Log an error message."""
        self.log(LogLevel.ERROR, message, *args)
