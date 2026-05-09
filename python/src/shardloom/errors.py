"""Errors raised by the ShardLoom Python CLI client."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from .models import OutputEnvelope


class ShardLoomProtocolError(RuntimeError):
    """Raised when CLI output does not match the expected JSON protocol."""


class ShardLoomBinaryNotFoundError(RuntimeError):
    """Raised when an explicit ShardLoom CLI command cannot resolve a binary."""


@dataclass(slots=True)
class ShardLoomCommandError(RuntimeError):
    """Raised when an explicit ShardLoom CLI command returns an error envelope."""

    command: Sequence[str]
    returncode: int
    envelope: OutputEnvelope
    stderr: str

    def __str__(self) -> str:
        fallback = self.envelope.fallback
        return (
            f"ShardLoom command failed with status {self.envelope.status!r} "
            f"and return code {self.returncode}: {self.envelope.summary}. "
            f"fallback_attempted={fallback.attempted}"
        )
