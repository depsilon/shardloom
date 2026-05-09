"""Thin Python client for the ShardLoom CLI JSON protocol.

The package does not provide a native binding or fallback execution engine.
It invokes explicit ShardLoom CLI commands and parses their JSON envelopes.
"""

from .client import ShardLoomClient
from .errors import ShardLoomCommandError, ShardLoomProtocolError
from .models import Diagnostic, FieldEntry, FallbackStatus, OutputEnvelope

__all__ = [
    "Diagnostic",
    "FallbackStatus",
    "FieldEntry",
    "OutputEnvelope",
    "ShardLoomClient",
    "ShardLoomCommandError",
    "ShardLoomProtocolError",
]
