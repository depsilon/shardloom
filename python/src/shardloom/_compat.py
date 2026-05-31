"""Compatibility helpers for local ShardLoom tooling."""

from __future__ import annotations

import sys
from dataclasses import dataclass as _dataclass
from typing import Any


def dataclass(_cls: type[Any] | None = None, **kwargs: Any) -> Any:
    """Wrap `dataclasses.dataclass` while tolerating Python 3.9 local tooling.

    The packaged client advertises Python >=3.10. Repo-local release/readiness
    scripts are often invoked with macOS `python3`, which can still be 3.9 and
    lacks the `slots=` keyword. Dropping that keyword on 3.9 preserves script
    importability without changing the supported package metadata.
    """

    if sys.version_info < (3, 10):
        kwargs.pop("slots", None)
    if _cls is None:
        return _dataclass(**kwargs)
    return _dataclass(_cls, **kwargs)
