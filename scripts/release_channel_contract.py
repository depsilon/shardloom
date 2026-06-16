#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Shared release-channel contract for ShardLoom technical-preview publication."""

from __future__ import annotations

from typing import Any


SELECTED_V0_1_0_RELEASE_CHANNEL_IDS = [
    "github_prerelease",
    "testpypi",
    "pypi",
    "homebrew_tap",
]

SELECTED_V0_1_0_FEASIBILITY_STATUS = "included_channel_proof_passed"
SELECTED_V0_1_0_PUBLICATION_AUTHORIZATION_STATUS = "approved_channel_proof_passed"
SELECTED_V0_1_0_INSTALL_ACCESS_BOUNDARY = (
    "selected v0.1.0 GitHub/TestPyPI/PyPI/Homebrew install access"
)


def selected_channel_ids(matrix: dict[str, Any] | None) -> list[str]:
    """Return the selected release-channel ids from a matrix, or the canonical ids."""
    if isinstance(matrix, dict):
        ids = matrix.get("selected_v0_1_0_release_channel_ids")
        if ids == SELECTED_V0_1_0_RELEASE_CHANNEL_IDS:
            return list(ids)
    return list(SELECTED_V0_1_0_RELEASE_CHANNEL_IDS)


def channel_rows(matrix: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(matrix, dict):
        return []
    rows = matrix.get("channels", [])
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def selected_channel_rows(matrix: dict[str, Any] | None) -> list[dict[str, Any]]:
    selected = set(selected_channel_ids(matrix))
    return [row for row in channel_rows(matrix) if row.get("channel_id") in selected]


def selected_channels_ready(matrix: dict[str, Any] | None) -> bool:
    rows = selected_channel_rows(matrix)
    return len(rows) == len(SELECTED_V0_1_0_RELEASE_CHANNEL_IDS) and all(
        row.get("ready") is True for row in rows
    )


def selected_ready_channel_count(matrix: dict[str, Any] | None) -> int:
    return sum(1 for row in selected_channel_rows(matrix) if row.get("ready") is True)
