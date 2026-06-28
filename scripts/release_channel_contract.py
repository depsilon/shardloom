#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Shared selected-channel contract for ShardLoom technical-preview publication.

This file owns the already-published package-channel proof version. The current
source/package-prep version remains rooted in Cargo.toml and may be ahead of
this contract while a patch release is being prepared.
"""

from __future__ import annotations

from typing import Any


SELECTED_V0_1_0_RELEASE_CHANNEL_IDS = [
    "github_prerelease",
    "testpypi",
    "pypi",
    "homebrew_tap",
]

# The JSON field names still include v0_1_0 for schema compatibility. The
# selected release value itself is the current proof-backed package version.
SELECTED_PACKAGE_RELEASE_VERSION = "0.2.1"
SELECTED_PACKAGE_RELEASE_TAG = f"v{SELECTED_PACKAGE_RELEASE_VERSION}"
SELECTED_PACKAGE_CHANNEL_STATUS_MARKER = (
    f"published_v{SELECTED_PACKAGE_RELEASE_VERSION}_selected_channels"
)
SELECTED_PACKAGE_INSTALL_SPEC = f"shardloom=={SELECTED_PACKAGE_RELEASE_VERSION}"
SELECTED_PACKAGE_GITHUB_DOWNLOAD_COMMAND_MARKER = (
    f"gh release download {SELECTED_PACKAGE_RELEASE_TAG}"
)

SELECTED_V0_1_0_FEASIBILITY_STATUS = "included_channel_proof_passed"
SELECTED_V0_1_0_PUBLICATION_AUTHORIZATION_STATUS = "approved_channel_proof_passed"
SELECTED_V0_1_0_INSTALL_ACCESS_BOUNDARY = (
    f"selected {SELECTED_PACKAGE_RELEASE_TAG} GitHub/TestPyPI/PyPI/Homebrew install access"
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
