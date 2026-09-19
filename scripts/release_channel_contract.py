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
SELECTED_PACKAGE_RELEASE_VERSION = "0.2.4"
SELECTED_PACKAGE_RELEASE_TAG = f"v{SELECTED_PACKAGE_RELEASE_VERSION}"
# Approved identities observed during the publication train. Keep these keyed
# by release so advancing the selected version cannot reuse a prior build's
# source/run binding. PyPI's source adds only the prerequisite proof documents.
PUBLISHED_REGISTRY_BUILD_IDENTITIES = {
    "0.2.4": {
        "testpypi": {
            "source_commit": "8759b16e3421153302c9034e5a00c9d80b61d3d9",
            "workflow_run_id": 34747808607,
        },
        "pypi": {
            "source_commit": "1f180c47419b420509ff59831e416db618ce5ce7",
            "workflow_run_id": 34748638941,
        },
    },
}
PUBLISHED_REGISTRY_DISTRIBUTIONS = {
    "0.2.4": (
        "shardloom-0.2.4-cp313-cp313-macosx_26_0_arm64.whl",
        "shardloom-0.2.4-cp313-cp313-manylinux_2_39_x86_64.whl",
        "shardloom-0.2.4-cp313-cp313-win_amd64.whl",
        "shardloom-0.2.4.tar.gz",
    ),
}
# Audited -c program embedded in both immutable bundled-wheel transcripts.
# It executes smoke_check, a DataFrame and two SQL calls, asserts complete typed
# results/no fallback, and prints the captured JSON result. A new release must
# approve its own program; arbitrary isolated Python is not execution evidence.
PUBLISHED_REGISTRY_BUNDLED_SMOKE_SHA256 = {
    "0.2.4": "d8f5c017d800ec0191dd058fd4b986b733c5f0379082f8e64450c7aa7353a6fb",
}
# Immutable, reviewed post-publication records. Pins bind every nested asset,
# command, output, recovery note and lifecycle result, including non-registry
# channels. Updating a record requires explicit review of a new approved pin.
PUBLISHED_CHANNEL_TRANSCRIPTS = {
    "0.2.4": {
        "github_prerelease": ("github-prerelease", "shardloom.github_prerelease_channel_proof.v1",
            "33ddeaae56d49a2942ea7fde303dc57902ad286722e0b0a4c04274381919ef43"),
        "testpypi": ("testpypi", "shardloom.python_registry_package_proof.v1",
            "1503562681588e8e1fb4b7c8195f68958b3c22d2047aa65cbee78cefd56854e8"),
        "pypi": ("pypi", "shardloom.python_registry_package_proof.v1",
            "2ba6c818a6fe78ed5b9954d41edf12d688125271cbe64f86620fedc7d8a1d895"),
        "homebrew_tap": ("homebrew", "shardloom.homebrew_channel_proof.v1",
            "bf4d86205eabab40727bb000dc82c9a1fb6d0ff1b0e9f3cb8fa1af0aabd05e94"),
    },
}
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
