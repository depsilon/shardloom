#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Offline binding of ready channels to immutable, approved publication records."""

import hashlib
import json
from pathlib import Path

from release_channel_contract import (
    PUBLISHED_CHANNEL_TRANSCRIPTS,
    PUBLISHED_REGISTRY_BUILD_IDENTITIES,
    SELECTED_PACKAGE_RELEASE_VERSION,
    selected_channel_rows,
)


def validate_published_channel_proofs(repo_root: Path, matrix: dict | None) -> dict:
    blockers, verified = [], []
    version = SELECTED_PACKAGE_RELEASE_VERSION
    approved = PUBLISHED_CHANNEL_TRANSCRIPTS.get(version, {})
    for row in selected_channel_rows(matrix):
        if row.get("ready") is not True:
            continue
        channel = row["channel_id"]
        prefix = f"{channel}: published channel proof "
        start = len(blockers)
        identity = approved.get(channel)
        if identity is None:
            blockers.append(prefix + "requires an approved transcript for the selected version")
            continue
        stem, schema, digest = identity
        ref = f"docs/release/channel-proofs/{stem}-v{version}-transcript.json"
        for field in ("install_transcript_ref", "uninstall_transcript_ref", "clean_install_transcript_ref", "smoke_transcript_ref"):
            if row.get(field) != ref:
                blockers.append(prefix + f"{field} must reference the approved transcript")
        for field in ("prepared_local_install_smoke_ref", "prepared_registry_install_smoke_proof_ref"):
            if field in row and row[field] != ref:
                blockers.append(prefix + f"{field} must reference the approved transcript")
        path = repo_root / ref
        try:
            if not path.resolve().is_relative_to(repo_root.resolve()):
                raise ValueError("transcript escapes repository")
            with path.open("rb") as stream:
                raw = stream.read((4 << 20) + 1)
            if len(raw) > 4 << 20 or hashlib.sha256(raw).hexdigest() != digest:
                blockers.append(prefix + "SHA256 differs from the approved immutable transcript")
            proof = json.loads(raw)
            if not isinstance(proof, dict):
                raise ValueError("transcript is not a JSON object")
        except (OSError, ValueError, UnicodeError):
            blockers.append(prefix + "requires a readable JSON transcript inside the repository")
            continue
        for field, value in {"schema_version": schema, "channel_id": channel,
                             "proof_status": "passed", "install_transcript_status": "passed",
                             "uninstall_transcript_status": "passed", "smoke_check_status": "passed",
                             "fallback_attempted": False, "external_engine_invoked": False,
                             "secrets_required": False, "publication_attempted_by_this_tool": False}.items():
            if type(proof.get(field)) is not type(value) or proof[field] != value:
                blockers.append(prefix + f"{field} must be {value}")
        if channel in {"github_prerelease", "homebrew_tap"}:
            source_field = "target_commit" if channel == "github_prerelease" else "source_commit"
            source = PUBLISHED_REGISTRY_BUILD_IDENTITIES.get(version, {}).get("testpypi", {}).get("source_commit")
            if source is None or proof.get(source_field) != source or proof.get("release_tag") != f"v{version}":
                blockers.append(prefix + "must match the approved release source and tag")
            base = f"https://github.com/depsilon/shardloom/releases/download/v{version}/"
            for field, asset in {"sbom_ref": "shardloom-rust-workspace.cdx.json",
                                 "checksum_ref": "checksums.sha256",
                                 "provenance_ref": "supply-chain-release-evidence.json"}.items():
                if row.get(field) != base + asset:
                    blockers.append(prefix + f"{field} must bind the approved release asset")
            if channel == "homebrew_tap":
                for field in ("formula_version", "installed_version", "uninstalled_verified_version", "restored_installed_version"):
                    if proof.get(field) != version:
                        blockers.append(prefix + f"{field} must match the selected version")
                for field in ("clean_install_proof_status", "audit_status", "style_status", "linkage_status"):
                    if proof.get(field) != "passed":
                        blockers.append(prefix + f"{field} must be passed")
        elif proof.get("package_version") != version:
            blockers.append(prefix + "package_version must match the selected version")
        if len(blockers) == start:
            verified.append(channel)
    return {"status": "blocked" if blockers else "passed", "verified_channels": verified, "blockers": blockers}
