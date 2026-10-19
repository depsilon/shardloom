#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate v1 docs productization surfaces and generated support docs."""

from __future__ import annotations

import argparse
import ast
import json
import re
from pathlib import Path
from typing import Any

from release_report_utils import fail_closed_fields, read_text, require_markers, resolve_path, write_json
from write_v1_supported_unsupported_docs import (
    DEFAULT_OUTPUT as SUPPORTED_UNSUPPORTED_DOC,
)
from write_v1_supported_unsupported_docs import (
    DEFAULT_PACKAGE_MATRIX,
    DEFAULT_RUNS_TODAY,
)
from write_v1_supported_unsupported_docs import render as render_supported_unsupported


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = "shardloom.v1_docs_productization_report.v1"

DOC_MARKERS: dict[str, tuple[str, ...]] = {
    # Independent contributor/safety contracts, not onboarding prose.
    "docs/getting-started/source-checkout-install.md": (
        "source_checkout_install_v1=true", "SHARDLOOM_BIN", "fallback_attempted=false",
    ),
    "docs/getting-started/troubleshooting-support.md": (
        "doctor --format json", "support-bundle --format json",
        "support_bundle_written=false", "fallback_attempted=false", "external_engine_invoked=false",
    ),
}
GUIDE_ROOT = Path("website-src/src/content/docs/field-guide")
GUIDE_SLUGS = (
    "index", "start-local-proof", "python-surface", "sql", "cli", "runtime-and-io",
    "resources", "execution-model", "compute-flow", "execution-routes", "reference",
    "embedding", "limitations", "troubleshooting", "development", "benchmark-methodology",
)
LEGACY_ONBOARDING = (
    "docs/getting-started/install.md",
    "docs/getting-started/package-user-install.md",
    "docs/getting-started/first-10-minutes.md",
    SUPPORTED_UNSUPPORTED_DOC.as_posix(),
)
CONTRACT_INPUTS = (
    "README.md", "python/README.md", "examples/paid-orders/quickstart.py",
    "examples/paid-orders/context-result.py",
    "website-src/src/data/quickstart.mjs", "website-src/src/data/release.mjs",
    "website-src/src/data/public-guide.json", "website-src/scripts/public-guide-data.mjs",
    "website-src/src/components/GuideVersion.astro",
    "website-src/src/components/PackagePlatforms.astro",
    "website-src/src/components/CapabilityTable.astro",
    "website-src/src/components/BenchmarkSummary.astro",
    "website-src/src/pages/start.astro", "website-src/src/pages/index.astro",
    "website-src/scripts/sync-content.mjs", "website-src/astro.config.mjs",
    "website-public/_redirects", "docs/release/local-source-rehearsal.md",
    *LEGACY_ONBOARDING,
    *(str(GUIDE_ROOT / (slug + ".mdx")) for slug in GUIDE_SLUGS),
)

FORBIDDEN_LIVE_PACKAGE_COMMANDS: tuple[str, ...] = (
    "cargo install shardloom",
    "conda install shardloom",
    "brew install shardloom",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("target/v1-docs-productization-report.json"),
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_generated_doc(repo_root: Path) -> list[str]:
    runs_today_path = resolve_path(repo_root, DEFAULT_RUNS_TODAY)
    package_matrix_path = resolve_path(repo_root, DEFAULT_PACKAGE_MATRIX)
    missing_inputs = [
        path
        for path in (runs_today_path, package_matrix_path)
        if not path.exists()
    ]
    if missing_inputs:
        return [
            f"{path.relative_to(repo_root).as_posix()}: missing generated-doc input"
            for path in missing_inputs
        ]
    runs_today = load_json(runs_today_path)
    package_matrix = load_json(package_matrix_path)
    expected = render_supported_unsupported(runs_today, package_matrix)
    actual = read_text(resolve_path(repo_root, SUPPORTED_UNSUPPORTED_DOC))
    if actual != expected:
        return [f"{SUPPORTED_UNSUPPORTED_DOC.as_posix()}: generated docs are stale"]
    return []


def example_regions(source: str) -> dict[str, str]:
    return {
        name: code.strip()
        for name, code in re.findall(
            r"^# snippet: (\w+)\n([\s\S]*?)(?=^# snippet: |\Z)", source, re.MULTILINE,
        )
        if name != "end"
    }


def readme_block(text: str, name: str) -> str | None:
    start, end = f"<!-- BEGIN SHARDLOOM {name} -->", f"<!-- END SHARDLOOM {name} -->"
    if text.count(start) != 1 or text.count(end) != 1 or text.index(start) > text.index(end):
        return None
    return text.split(start)[1].split(end)[0].strip()


def validate_release_facts(repo_root: Path) -> list[str]:
    blockers: list[str] = []
    try:
        matrix = load_json(repo_root / DEFAULT_PACKAGE_MATRIX)
        guide = matrix["user_guide"]
        if (guide["release_tag"] != matrix["selected_v0_1_0_release_tag"] or
                guide["source_revision"] != matrix["selected_v0_1_0_release_commit"]):
            blockers.append("guide release/revision must match the selected verified artifact")
        channel = next(row for row in matrix["channels"] if row["channel_id"] == guide["artifact_channel"])
        proof = load_json(repo_root / channel["install_transcript_ref"])
        version = guide["release_tag"].removeprefix("v")
        if not channel["ready"] or proof["proof_status"] != "passed" or proof["package_version"] != version:
            blockers.append("guide package channel must have matching passed install proof")
        artifacts = {row["filename"]: row for row in proof["registry_release_artifacts"]}
        if not guide["platforms"]:
            blockers.append("guide platforms must not be empty")
        tags: set[str] = set()
        for platform in guide["platforms"]:
            tag = platform["wheel_tag"]
            artifact = artifacts.get(f"shardloom-{version}-{tag}.whl", {})
            if tag in tags or not re.fullmatch(r"[a-f0-9]{64}", artifact.get("sha256", "")):
                blockers.append(f"guide platform has duplicate or unverified wheel tag: {tag}")
            tags.add(tag)
            if not platform["platform"] or not platform["python"] or not platform["backends"]:
                blockers.append(f"guide platform requirements are incomplete: {tag}")
            if set(platform["backends"]) - {"native", "subprocess"}:
                blockers.append(f"guide platform has an unknown backend: {tag}")
        ids: set[str] = set()
        if not guide["capabilities"]:
            blockers.append("guide capabilities must not be empty")
        for row in guide["capabilities"]:
            for field in ("id", "operation", "types_formats", "artifact_requirement", "restriction", "reference"):
                if not isinstance(row.get(field), str) or not row[field].strip():
                    blockers.append(f"guide capability {row.get('id')} missing {field}")
            if row["id"] in ids:
                blockers.append(f"duplicate guide capability: {row['id']}")
            ids.add(row["id"])
            if not row["reference"].startswith("/field-guide/"):
                blockers.append(f"guide capability must link an application contract: {row['id']}")
            else:
                slug = row["reference"].split("#", 1)[0].strip("/").split("/")[-1]
                if slug not in GUIDE_SLUGS:
                    blockers.append(f"guide capability reference does not resolve: {row['id']}")
    except (OSError, ValueError, KeyError, TypeError, StopIteration) as error:
        blockers.append(f"invalid public release facts: {error}")
    return blockers


def validate_projected_data(repo_root: Path, regions: dict[str, str]) -> list[str]:
    """A generated copy must match its executable and verified metadata owners."""
    blockers: list[str] = []
    try:
        data = load_json(repo_root / "website-src/src/data/public-guide.json")
        examples = data["examples"]
        development_source = (repo_root / "examples/paid-orders/context-result.py").read_text(encoding="utf-8").strip()
        if examples.get("developmentExample") != development_source:
            blockers.append("generated development example differs from its executable source")
        if examples["snippets"] != regions:
            blockers.append("generated guide snippets differ from the executable source")
        for key, names in (("pythonExample", ("setup", "dataframe", "cleanup")),
                           ("sqlExample", ("setup", "sql", "cleanup"))):
            if examples[key] != "\n\n".join(regions[name] for name in names):
                blockers.append(f"generated {key} differs from the executable source")
        matrix = load_json(repo_root / DEFAULT_PACKAGE_MATRIX)
        guide = matrix["user_guide"]
        release = data["release"]
        for key, value in guide.items():
            if key != "platforms" and release.get(key) != value:
                blockers.append(f"generated guide release fact is stale: {key}")
        channel = next(row for row in matrix["channels"] if row["channel_id"] == guide["artifact_channel"])
        proof = load_json(repo_root / channel["install_transcript_ref"])
        version = guide["release_tag"].removeprefix("v")
        artifacts = {row["filename"]: row for row in proof["registry_release_artifacts"]}
        platforms = [dict(row, artifact=artifacts[f"shardloom-{version}-{row['wheel_tag']}.whl"])
                     for row in guide["platforms"]]
        if release["platforms"] != platforms or release["installCommand"] != channel["install_command"]:
            blockers.append("generated guide package/platform facts differ from verified metadata")
        if release["version"] != version:
            blockers.append("generated guide version differs from the named release")
    except (OSError, ValueError, KeyError, TypeError, StopIteration) as error:
        blockers.append(f"invalid generated public guide data: {error}")
    return blockers


def validate_public_contract(repo_root: Path) -> list[str]:
    blockers: list[str] = []
    content = {path: read_text(repo_root / path) for path in CONTRACT_INPUTS}
    blockers.extend(f"{path}: missing public documentation input" for path, text in content.items() if not text)
    for path in LEGACY_ONBOARDING:
        text = content[path]
        if "https://shardloom.io/field-guide/start-local-proof" not in text:
            blockers.append(f"{path}: missing canonical first-workflow link")
        if "```" in text:
            blockers.append(f"{path}: obsolete onboarding still owns executable examples")
    regions = example_regions(content["examples/paid-orders/quickstart.py"])
    if set(regions) != {"setup", "dataframe", "sql", "write", "cleanup"}:
        blockers.append("quickstart regions do not cover the complete documented workflow")
    else:
        for name, code in regions.items():
            try:
                ast.parse(code)
            except SyntaxError:
                blockers.append(f"quickstart region is not executable Python: {name}")
        program = "\n\n".join(regions[name] for name in ("setup", "dataframe", "cleanup"))
        for path in ("README.md", "python/README.md"):
            block = readme_block(content[path], "QUICKSTART")
            if block is None or f"```python\n{program}\n```" not in block:
                blockers.append(f"{path}: quickstart differs from the executable source")
        if readme_block(content["README.md"], "RELEASE") != readme_block(content["python/README.md"], "RELEASE"):
            blockers.append("README release facts disagree")
        if readme_block(content["README.md"], "RELEASE") is None:
            blockers.append("README release facts are missing")
        blockers.extend(validate_projected_data(repo_root, regions))
    for page in ("start-local-proof", "sql"):
        if "../../../data/quickstart.mjs" not in content[str(GUIDE_ROOT / f"{page}.mdx")]:
            blockers.append(f"{page}: does not consume the canonical example")
    homepage = content["website-src/src/pages/index.astro"]
    if "../data/quickstart.mjs" not in homepage or "../data/release.mjs" not in homepage:
        blockers.append("homepage does not consume the example/release authorities")
    if 'Astro.redirect("/field-guide/start-local-proof", 301)' not in content["website-src/src/pages/start.astro"]:
        blockers.append("legacy /start must redirect to the canonical workflow")
    for route in ("/start", "/start/", "/start.html"):
        if f"{route} /field-guide/start-local-proof 301" not in content["website-public/_redirects"].splitlines():
            blockers.append(f"missing canonical HTTP redirect: {route}")
    sync = content["website-src/scripts/sync-content.mjs"]
    if "function docsPage(" in sync or "const durableDocsPages" in sync or "const fieldGuideIndex" in sync:
        blockers.append("sync-content still owns narrative guide pages")
    for group in ("Start", "Work with data", "Understand the engine", "Reference", "Versions and help"):
        if f'label: "{group}"' not in content["website-src/astro.config.mjs"]:
            blockers.append(f"task navigation group missing: {group}")
    if "pagefind: false" not in content[str(GUIDE_ROOT / "development.mdx")]:
        blockers.append("development/contributor material must be outside default product search")
    # All local guide links, including retained fragments, must resolve in authored content.
    pages = {("field-guide" if slug == "index" else f"field-guide/{slug}"):
             content[str(GUIDE_ROOT / f"{slug}.mdx")] for slug in GUIDE_SLUGS}
    for path, text in content.items():
        for target in re.findall(r'(?:href=["\']|\]\()(/field-guide[^"\'\s)]*)', text):
            route, _, fragment = target.partition("#")
            page = pages.get(route.strip("/"))
            if page is None:
                blockers.append(f"{path}: unresolved guide link {target}")
            elif fragment:
                headings = re.findall(r"^#{1,6} (.+)$", page, re.MULTILINE)
                anchors = set(re.findall(r'id=["\']([^"\']+)', page))
                anchors.update(re.sub(r"[^\w -]", "", title.lower()).replace(" ", "-") for title in headings)
                if fragment not in anchors:
                    blockers.append(f"{path}: unresolved guide fragment {target}")
        if path.endswith((".md", ".mdx")):
            for command in FORBIDDEN_LIVE_PACKAGE_COMMANDS:
                if command in text.lower():
                    blockers.append(f"{path}: forbidden live package command {command!r}")
    blockers.extend(validate_release_facts(repo_root))
    return blockers


def build_report(repo_root: Path) -> dict[str, Any]:
    blockers: list[str] = []
    checked_docs: list[str] = []
    for rel_path, markers in DOC_MARKERS.items():
        text = read_text(resolve_path(repo_root, rel_path))
        blockers.extend(require_markers(rel_path, text, markers))
        checked_docs.append(rel_path)
        if rel_path.startswith("docs/getting-started/") or rel_path == "README.md":
            lower_text = text.lower()
            for command in FORBIDDEN_LIVE_PACKAGE_COMMANDS:
                if command in lower_text:
                    blockers.append(f"{rel_path}: forbidden live package command {command!r}")
    blockers.extend(validate_generated_doc(repo_root))
    blockers.extend(validate_public_contract(repo_root))
    checked_docs.extend(CONTRACT_INPUTS)
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "passed" if not blockers else "failed",
        "checked_docs": checked_docs,
        "checked_doc_count": len(checked_docs),
        "generated_docs": [SUPPORTED_UNSUPPORTED_DOC.as_posix()],
        "runtime_examples_executed_by_this_validator": False,
        "claim_gate_status": "not_claim_grade",
        "blockers": blockers,
        **fail_closed_fields(),
    }


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    report = build_report(repo_root)
    write_json(resolve_path(repo_root, args.output), report)
    print(resolve_path(repo_root, args.output))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
