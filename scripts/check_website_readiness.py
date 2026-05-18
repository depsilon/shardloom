#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate the static ShardLoom website public-post readiness gate."""

from __future__ import annotations

import argparse
import json
import re
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from xml.etree import ElementTree


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_PAGES = [
    "index.html",
    "benchmarks.html",
    "compute-engine-flow.html",
    "status.html",
    "use-cases/index.html",
    "use-cases/first-10-minutes-local-smoke.html",
    "use-cases/local-file-etl-cleanup-smoke.html",
    "use-cases/compatibility-import-certified-local.html",
    "use-cases/prepared-native-vortex-runtime-direction.html",
    "use-cases/python-wrapper-client-smoke.html",
    "use-cases/sql-dataframe-capability-posture.html",
    "use-cases/source-free-generated-output-boundary.html",
    "use-cases/messy-data-local-fixtures.html",
    "use-cases/query-scenario-cookbook-smoke.html",
    "use-cases/output-result-sink-and-fanout-boundary.html",
    "use-cases/object-store-boundary-report.html",
    "use-cases/table-lakehouse-boundary-report.html",
    "use-cases/foundry-local-proof-boundary.html",
    "use-cases/evidence-audit-claim-gates.html",
    "use-cases/benchmark-interpretation-evidence-not-leaderboard.html",
    "use-cases/package-channel-readiness-boundary.html",
    "readme.html",
    "field-guide/index.html",
    "field-guide/no-fallback.html",
    "field-guide/execution-modes.html",
    "field-guide/compatibility-import-certified.html",
    "field-guide/prepared-vortex.html",
    "field-guide/native-vortex.html",
    "field-guide/native-io-certificate.html",
    "field-guide/materialization-boundary.html",
    "field-guide/claim-gates.html",
    "field-guide/benchmark-telemetry.html",
    "field-guide/unsupported-diagnostics.html",
]
EXPECTED_ASSETS = [
    "assets/logo/shardloom-favicon.png",
    "assets/logo/shardloom-logo.png",
    "assets/logo/shardloom-logo-trim.png",
    "assets/site.css",
    "assets/use-cases.js",
    "assets/compute-flow.js",
    "assets/data/compute-engine-flow-reference.md",
    "assets/data/benchmark-evidence.json",
    "assets/benchmarks/latest/manifest.json",
    "assets/benchmarks/latest/benchmark-results.json",
]
RUNTIME_SUFFIXES = (".html", ".js", ".css", ".xml", ".txt")
RUNTIME_NAMES = {"_headers", "_redirects"}
CLAIM_PHRASES = [
    r"\bShardLoom is faster\b",
    r"\bShardLoom is better\b",
    r"\bSpark replacement\b",
    r"\bproduction ready\b",
    r"\bproduction-ready\b",
    r"\bPolars cannot\b",
    r"\bDuckDB cannot\b",
    r"\bDataFusion cannot\b",
]
PACKAGE_CLAIM_PHRASES = [
    r"\bpip install shardloom\b",
    r"\bcargo install shardloom\b",
    r"\bpublished to PyPI\b",
    r"\bpublished crate\b",
]
BRAND_GUARDRAIL_PHRASES = [
    r"\bFallout\b",
    r"\bVault-Tec\b",
    r"\bPip-Boy\b",
    r"\bBethesda\b",
    r"\bModal\b",
    r"\bPalantir\b",
]
PRIVATE_MEMO_PHRASES = [
    r"\bprivate memo\b",
    r"\binternal-only\b",
    r"\bdo not publish\b",
]
RAW_GITHUB_HOST = "raw.githubusercontent.com"
URL_PATTERN = re.compile(r"https?://[^\s\"'<>]+")


class HtmlRefs(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.asset_refs: list[str] = []
        self.local_refs: list[str] = []
        self.canonical: str | None = None
        self.og: dict[str, str] = {}
        self.favicon_seen = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {key.lower(): value or "" for key, value in attrs}
        for key in ("src", "href", "content"):
            value = values.get(key)
            if not value:
                continue
            if "/assets/" in value:
                self.asset_refs.append(value)
        if tag == "a" and values.get("href"):
            self.local_refs.append(values["href"])
        if tag == "link" and values.get("rel") == "canonical":
            self.canonical = values.get("href")
        if tag == "meta" and values.get("property", "").startswith("og:"):
            self.og[values["property"]] = values.get("content", "")
        if tag == "link" and values.get("rel") in {"icon", "apple-touch-icon"}:
            if values.get("href") == "/assets/logo/shardloom-favicon.png":
                self.favicon_seen = True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=Path("target/website-readiness-report.json"))
    return parser.parse_args()


def rel(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def site_url_for(relative_path: str) -> str:
    if relative_path == "index.html":
        return "https://shardloom.io/"
    if relative_path.endswith("/index.html"):
        return f"https://shardloom.io/{relative_path.removesuffix('index.html')}"
    if relative_path.endswith(".html"):
        return f"https://shardloom.io/{relative_path.removesuffix('.html')}"
    return f"https://shardloom.io/{relative_path}"


def public_files(website: Path) -> list[Path]:
    files: list[Path] = []
    for path in website.rglob("*"):
        if path.is_file() and (path.suffix in RUNTIME_SUFFIXES or path.name in RUNTIME_NAMES or path.suffix == ".md"):
            files.append(path)
    return files


def runtime_files(website: Path) -> list[Path]:
    files: list[Path] = []
    for path in website.rglob("*"):
        if path.is_file() and (path.suffix in RUNTIME_SUFFIXES or path.name in RUNTIME_NAMES):
            files.append(path)
    return files


def normalize_asset_ref(ref: str) -> str | None:
    parsed = urlparse(ref)
    if parsed.scheme:
        if parsed.scheme != "https" or parsed.netloc != "shardloom.io":
            return None
        cleaned = parsed.path
    else:
        cleaned = ref.split("#", 1)[0].split("?", 1)[0]
    if not cleaned.startswith("/assets/"):
        return None
    return cleaned.removeprefix("/")


def check_patterns(
    files: list[Path],
    patterns: list[str],
    root: Path,
    blockers: list[str],
    label: str,
) -> None:
    combined = re.compile("|".join(f"(?:{pattern})" for pattern in patterns), re.IGNORECASE)
    for path in files:
        text = path.read_text(encoding="utf-8")
        if combined.search(text):
            blockers.append(f"{label}: {rel(path, root)}")


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    website = repo_root / "website"
    output = args.output if args.output.is_absolute() else repo_root / args.output
    blockers: list[str] = []
    warnings: list[str] = []

    if not website.exists():
        blockers.append("missing website directory")
    else:
        for expected in EXPECTED_PAGES + EXPECTED_ASSETS + ["404.html", "_headers", "_redirects", "robots.txt", "sitemap.xml"]:
            if not (website / expected).exists():
                blockers.append(f"missing expected website file: {expected}")

        html_files = sorted(website.rglob("*.html"))
        public_scan_files = public_files(website)
        runtime_scan_files = runtime_files(website)

        for path in runtime_scan_files:
            text = path.read_text(encoding="utf-8")
            relative = rel(path, website)
            for url in URL_PATTERN.findall(text):
                if urlparse(url).hostname == RAW_GITHUB_HOST:
                    blockers.append(f"runtime GitHub raw fetch/reference found: {relative}")
            if relative == "assets/compute-flow.js" and 'cache: "no-store"' in text:
                blockers.append("compute-flow.js bypasses static cache with no-store")

        missing_assets: list[str] = []
        missing_canonicals: list[str] = []
        missing_og: list[str] = []
        missing_favicon: list[str] = []
        for path in html_files:
            parser = HtmlRefs()
            parser.feed(path.read_text(encoding="utf-8"))
            relative = rel(path, website)
            for asset_ref in parser.asset_refs:
                normalized = normalize_asset_ref(asset_ref)
                if normalized and not (website / normalized).exists():
                    missing_assets.append(f"{relative} -> {normalized}")
            if relative != "404.html":
                expected_url = site_url_for(relative)
                if parser.canonical != expected_url:
                    missing_canonicals.append(f"{relative} expected {expected_url}")
                for key in ("og:title", "og:description", "og:image", "og:url"):
                    if not parser.og.get(key):
                        missing_og.append(f"{relative} missing {key}")
            if not parser.favicon_seen:
                missing_favicon.append(relative)

        if missing_assets:
            blockers.append("missing local asset refs: " + "; ".join(missing_assets))
        if missing_canonicals:
            blockers.append("missing/incorrect canonicals: " + "; ".join(missing_canonicals))
        if missing_og:
            blockers.append("missing Open Graph metadata: " + "; ".join(missing_og))
        if missing_favicon:
            blockers.append("missing favicon links: " + "; ".join(missing_favicon))

        sitemap = website / "sitemap.xml"
        if sitemap.exists():
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            tree = ElementTree.parse(sitemap)
            locs = {node.text or "" for node in tree.findall(".//sm:loc", ns)}
            for expected in EXPECTED_PAGES:
                expected_url = site_url_for(expected)
                if expected_url not in locs:
                    blockers.append(f"sitemap missing expected page: {expected_url}")
        else:
            blockers.append("missing sitemap.xml")

        check_patterns(public_scan_files, CLAIM_PHRASES, website, blockers, "forbidden public claim phrase")
        check_patterns(public_scan_files, PACKAGE_CLAIM_PHRASES, website, blockers, "package-publication claim phrase")
        check_patterns(runtime_scan_files, BRAND_GUARDRAIL_PHRASES, website, blockers, "third-party brand reference in runtime surface")
        check_patterns(public_scan_files, PRIVATE_MEMO_PHRASES, website, blockers, "private memo reference")

        site_css = website / "assets" / "site.css"
        if site_css.exists():
            css = site_css.read_text(encoding="utf-8")
            uses_motion = any(token in css for token in ("animation:", "transition:", "@keyframes"))
            if uses_motion and "prefers-reduced-motion" not in css:
                blockers.append("motion CSS exists without prefers-reduced-motion guard")
        else:
            blockers.append("missing assets/site.css")

        if not (website / "assets" / "data" / "compute-engine-flow-reference.md").exists():
            blockers.append("missing local compute-flow reference snapshot")

        field_guide_index = website / "content" / "field-guide-index.json"
        field_guide_page = website / "field-guide" / "index.html"
        if field_guide_index.exists():
            try:
                field_guide_data = json.loads(field_guide_index.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                blockers.append(f"Field Guide index is not valid JSON: {exc}")
            else:
                entries = field_guide_data.get("entries") or []
                if len(entries) < 50:
                    blockers.append("Field Guide index must contain at least 50 entries")
                reading_paths = field_guide_data.get("reading_paths") or []
                if len(reading_paths) < 7:
                    blockers.append("Field Guide index must contain at least 7 reading paths")
                categories = set(field_guide_data.get("categories") or [])
                for required in [
                    "Start Here",
                    "Execution Modes",
                    "Engine Modes",
                    "Vortex Runtime",
                    "Evidence And Claims",
                    "Benchmark Telemetry",
                    "User Workflows",
                    "I/O And Output",
                    "Platform Boundaries",
                    "Performance Architecture",
                    "Release And Trust",
                ]:
                    if required not in categories:
                        blockers.append(f"Field Guide index missing category: {required}")
                reading_path_ids = {path.get("id") for path in reading_paths}
                for required in [
                    "new-to-shardloom",
                    "run-a-local-workflow",
                    "understand-benchmarks",
                    "understand-vortex-native-paths",
                    "use-python-sql-dataframe",
                    "know-what-is-blocked",
                    "foundry-and-platform-context",
                ]:
                    if required not in reading_path_ids:
                        blockers.append(f"Field Guide index missing reading path: {required}")
                entry_slugs = {entry.get("slug") for entry in entries}
                for path in reading_paths:
                    path_id = path.get("id", "<missing>")
                    for required in ("id", "title", "summary", "status", "terms", "use_cases", "claim_boundary"):
                        if not path.get(required):
                            blockers.append(f"Field Guide reading path {path_id} missing {required}")
                    for term in path.get("terms") or []:
                        if term not in entry_slugs:
                            blockers.append(
                                f"Field Guide reading path {path_id} unknown term: {term}"
                            )
                for entry in entries:
                    entry_id = entry.get("slug", "<missing>")
                    for required in ("slug", "title", "category", "status", "summary", "reference_files", "claim_boundary"):
                        if not entry.get(required):
                            blockers.append(f"Field Guide entry {entry_id} missing {required}")
                    slug_value = entry.get("slug")
                    if slug_value and not (website / "field-guide" / f"{slug_value}.html").exists():
                        blockers.append(f"Field Guide entry missing generated page: {slug_value}")
        else:
            blockers.append("missing website/content/field-guide-index.json")

        if field_guide_page.exists():
            field_guide_text = field_guide_page.read_text(encoding="utf-8")
            for required in [
                "Reading paths",
                "New to ShardLoom",
                "Run a local workflow",
                "Understand benchmarks",
                "Understand Vortex-native paths",
                "Use Python, SQL, or DataFrame surfaces",
                "Know what is blocked",
                "Foundry and platform context",
                "Table of contents",
                "Start Here",
                "Execution Modes",
                "Vortex Runtime",
                "Evidence And Claims",
                "Performance Architecture",
                "Release And Trust",
            ]:
                if required not in field_guide_text:
                    blockers.append(f"Field Guide index page missing atlas field: {required}")
        else:
            blockers.append("missing field-guide/index.html")

        status_page = website / "status.html"
        if status_page.exists():
            status_text = status_page.read_text(encoding="utf-8")
            for required in [
                "Answer common capability questions in under two minutes.",
                "runtime supported",
                "smoke supported",
                "report only",
                "blocked",
                "planned",
                "not planned",
                "Public package channels",
                "Enterprise evidence export pack",
                "docs/release/enterprise-evidence-export-pack.json",
                "Foundry dev-stack starter",
                "docs/foundry/dev-stack-starter-kit.json",
                "Workflow recipe library",
                "docs/use-cases/recipes/recipe-index.json",
                "docs/architecture/universal-compatibility-coverage-scoreboard.json",
                "docs/release/package-channel-readiness-matrix.json",
                "fallback_attempted=false",
                "external_engine_invoked=false",
            ]:
                if required not in status_text:
                    blockers.append(f"status page missing buyer-facing scorecard field: {required}")

        manifest_path = website / "assets" / "benchmarks" / "latest" / "manifest.json"
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                blockers.append(f"benchmark manifest is not valid JSON: {exc}")
            else:
                if manifest.get("performance_claim_allowed") is not False:
                    blockers.append("benchmark manifest must keep performance_claim_allowed=false")
                if not manifest.get("expected_lanes"):
                    blockers.append("benchmark manifest must list expected_lanes")
                if "available_lanes" not in manifest or "missing_lanes" not in manifest:
                    blockers.append("benchmark manifest must list available_lanes and missing_lanes")
                artifact_paths = manifest.get("artifact_paths") or {}
                artifact_json = artifact_paths.get("json")
                if artifact_json and not (repo_root / artifact_json).exists():
                    blockers.append(f"benchmark manifest artifact_paths.json does not exist: {artifact_json}")

    report: dict[str, Any] = {
        "schema_version": "shardloom.website_readiness.v1",
        "status": "passed" if not blockers else "blocked",
        "blockers": blockers,
        "warnings": warnings,
        "expected_pages": EXPECTED_PAGES,
        "expected_assets": EXPECTED_ASSETS,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"website readiness {report['status']}: {output}")
    if blockers:
        for blocker in blockers:
            print(f"- {blocker}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
