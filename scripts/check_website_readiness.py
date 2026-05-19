#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate the static ShardLoom website public-post readiness gate."""

from __future__ import annotations

import argparse
import html
import json
import re
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from xml.etree import ElementTree

from check_use_case_index import (
    EXPLANATION_STATUSES,
    SUPPORTED_STATUSES,
    load_index as load_use_case_index,
)
from check_universal_ingress_routes import (
    TAXONOMY_PATH as UNIVERSAL_INGRESS_TAXONOMY_PATH,
    validate_taxonomy as validate_universal_ingress_taxonomy,
)


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
    "pagefind/pagefind-component-ui.css",
    "pagefind/pagefind-component-ui.js",
    "pagefind/pagefind-entry.json",
    "pagefind/pagefind.js",
    "pagefind/pagefind-worker.js",
    "pagefind/wasm.en.pagefind",
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
    r"\bmodal\.com\b",
    r"\bPalantir\b",
]
PRIVATE_MEMO_PHRASES = [
    r"\bprivate memo\b",
    r"\binternal-only\b",
    r"\bdo not publish\b",
]
PAGEFIND_MODAL_COMPONENT_RE = re.compile(
    r"\b(?:pagefind-modal(?:-[a-z]+)?|pf-modal(?:-[a-z]+)?|modal-trigger)\b",
    re.IGNORECASE,
)
RAW_GITHUB_HOST = "raw.githubusercontent.com"
URL_PATTERN = re.compile(r"https?://[^\s\"'<>]+")
FIELD_GUIDE_DOSSIER_REQUIRED_FIELDS = [
    "atlas-sidebar",
    "atlas-article-hero",
    "atlas-meta-grid",
    "atlas-article-jump",
    'id="meaning"',
    'id="why"',
    'id="how"',
    'id="support"',
    'id="evidence"',
    'id="boundary"',
    'id="try-it"',
    'id="related"',
    'id="sources"',
    "Plain-English meaning",
    "Why it matters",
    "How ShardLoom uses it",
    "Current support",
    "Evidence fields",
    "What it does not claim",
    "Try it / related use cases",
    "Related concepts",
    "Reference files",
    "related-concepts-rail",
    "claim-badge",
    'data-citation-block="reference-files"',
    "What this proves:",
]
USE_CASE_PAGE_REQUIRED_FIELDS = [
    "atlas-surface",
    "atlas-surface-sidebar",
    "atlas-surface-body",
    "Knowledge Atlas",
    "Core Surfaces",
    "On This Page",
    "Use Case Atlas",
    "Plain-English Summary",
    "Status Table",
    "Claim Boundary",
    "Internal Flow",
    "Expected Evidence Fields",
    "Expected Output Or Evidence",
    "Common Mistakes",
    "Reference Files",
    "Related Field Guide Terms",
    "Related Use Cases",
    "Claim gate",
    "fallback_attempted=false",
    "external_engine_invoked=false",
    'data-citation-block="reference-files"',
    "What this proves:",
]
ATLAS_SURFACE_REQUIRED_FIELDS = [
    "atlas-surface",
    "atlas-surface-sidebar",
    "atlas-surface-body",
    "Knowledge Atlas",
    "Core Surfaces",
    "On This Page",
    "pagefind-modal-trigger",
]
ATLAS_SURFACE_PAGES = [
    "compute-engine-flow.html",
    "status.html",
    "readme.html",
    "use-cases/index.html",
]
FIELD_GUIDE_REQUIRED_ENTRY_LISTS = [
    "evidence_fields",
    "related_terms",
    "related_use_cases",
    "reference_files",
]
FIELD_GUIDE_TEXT_LIMITS = {
    "title": 80,
    "summary": 220,
    "status": 48,
    "claim_boundary": 300,
}
FIELD_GUIDE_DOSSIER_SECTION_WORD_LIMITS = {
    "meaning": 45,
    "why": 70,
    "how": 80,
    "support": 85,
    "boundary": 90,
    "try-it": 75,
}
FIELD_GUIDE_READING_PATH_LIMITS = {
    "title": 80,
    "summary": 220,
    "status": 48,
    "claim_boundary": 300,
}
USE_CASE_HTML_SECTION_WORD_LIMITS = {
    "plain-english-summary": 90,
    "claim-boundary": 220,
    "expected-output-or-evidence": 220,
}
TAG_RE = re.compile(r"<[^>]+>")


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
        if label.startswith("third-party brand reference"):
            text = PAGEFIND_MODAL_COMPONENT_RE.sub("pagefind-search-component", text)
        if combined.search(text):
            blockers.append(f"{label}: {rel(path, root)}")


def inline_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def compact_text_from_html(source: str) -> str:
    return " ".join(html.unescape(TAG_RE.sub(" ", source)).split())


def html_section_text(source: str, section_id: str) -> str:
    section_pattern = re.compile(
        rf'<section\b[^>]*\bid="{re.escape(section_id)}"[^>]*>(.*?)</section>',
        re.IGNORECASE | re.DOTALL,
    )
    match = section_pattern.search(source)
    if match:
        return compact_text_from_html(match.group(1))
    heading_pattern = re.compile(
        rf'<h2\b[^>]*\bid="{re.escape(section_id)}"[^>]*>.*?</h2>(.*?)(?=<h2\b[^>]*\bid=|</article>)',
        re.IGNORECASE | re.DOTALL,
    )
    match = heading_pattern.search(source)
    if match:
        return compact_text_from_html(match.group(1))
    return ""


def word_count(text: str) -> int:
    return len(re.findall(r"\b[\w./=+-]+\b", text))


def enforce_text_limits(
    *,
    label: str,
    item: dict[str, Any],
    limits: dict[str, int],
    blockers: list[str],
) -> None:
    for field, limit in limits.items():
        value = str(item.get(field, ""))
        if len(value) > limit:
            blockers.append(f"{label} {field} is too long: {len(value)} > {limit}")


def validate_field_guide_dossier_concision(
    *,
    slug_value: str,
    dossier_text: str,
    blockers: list[str],
) -> None:
    for section_id, limit in FIELD_GUIDE_DOSSIER_SECTION_WORD_LIMITS.items():
        text = html_section_text(dossier_text, section_id)
        if not text:
            blockers.append(f"Field Guide dossier {slug_value} missing section id: {section_id}")
            continue
        words = word_count(text)
        if words > limit:
            blockers.append(
                f"Field Guide dossier {slug_value} section {section_id} is too long: "
                f"{words} words > {limit}"
            )
    boundary_text = html_section_text(dossier_text, "boundary")
    paragraphs = [
        compact_text_from_html(match)
        for match in re.findall(r"<p\b[^>]*>(.*?)</p>", boundary_text, flags=re.DOTALL)
    ]
    paragraphs = [paragraph for paragraph in paragraphs if paragraph and paragraph != "Claim boundary"]
    if len(paragraphs) != len(set(paragraphs)):
        blockers.append(f"Field Guide dossier {slug_value} repeats claim-boundary text")


def validate_use_case_page_concision(
    *,
    relative: str,
    source: str,
    blockers: list[str],
) -> None:
    for section_id, limit in USE_CASE_HTML_SECTION_WORD_LIMITS.items():
        text = html_section_text(source, section_id)
        if not text:
            blockers.append(f"use-case page {relative} missing section id: {section_id}")
            continue
        words = word_count(text)
        if words > limit:
            blockers.append(
                f"use-case page {relative} section {section_id} is too long: "
                f"{words} words > {limit}"
            )


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    website = repo_root / "website"
    output = args.output if args.output.is_absolute() else repo_root / args.output
    blockers: list[str] = []
    warnings: list[str] = []

    if UNIVERSAL_INGRESS_TAXONOMY_PATH.exists():
        try:
            route_taxonomy = json.loads(
                UNIVERSAL_INGRESS_TAXONOMY_PATH.read_text(encoding="utf-8")
            )
        except json.JSONDecodeError as exc:
            blockers.append(f"UniversalIngress route taxonomy is not valid JSON: {exc}")
        else:
            blockers.extend(
                f"UniversalIngress route taxonomy: {error}"
                for error in validate_universal_ingress_taxonomy(route_taxonomy)
            )
    else:
        blockers.append("missing UniversalIngress route taxonomy JSON")

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
        brand_scan_files = [
            path
            for path in runtime_scan_files
            if not rel(path, website).startswith("pagefind/")
        ]
        check_patterns(brand_scan_files, BRAND_GUARDRAIL_PHRASES, website, blockers, "third-party brand reference in runtime surface")
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
        pagefind_entry_path = website / "pagefind" / "pagefind-entry.json"
        if pagefind_entry_path.exists():
            try:
                pagefind_entry = json.loads(pagefind_entry_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                blockers.append(f"Pagefind entry metadata is not valid JSON: {exc}")
            else:
                if pagefind_entry.get("version") != "1.5.2":
                    blockers.append("Pagefind entry metadata must record version 1.5.2")
                if (pagefind_entry.get("languages") or {}).get("en", {}).get("page_count", 0) < 90:
                    blockers.append("Pagefind index must cover generated website pages")
        else:
            blockers.append("missing Pagefind entry metadata")

        headers = website / "_headers"
        if headers.exists():
            headers_text = headers.read_text(encoding="utf-8")
            for required in [
                "/pagefind/*",
                "worker-src 'self'",
                "script-src 'self' 'wasm-unsafe-eval'",
            ]:
                if required not in headers_text:
                    blockers.append(f"_headers missing Pagefind static-search policy: {required}")

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
                    enforce_text_limits(
                        label=f"Field Guide reading path {path_id}",
                        item=path,
                        limits=FIELD_GUIDE_READING_PATH_LIMITS,
                        blockers=blockers,
                    )
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
                    enforce_text_limits(
                        label=f"Field Guide entry {entry_id}",
                        item=entry,
                        limits=FIELD_GUIDE_TEXT_LIMITS,
                        blockers=blockers,
                    )
                    for required_list in FIELD_GUIDE_REQUIRED_ENTRY_LISTS:
                        values = inline_list(entry.get(required_list))
                        if not values:
                            blockers.append(
                                f"Field Guide entry {entry_id} field {required_list} must not be empty"
                            )
                    for reference in inline_list(entry.get("reference_files")):
                        if not isinstance(reference, str):
                            blockers.append(
                                f"Field Guide entry {entry_id} reference must be a string"
                            )
                            continue
                        if "*" in reference:
                            blockers.append(
                                f"Field Guide entry {entry_id} reference must be exact, not a glob: {reference}"
                            )
                            continue
                        if not (repo_root / reference).exists():
                            blockers.append(
                                f"Field Guide entry {entry_id} reference does not exist: {reference}"
                            )
                    slug_value = entry.get("slug")
                    if slug_value:
                        dossier_path = website / "field-guide" / f"{slug_value}.html"
                        if not dossier_path.exists():
                            blockers.append(f"Field Guide entry missing generated page: {slug_value}")
                        else:
                            dossier_text = dossier_path.read_text(encoding="utf-8")
                            for required in FIELD_GUIDE_DOSSIER_REQUIRED_FIELDS:
                                if required not in dossier_text:
                                    blockers.append(
                                        f"Field Guide dossier {slug_value} missing public-readiness field: {required}"
                                    )
                            validate_field_guide_dossier_concision(
                                slug_value=str(slug_value),
                                dossier_text=dossier_text,
                                blockers=blockers,
                            )
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
                "Search atlas",
                "pagefind-component-ui.css",
                "pagefind-component-ui.js",
                "pagefind-modal-trigger",
                "pagefind-filter-dropdown",
                'data-pagefind-filter="section"',
                'data-pagefind-filter="status"',
                "atlas-sidebar",
                "atlas-stat-row",
                "atlas-reading-grid",
                "atlas-family",
                "atlas-term-row",
                "reference-badge",
            ]:
                if required not in field_guide_text:
                    blockers.append(f"Field Guide index page missing atlas field: {required}")
        else:
            blockers.append("missing field-guide/index.html")

        for relative in ATLAS_SURFACE_PAGES:
            atlas_page = website / relative
            if not atlas_page.exists():
                blockers.append(f"missing atlas surface page: {relative}")
                continue
            atlas_text = atlas_page.read_text(encoding="utf-8")
            for required in ATLAS_SURFACE_REQUIRED_FIELDS:
                if required not in atlas_text:
                    blockers.append(f"atlas surface page {relative} missing field: {required}")

        status_page = website / "status.html"
        if status_page.exists():
            status_text = status_page.read_text(encoding="utf-8")
            for required in [
                "Answer common capability questions in under two minutes.",
                "Capability status matrix",
                "data-status-matrix-filters",
                'data-status-matrix-filter="status"',
                'data-status-matrix-filter="input"',
                'data-status-matrix-filter="output"',
                'data-status-matrix-filter="execution"',
                'data-status-matrix-filter="evidence"',
                'data-status-matrix-filter="platform"',
                "data-status-matrix-grid",
                "data-status-matrix-count",
                "docs/use-cases/use-case-index.yml",
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

        use_case_pages = sorted((website / "use-cases").glob("*.html"))
        use_case_index_path = repo_root / "docs" / "use-cases" / "use-case-index.yml"
        indexed_use_cases: dict[str, dict[str, Any]] = {}
        if use_case_index_path.exists():
            try:
                use_case_data = load_use_case_index(use_case_index_path)
            except ValueError as exc:
                blockers.append(f"use-case index cannot be loaded for website readiness: {exc}")
            else:
                indexed_use_cases = {
                    str(use_case.get("id")): use_case
                    for use_case in use_case_data.get("use_cases", [])
                    if isinstance(use_case, dict) and use_case.get("id")
                }
                # Every use case still needs a runnable example or blocker explanation;
                # supported statuses require runnable examples, while explanation statuses
                # require blocked explanations so support cannot be inferred from blockers.
                for use_case_id, use_case in indexed_use_cases.items():
                    expected_page = website / "use-cases" / f"{use_case_id}.html"
                    if not expected_page.exists():
                        blockers.append(f"use-case index missing generated website page: {use_case_id}")
                    status = str(use_case.get("status"))
                    if status in SUPPORTED_STATUSES and not use_case.get("runnable_example"):
                        blockers.append(
                            f"use case {use_case_id} status {status} requires runnable_example"
                        )
                    if status in EXPLANATION_STATUSES and not use_case.get("blocked_explanation"):
                        blockers.append(
                            f"use case {use_case_id} status {status} requires blocked_explanation"
                        )
        else:
            blockers.append("missing docs/use-cases/use-case-index.yml")

        for use_case_page in use_case_pages:
            if use_case_page.name == "index.html":
                continue
            relative = rel(use_case_page, website)
            use_case_text = use_case_page.read_text(encoding="utf-8")
            for required in USE_CASE_PAGE_REQUIRED_FIELDS:
                if required not in use_case_text:
                    blockers.append(f"use-case page {relative} missing public-readiness field: {required}")
            validate_use_case_page_concision(
                relative=relative,
                source=use_case_text,
                blockers=blockers,
            )
            use_case_id = use_case_page.stem
            indexed_use_case = indexed_use_cases.get(use_case_id)
            if indexed_use_case:
                if indexed_use_case.get("runnable_example") and "Quick Example" not in use_case_text:
                    blockers.append(f"use-case page {relative} missing runnable Quick Example")
                status = str(indexed_use_case.get("status"))
                if status in EXPLANATION_STATUSES and "use-case-blocker" not in use_case_text:
                    blockers.append(
                        f"use-case page {relative} missing blocker explanation for {status} status"
                    )

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
