#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate the ShardLoom public website."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from check_runtime_execution_envelopes import validate_repo as validate_runtime_envelopes
from check_runtime_promotion_evidence import validate_runtime_promotion_evidence
from check_benchmark_optimization_targets import build_report as build_optimization_target_report


ROOT = Path(__file__).resolve().parents[1]
CLOUDFLARE_STATIC_ASSET_MAX_BYTES = 25 * 1024 * 1024
EXPECTED_PAGES = [
    "index.html",
    "about.html",
    "about/index.html",
    "start.html",
    "start/index.html",
    "field-guide.html",
    "field-guide/index.html",
    "field-guide/start-local-proof/index.html",
    "field-guide/python-surface/index.html",
    "field-guide/benchmark-methodology/index.html",
    "field-guide/limitations/index.html",
    "benchmarks.html",
    "benchmarks/index.html",
    "compute-engine-flow.html",
    "compute-engine-flow/index.html",
    "404.html",
]
EXPECTED_ASSETS = [
    "assets/logo/shardloom-favicon.png",
    "assets/logo/shardloom-logo.png",
    "assets/logo/shardloom-logo-trim.png",
    "assets/site.css",
    "assets/data/compute-engine-flow-reference.md",
    "assets/data/benchmark-evidence.json",
    "assets/benchmarks/latest/manifest.json",
    "assets/benchmarks/latest/benchmark-results.json",
    "pagefind/pagefind-entry.json",
]
EXPECTED_REDIRECTS = [
    "/architecture",
    "/architecture.html",
    "/use-cases",
    "/use-cases.html",
    "/status",
    "/status.html",
    "/docs",
    "/docs.html",
    "/readme",
    "/can-i-use-this",
]
EXPECTED_NAV_PATHS = {
    "/",
    "/about",
    "/start",
    "/field-guide",
    "/benchmarks",
    "/compute-engine-flow",
}
STATUS_VOCABULARY = {
    "runtime_supported",
    "scoped_runtime_supported",
    "smoke_supported",
    "internal_smoke_only",
    "fixture_smoke_only",
    "ready_local",
    "report_only",
    "planned",
    "blocked",
    "unsupported",
    "not_planned",
    "executable",
    "feature_gated",
    "diagnostic_only",
    "claim_grade",
    "not_claim_grade",
    "external_baseline_only",
    "local_equivalence_evidence_present_claim_gated",
    "claim_blocked",
    "claim_allowed",
    "optimization_ready",
    "not_optimization_ready",
    "future",
    "current",
    "not reported",
    "stale_or_dirty",
    "stale or dirty",
}
REQUIRED_BENCHMARK_ROUTE_CARDS = {
    "cold_certified_route": "ShardLoom Cold Certified Route",
    "prepare_once_first_query": "ShardLoom Prepare-Once First Query",
    "prepare_once_batch": "ShardLoom Prepare-Once Batch",
    "warm_prepared_query": "ShardLoom Warm Prepared Query",
    "native_vortex_query": "ShardLoom Native Vortex Query",
}
REQUIRED_BENCHMARK_TIMING_SURFACE_STRINGS = {
    "data-route-timing-surface-dashboard",
    "hot_runtime",
    "publication_proof",
    "Hot route geomean",
    "Publication-proof route geomean",
    "timing_surface=hot_runtime",
    "timing_surface=publication_proof",
    "result-sink and evidence-render work",
    "Hot runtime rows drive the primary ShardLoom route grid.",
    "Cold-certified route totals include raw-source ingest",
    "Scale",
}
REQUIRED_BENCHMARK_STAGE_STRINGS = {
    "Stage attribution",
    "Included hot runtime",
    "Included publication proof",
    "Diagnostic only",
    "route timing instrument readiness",
}
REQUIRED_BENCHMARK_ROUTE_SHARE_STRINGS = {
    "Optimization direction",
    "Route-share attribution",
    "Route-share attribution ranks measured stage cost",
    "diagnostic_absent_or_retired",
    "target_disappearance_policy=diagnostic_absent_or_retired_not_release_blocker",
    "retired optimization targets",
    "shardloom.traditional_analytics.route_share_amdahl.v1",
}
REQUIRED_BENCHMARK_RUNTIME_STRINGS = {
    "Runtime support is separate from claim readiness.",
    "ShardLoom unsupported rows",
    "External baseline unsupported rows",
    "runtime_supported",
    "scoped_runtime_supported",
    "claim_grade",
    "not_claim_grade",
    "external_baseline_only",
}
PUBLIC_FRONT_DOOR_BENCHMARK_SCHEMA_VERSION = (
    "shardloom.public_front_door_benchmark_rows.v1"
)
REQUIRED_PUBLIC_FRONT_DOOR_BENCHMARK_IDS = {
    "local_source_auto_prepare_vortex_front_door",
    "generated_source_prepare_vortex_front_door",
}
REQUIRED_PUBLIC_FRONT_DOOR_HTML_TOKENS = {
    "Public front doors",
    "Route rows name the user-facing prepared paths.",
    "ctx.prepare_vortex(&#39;fact.csv&#39;, dim=&#39;dim.csv&#39;, workspace=&#39;target/shardloom-prepared&#39;).query(&#39;selective filter&#39;).collect()",
    "ctx.from_rows([{&#39;id&#39;: 1, &#39;label&#39;: &#39;alpha&#39;}]).prepare_vortex(workspace=&#39;target/shardloom-prepared&#39;)",
    "not_timing_row_route_identity_only",
    "SourceState",
    "GeneratedSourceState",
    "VortexPreparedState",
}
REQUIRED_BENCHMARK_ARTIFACT_STRINGS = {
    "Promoted artifact",
    "The website results are current to the promoted artifact.",
    "performance_claim_allowed",
    "Benchmark constitution gate",
    "Route timing instrument status",
    "Benchmark data ownership",
    "Static mirrors have one canonical artifact.",
    "website-public/assets/benchmarks/latest/benchmark-results.json",
    "mirror digest checks",
    "Artifact lane availability",
    "Expected lanes stay visible.",
    "Format coverage rows",
    "Prepared/native source-state coverage",
    "source_state_coverage_all_requested_scenarios_classified",
    "Front-door equivalence",
    "SQL, Python, and DataFrame rows share the same local Vortex route.",
    "local_equivalence_evidence_present_claim_gated",
    "metadata_sink",
    "claim_blocked",
    "Raw timing tables",
}
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
REMOVED_WEBSITE_SURFACES: list[str] = [
    "architecture.html",
    "architecture/index.html",
    "docs.html",
    "docs/index.html",
    "status.html",
    "status/index.html",
    "use-cases.html",
    "use-cases/index.html",
]
RUNTIME_SUFFIXES = (".html", ".js", ".css", ".xml", ".txt")
RUNTIME_NAMES = {"_headers", "_redirects"}
FORBIDDEN_RUNTIME_HOSTS = {"raw.githubusercontent.com"}
FORBIDDEN_RUNTIME_SNIPPETS = {"docs/architecture/phased-execution-plan.md"}
URL_RE = re.compile(r"https?://[^\s\"'<>)]+")
STATUS_CHIP_RE = re.compile(r'<span class="status-chip[^"]*">([^<]+)</span>')
ROUTE_CARD_ID_RE = re.compile(r'data-route-card-id="([^"]+)"')
PUBLIC_FRONT_DOOR_ID_RE = re.compile(r'data-public-front-door-id="([^"]+)"')
DUPLICATE_SUFFIX_RE = re.compile(r" \d+(?:\.[^.]+)?$")
ROW_ADMISSION_MANIFEST_SCHEMA_VERSION = (
    "shardloom.website.benchmark_row_admission_manifest.v1"
)


class HtmlRefs(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.html_lang: str | None = None
        self.in_title = False
        self.title_parts: list[str] = []
        self.meta: dict[str, str] = {}
        self.canonical: str | None = None
        self.og: dict[str, str] = {}
        self.assets: list[str] = []
        self.local_links: list[str] = []
        self.nav_links: set[str] = set()
        self.images: list[dict[str, str]] = []
        self.unlabeled_controls: list[str] = []
        self.anchor_without_href_count = 0
        self.h1_count = 0
        self.landmarks: set[str] = set()
        self.open_details_count = 0
        self.label_depth = 0
        self.favicon_seen = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {key.lower(): value or "" for key, value in attrs}
        if tag == "html":
            self.html_lang = values.get("lang")
        if tag == "title":
            self.in_title = True
        if tag == "meta" and values.get("name"):
            self.meta[values["name"].lower()] = values.get("content", "")
        if tag in {"header", "main", "footer"}:
            self.landmarks.add(tag)
        if tag == "h1":
            self.h1_count += 1
        if tag == "label":
            self.label_depth += 1
        for key in ("href", "src", "content"):
            value = values.get(key)
            if value and "/assets/" in value:
                self.assets.append(value)
        if tag == "a" and values.get("href", "").startswith("/"):
            self.local_links.append(values["href"])
            if values["href"] in EXPECTED_NAV_PATHS:
                self.nav_links.add(values["href"])
        if tag == "a" and "href" not in values:
            self.anchor_without_href_count += 1
        if tag == "img":
            self.images.append(values)
        if tag in {"input", "select", "textarea", "button"}:
            input_type = values.get("type", "").lower()
            labelled = (
                self.label_depth > 0
                or bool(values.get("aria-label"))
                or bool(values.get("aria-labelledby"))
            )
            if input_type != "hidden" and not labelled:
                self.unlabeled_controls.append(tag)
        if tag == "details" and "open" in values:
            self.open_details_count += 1
        if tag == "link" and values.get("rel") == "canonical":
            self.canonical = values.get("href")
        if tag == "meta" and values.get("property", "").startswith("og:"):
            self.og[values["property"]] = values.get("content", "")
        if tag == "link" and values.get("rel") in {"icon", "apple-touch-icon"}:
            if values.get("href") == "/assets/logo/shardloom-favicon.png":
                self.favicon_seen = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self.in_title = False
        if tag == "label" and self.label_depth:
            self.label_depth -= 1

    def handle_data(self, data: str) -> None:
        if self.in_title:
            self.title_parts.append(data)

    @property
    def title(self) -> str:
        return " ".join(part.strip() for part in self.title_parts if part.strip()).strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=Path("target/website-readiness-report.json"))
    return parser.parse_args()


def rel(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def site_path_from_url(value: str) -> str | None:
    if value.startswith("https://shardloom.io/"):
        return urlsplit(value).path.strip("/")
    if value.startswith("/"):
        return urlsplit(value).path.strip("/")
    return None


def expected_canonical_url(relative: str) -> str:
    if relative == "index.html":
        return "https://shardloom.io/"
    if relative.endswith("/index.html"):
        canonical_path = relative.removesuffix("/index.html")
    elif relative.endswith(".html"):
        canonical_path = relative.removesuffix(".html")
    else:
        canonical_path = relative
    return f"https://shardloom.io/{canonical_path}".rstrip("/")


def runtime_files(website: Path) -> list[Path]:
    files: list[Path] = []
    for path in website.rglob("*"):
        if path.is_file() and (
            path.suffix in RUNTIME_SUFFIXES or path.name in RUNTIME_NAMES
        ):
            if path.name == "validate_static_assets.js":
                continue
            files.append(path)
    return files


def check_cloudflare_asset_sizes(website: Path, repo_root: Path, blockers: list[str]) -> None:
    for path in website.rglob("*"):
        if path.is_file() and path.name != "validate_static_assets.js":
            size = path.stat().st_size
            if size > CLOUDFLARE_STATIC_ASSET_MAX_BYTES:
                blockers.append(
                    "Cloudflare Workers static asset exceeds 25 MiB: "
                    f"{rel(path, repo_root)} ({size} bytes)"
                )


def check_duplicate_suffixed_artifacts(
    roots: list[Path],
    repo_root: Path,
    blockers: list[str],
) -> None:
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if DUPLICATE_SUFFIX_RE.search(path.name):
                blockers.append(
                    "duplicate suffixed generated artifact remains: "
                    f"{rel(path, repo_root)}"
                )


def cleanup_duplicate_suffixed_artifacts(
    roots: list[Path],
    repo_root: Path,
) -> list[str]:
    removed: list[str] = []
    for root in roots:
        if not root.exists():
            continue
        paths = sorted(
            (path for path in root.rglob("*") if DUPLICATE_SUFFIX_RE.search(path.name)),
            key=lambda path: len(path.parts),
            reverse=True,
        )
        for path in paths:
            if not path.exists():
                continue
            removed.append(rel(path, repo_root))
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
    return removed


def check_claim_phrases(text: str, label: str, blockers: list[str]) -> None:
    for pattern in [*CLAIM_PHRASES, *PACKAGE_CLAIM_PHRASES]:
        if re.search(pattern, text, re.IGNORECASE):
            blockers.append(f"{label} contains forbidden claim phrase: {pattern}")


def same_text(path_a: Path, path_b: Path) -> bool:
    return path_a.read_text(encoding="utf-8").replace("\r\n", "\n") == path_b.read_text(
        encoding="utf-8"
    ).replace("\r\n", "\n")


def check_mirrored_file(
    *,
    source: Path,
    mirror: Path,
    label: str,
    repo_root: Path,
    blockers: list[str],
) -> None:
    if not source.exists():
        blockers.append(f"missing canonical {label}: {rel(source, repo_root)}")
        return
    if not mirror.exists():
        blockers.append(f"missing mirrored {label}: {rel(mirror, repo_root)}")
        return
    if not same_text(source, mirror):
        blockers.append(
            f"{label} drift: {rel(mirror, repo_root)} does not match "
            f"{rel(source, repo_root)}"
        )


def forbidden_runtime_hosts(text: str) -> set[str]:
    hosts: set[str] = set()
    for match in URL_RE.finditer(text):
        hostname = urlsplit(match.group(0)).hostname
        if hostname in FORBIDDEN_RUNTIME_HOSTS:
            hosts.add(hostname)
    return hosts


def resolve_html_page(path: Path) -> Path:
    if path.is_dir():
        index = path / "index.html"
        if index.is_file():
            return index
    return path


def html_relative(path: Path, website: Path) -> str:
    if path.name == "index.html" and path.parent.name.endswith(".html"):
        return rel(path.parent, website)
    return rel(path, website)


def validate_html_page(
    path: Path,
    root: Path,
    website: Path,
    blockers: list[str],
    *,
    relative_override: str | None = None,
) -> None:
    html = path.read_text(encoding="utf-8")
    is_starlight = "Starlight v" in html or "starlight__sidebar" in html
    parser = HtmlRefs()
    parser.feed(html)
    relative = relative_override or html_relative(path, website)
    if parser.html_lang != "en":
        blockers.append(f"{relative} must declare html lang=en")
    if not parser.title:
        blockers.append(f"{relative} missing document title")
    elif len(parser.title) > 80:
        blockers.append(f"{relative} title is too long for share/search surfaces")
    description = parser.meta.get("description", "")
    if not description:
        blockers.append(f"{relative} missing meta description")
    elif len(description) > 220:
        blockers.append(f"{relative} meta description is too long")
    viewport = parser.meta.get("viewport", "")
    if "width=device-width" not in viewport or "initial-scale=1" not in viewport:
        blockers.append(f"{relative} missing responsive viewport metadata")
    if parser.meta.get("robots") != "index,follow":
        blockers.append(f"{relative} must keep robots=index,follow")
    if not parser.canonical:
        blockers.append(f"{relative} missing canonical URL")
    elif parser.canonical != expected_canonical_url(relative):
        blockers.append(
            f"{relative} canonical URL mismatch: expected "
            f"{expected_canonical_url(relative)}, got {parser.canonical}"
        )
    if "og:title" not in parser.og or "og:description" not in parser.og:
        blockers.append(f"{relative} missing Open Graph title/description")
    if parser.h1_count != 1:
        blockers.append(f"{relative} must contain exactly one h1; found {parser.h1_count}")
    required_landmarks = ("header", "main") if is_starlight else ("header", "main", "footer")
    for landmark in required_landmarks:
        if landmark not in parser.landmarks:
            blockers.append(f"{relative} missing {landmark} landmark")
    if not parser.favicon_seen and "/assets/logo/shardloom-favicon.png" not in html:
        blockers.append(f"{relative} missing ShardLoom favicon")
    if parser.anchor_without_href_count:
        blockers.append(f"{relative} contains anchor(s) without href")
    if parser.open_details_count and not is_starlight:
        blockers.append(f"{relative} contains details open by default")
    if not is_starlight:
        for control in parser.unlabeled_controls:
            blockers.append(f"{relative} contains unlabeled {control} control")
    if (
        relative in EXPECTED_PAGES
        and relative != "404.html"
        and not is_starlight
        and not EXPECTED_NAV_PATHS.issubset(parser.nav_links)
    ):
        missing = ", ".join(sorted(EXPECTED_NAV_PATHS - parser.nav_links))
        blockers.append(f"{relative} primary navigation missing paths: {missing}")
    for image in parser.images:
        src = image.get("src", "<unknown>")
        alt = image.get("alt")
        aria_hidden = image.get("aria-hidden") == "true"
        if alt is None:
            blockers.append(f"{relative} image missing alt text: {src}")
        elif aria_hidden and alt != "":
            blockers.append(f"{relative} decorative image must use empty alt text: {src}")
        elif not aria_hidden and not alt.strip():
            blockers.append(f"{relative} informative image has empty alt text: {src}")
        for dimension in ("width", "height"):
            raw = image.get(dimension, "")
            if not raw.isdigit() or int(raw) <= 0:
                blockers.append(f"{relative} image missing stable {dimension}: {src}")
    for asset in parser.assets:
        local = site_path_from_url(asset)
        if local and local.startswith("assets/") and not (website / local).exists():
            blockers.append(f"{relative} references missing asset: {asset}")
    redirects = (website / "_redirects").read_text(encoding="utf-8") if (website / "_redirects").exists() else ""
    for link in parser.local_links:
        local = site_path_from_url(link)
        if not local or local == "":
            continue
        expected_paths = [
            website / local,
            website / local / "index.html",
            website / f"{local}.html",
        ]
        if not any(expected.exists() for expected in expected_paths) and f"/{local}" not in redirects:
            blockers.append(f"{relative} links to unresolved local path: {link}")
    for status in STATUS_CHIP_RE.findall(html):
        value = status.strip()
        if value not in STATUS_VOCABULARY:
            blockers.append(f"{relative} has unexpected or empty status chip text: {value}")
    check_claim_phrases(html, relative, blockers)


def check_benchmark_timing_surface_dashboard(website: Path, blockers: list[str]) -> None:
    path = resolve_html_page(website / "benchmarks.html")
    if not path.exists():
        blockers.append("missing benchmark page for timing-surface validation")
        return
    html = path.read_text(encoding="utf-8")
    if "data-route-timing-surface-dashboard" not in html:
        blockers.append("benchmark page missing route timing-surface dashboard")
    card_ids = set(ROUTE_CARD_ID_RE.findall(html))
    missing_cards = sorted(set(REQUIRED_BENCHMARK_ROUTE_CARDS) - card_ids)
    if missing_cards:
        blockers.append(
            "benchmark page missing required hot-runtime route cards: "
            + ", ".join(missing_cards)
        )
    for card_id, label in REQUIRED_BENCHMARK_ROUTE_CARDS.items():
        if label not in html:
            blockers.append(f"benchmark route label missing for {card_id}: {label}")
    if "External Baseline End-to-End" not in html:
        blockers.append("benchmark page missing external baseline lane context")
    for required in sorted(REQUIRED_BENCHMARK_TIMING_SURFACE_STRINGS):
        if required not in html:
            blockers.append(f"benchmark page missing timing-surface string: {required}")
    for required in sorted(REQUIRED_BENCHMARK_STAGE_STRINGS):
        if required not in html:
            blockers.append(f"benchmark page missing stage-attribution string: {required}")
    for required in sorted(REQUIRED_BENCHMARK_ROUTE_SHARE_STRINGS):
        if required not in html:
            blockers.append(f"benchmark page missing route-share attribution string: {required}")
    for required in sorted(REQUIRED_BENCHMARK_RUNTIME_STRINGS):
        if required not in html:
            blockers.append(f"benchmark page missing runtime/claim string: {required}")
    for required in sorted(REQUIRED_BENCHMARK_ARTIFACT_STRINGS):
        if required not in html:
            blockers.append(f"benchmark page missing artifact/current-state string: {required}")
    public_front_door_ids = set(PUBLIC_FRONT_DOOR_ID_RE.findall(html))
    missing_public_front_doors = sorted(
        REQUIRED_PUBLIC_FRONT_DOOR_BENCHMARK_IDS - public_front_door_ids
    )
    if missing_public_front_doors:
        blockers.append(
            "benchmark page missing public front-door rows: "
            + ", ".join(missing_public_front_doors)
        )
    for token in sorted(REQUIRED_PUBLIC_FRONT_DOOR_HTML_TOKENS):
        if token not in html:
            blockers.append(f"benchmark page missing public front-door token: {token}")
    route_dashboard_index = html.find("data-route-timing-surface-dashboard")
    publication_index = html.find("Publication proof")
    optimization_index = html.find("Optimization direction")
    stage_index = html.find("Stage attribution")
    route_share_index = html.find("Route-share attribution")
    runtime_index = html.find("Runtime and claims")
    front_door_index = html.find("Public front doors")
    lane_index = html.find("Artifact lane availability")
    source_state_index = html.find("Prepared/native source-state coverage")
    raw_index = html.find("Raw timing tables")
    ordered_sections = [
        ("route timing dashboard", route_dashboard_index),
        ("publication proof", publication_index),
        ("optimization direction", optimization_index),
        ("route-share attribution", route_share_index),
        ("stage attribution", stage_index),
        ("runtime and claims", runtime_index),
        ("public front doors", front_door_index),
        ("artifact lane availability", lane_index),
        ("prepared/native source-state coverage", source_state_index),
        ("raw timing tables", raw_index),
    ]
    missing_sections = [label for label, index in ordered_sections if index == -1]
    if missing_sections:
        blockers.append(
            "benchmark page missing ordered section(s): " + ", ".join(missing_sections)
        )
    for (left_label, left_index), (right_label, right_index) in zip(
        ordered_sections, ordered_sections[1:]
    ):
        if left_index != -1 and right_index != -1 and left_index > right_index:
            blockers.append(
                f"benchmark page must show {right_label} after {left_label}"
            )


def check_field_guide_route_pair(website: Path, blockers: list[str]) -> None:
    alias_path = resolve_html_page(website / "field-guide.html")
    canonical_path = website / "field-guide" / "index.html"
    if not alias_path.exists() or not canonical_path.exists():
        return
    alias_html = alias_path.read_text(encoding="utf-8")
    canonical_html = canonical_path.read_text(encoding="utf-8")
    alias_parser = HtmlRefs()
    alias_parser.feed(alias_html)
    canonical_parser = HtmlRefs()
    canonical_parser.feed(canonical_html)
    if alias_parser.canonical != "https://shardloom.io/field-guide":
        blockers.append("field-guide.html must canonicalize to /field-guide")
    if canonical_parser.canonical != "https://shardloom.io/field-guide":
        blockers.append("field-guide/index.html must canonicalize to /field-guide")
    if "/field-guide" not in alias_parser.local_links:
        blockers.append("field-guide.html must link to the canonical Field Guide route")
    if "starlight__sidebar" not in canonical_html and "Starlight v" not in canonical_html:
        blockers.append("field-guide/index.html must serve the Starlight Field Guide")


def check_public_front_door_benchmark_payload(
    payload: dict[str, Any],
    blockers: list[str],
) -> None:
    if payload.get("public_front_door_benchmark_schema_version") != (
        PUBLIC_FRONT_DOOR_BENCHMARK_SCHEMA_VERSION
    ):
        blockers.append("benchmark results missing public front-door schema")
    rows = payload.get("public_front_door_benchmark_rows")
    if not isinstance(rows, list):
        blockers.append("benchmark results missing public_front_door_benchmark_rows")
        rows = []
    row_ids = {
        str(row.get("front_door_id"))
        for row in rows
        if isinstance(row, dict) and row.get("front_door_id")
    }
    missing = sorted(REQUIRED_PUBLIC_FRONT_DOOR_BENCHMARK_IDS - row_ids)
    extra = sorted(row_ids - REQUIRED_PUBLIC_FRONT_DOOR_BENCHMARK_IDS)
    if missing:
        blockers.append(
            "benchmark results missing public front-door rows: " + ", ".join(missing)
        )
    if extra:
        blockers.append(
            "benchmark results contain extra public front-door rows: " + ", ".join(extra)
        )
    if payload.get("public_front_door_benchmark_row_count") != len(rows):
        blockers.append("benchmark results public front-door row count mismatch")
    payload_ids = {
        str(item)
        for item in payload.get("public_front_door_benchmark_row_ids", [])
        if isinstance(item, str)
    }
    if payload_ids != row_ids:
        blockers.append("benchmark results public front-door row ids mismatch")

    for row in rows:
        if not isinstance(row, dict):
            blockers.append("benchmark public front-door row is not an object")
            continue
        front_door_id = str(row.get("front_door_id") or "missing")
        surface = str(row.get("public_user_surface") or "")
        if row.get("route_runtime_status") != "scoped_runtime_supported":
            blockers.append(f"{front_door_id}: public front-door runtime status drift")
        if front_door_id == "local_source_auto_prepare_vortex_front_door":
            if row.get("front_door_end_state") != "result_sink":
                blockers.append(f"{front_door_id}: public front-door end-state drift")
            if row.get("includes_query") is not True:
                blockers.append(f"{front_door_id}: public front-door query-inclusion drift")
            if ".query" not in surface or ".collect" not in surface:
                blockers.append(f"{front_door_id}: public front-door surface missing query collect")
        else:
            if row.get("front_door_end_state") != "VortexPreparedState":
                blockers.append(f"{front_door_id}: public front-door end-state drift")
            if row.get("includes_query") is not False:
                blockers.append(f"{front_door_id}: public front-door query-inclusion drift")
        if row.get("benchmark_timing_status") != "not_timing_row_route_identity_only":
            blockers.append(f"{front_door_id}: public front-door timing status drift")
        if row.get("benchmark_timing_row") is not False:
            blockers.append(f"{front_door_id}: public front-door row must not be timing")
        if row.get("fallback_attempted") is not False:
            blockers.append(f"{front_door_id}: public front-door fallback drift")
        if row.get("external_engine_invoked") is not False:
            blockers.append(f"{front_door_id}: public front-door external-engine drift")
        has_prepare_call = ".prepare_vortex" in surface or "ctx.prepare_vortex" in surface
        if not has_prepare_call or "workspace=" not in surface:
            blockers.append(f"{front_door_id}: public front-door surface missing workspace prepare")


def check_public_front_door_benchmark_manifest(
    manifest: dict[str, Any],
    blockers: list[str],
) -> None:
    if manifest.get("public_front_door_benchmark_schema_version") != (
        PUBLIC_FRONT_DOOR_BENCHMARK_SCHEMA_VERSION
    ):
        blockers.append("benchmark manifest missing public front-door schema")
    manifest_ids = {
        str(item)
        for item in manifest.get("public_front_door_benchmark_row_ids", [])
        if isinstance(item, str)
    }
    if manifest_ids != REQUIRED_PUBLIC_FRONT_DOOR_BENCHMARK_IDS:
        blockers.append("benchmark manifest public front-door row ids mismatch")
    if manifest.get("public_front_door_benchmark_row_count") != len(
        REQUIRED_PUBLIC_FRONT_DOOR_BENCHMARK_IDS
    ):
        blockers.append("benchmark manifest public front-door row count mismatch")


def check_row_admission_payload(payload: dict[str, Any], blockers: list[str]) -> None:
    admission = payload.get("published_benchmark_row_admission")
    if admission is None:
        return
    if not isinstance(admission, dict):
        blockers.append("benchmark row admission payload must be an object")
        return
    if admission.get("schema_version") != ROW_ADMISSION_MANIFEST_SCHEMA_VERSION:
        blockers.append("benchmark row admission payload schema mismatch")
    if admission.get("fallback_attempted") is not False:
        blockers.append("benchmark row admission payload fallback_attempted must be false")
    if admission.get("external_engine_invoked") is not False:
        blockers.append(
            "benchmark row admission payload external_engine_invoked must be false"
        )
    chunks = payload.get("published_benchmark_row_chunks")
    if isinstance(chunks, list) and admission.get("chunk_count") != len(chunks):
        blockers.append("benchmark row admission payload chunk_count mismatch")
    row_count = payload.get("published_benchmark_row_count")
    if isinstance(row_count, int) and admission.get("row_count") != row_count:
        blockers.append("benchmark row admission payload row_count mismatch")


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output_path = args.output if args.output.is_absolute() else repo_root / args.output
    website = repo_root / "website"
    blockers: list[str] = []
    generated_artifact_roots = [
        website,
        repo_root / "website-public",
        repo_root / "website-src/src",
        repo_root / "docs/use-cases/generated",
    ]
    duplicate_cleanup_paths: list[str] = []

    for page in EXPECTED_PAGES:
        path = website / page
        html_path = resolve_html_page(path)
        if not html_path.exists():
            blockers.append(f"missing expected page: {page}")
        else:
            validate_html_page(
                html_path,
                repo_root,
                website,
                blockers,
                relative_override=page,
            )

    for blocker in validate_runtime_promotion_evidence(repo_root=repo_root):
        blockers.append(f"runtime promotion evidence: {blocker}")

    for page in website.rglob("*.html"):
        if not page.is_file() or page.name == "validate_static_assets.js":
            continue
        validate_html_page(page, repo_root, website, blockers)

    for asset in EXPECTED_ASSETS:
        if not (website / asset).exists():
            blockers.append(f"missing expected asset: {asset}")
    check_cloudflare_asset_sizes(website, repo_root, blockers)
    check_duplicate_suffixed_artifacts(
        generated_artifact_roots,
        repo_root,
        blockers,
    )
    check_benchmark_timing_surface_dashboard(website, blockers)
    check_field_guide_route_pair(website, blockers)

    for removed in REMOVED_WEBSITE_SURFACES:
        if (website / removed).exists():
            blockers.append(f"removed public website surface still exists: {removed}")

    flow_snapshot = website / "assets/data/compute-engine-flow-reference.md"
    canonical_flow = repo_root / "docs/architecture/compute-engine-flow-reference.md"
    check_mirrored_file(
        source=canonical_flow,
        mirror=flow_snapshot,
        label="compute-flow snapshot",
        repo_root=repo_root,
        blockers=blockers,
    )
    check_mirrored_file(
        source=canonical_flow,
        mirror=repo_root / "website-public/assets/data/compute-engine-flow-reference.md",
        label="compute-flow public-dir snapshot",
        repo_root=repo_root,
        blockers=blockers,
    )

    canonical_benchmark_results = (
        repo_root / "website-public/assets/benchmarks/latest/benchmark-results.json"
    )
    canonical_benchmark_manifest = (
        repo_root / "website-public/assets/benchmarks/latest/manifest.json"
    )
    canonical_benchmark_data = repo_root / "website-public/assets/data/benchmark-evidence.json"
    for mirror in (
        website / "assets/benchmarks/latest/benchmark-results.json",
        website / "assets/data/benchmark-evidence.json",
        repo_root / "website-src/src/data/benchmark-evidence.json",
    ):
        check_mirrored_file(
            source=canonical_benchmark_results,
            mirror=mirror,
            label="benchmark evidence bundle",
            repo_root=repo_root,
            blockers=blockers,
        )
    if canonical_benchmark_results.exists():
        benchmark_payload = json.loads(canonical_benchmark_results.read_text(encoding="utf-8"))
        optimization_report = build_optimization_target_report(canonical_benchmark_results)
        if optimization_report.get("status") != "passed":
            blockers.append("benchmark optimization target report must pass")
        if optimization_report.get("release_blocking_target_count") != 0:
            blockers.append("benchmark optimization targets must not emit release-blocking targets")
        if (
            optimization_report.get("target_disappearance_policy")
            != "diagnostic_absent_or_retired_not_release_blocker"
        ):
            blockers.append("benchmark optimization target disappearance policy drifted")
        check_public_front_door_benchmark_payload(benchmark_payload, blockers)
        check_row_admission_payload(benchmark_payload, blockers)
        if benchmark_payload.get("published_benchmark_rows_inlined") != "summary_only":
            blockers.append("benchmark results must inline only summary rows for deployable asset safety")
        chunks = benchmark_payload.get("published_benchmark_row_chunks")
        if not isinstance(chunks, list) or not chunks:
            blockers.append("benchmark results missing published_benchmark_row_chunks")
        else:
            for chunk in chunks:
                if not isinstance(chunk, dict) or not chunk.get("path"):
                    blockers.append("benchmark row chunk entry missing path")
                    continue
                chunk_path = repo_root / str(chunk["path"])
                if not chunk_path.exists():
                    blockers.append(f"missing benchmark row chunk: {rel(chunk_path, repo_root)}")
                elif chunk_path.stat().st_size > CLOUDFLARE_STATIC_ASSET_MAX_BYTES:
                    blockers.append(
                        "benchmark row chunk exceeds Cloudflare asset limit: "
                        f"{rel(chunk_path, repo_root)}"
                    )
                elif chunk.get("sha256"):
                    digest = hashlib.sha256(chunk_path.read_bytes()).hexdigest()
                    if digest != chunk.get("sha256"):
                        blockers.append(
                            "benchmark row chunk sha256 mismatch: "
                            f"{rel(chunk_path, repo_root)}"
                        )
    check_mirrored_file(
        source=canonical_benchmark_results,
        mirror=canonical_benchmark_data,
        label="benchmark public-dir data snapshot",
        repo_root=repo_root,
        blockers=blockers,
    )
    for mirror in (
        website / "assets/benchmarks/latest/manifest.json",
        repo_root / "website-src/src/data/benchmark-manifest.json",
    ):
        check_mirrored_file(
            source=canonical_benchmark_manifest,
            mirror=mirror,
            label="benchmark manifest bundle",
            repo_root=repo_root,
            blockers=blockers,
        )

    manifest_path = website / "assets/benchmarks/latest/manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("performance_claim_allowed") is not False:
            blockers.append("benchmark manifest must keep performance_claim_allowed=false")
        check_public_front_door_benchmark_manifest(manifest, blockers)
        for field in ("expected_lanes", "available_lanes", "missing_lanes"):
            if not isinstance(manifest.get(field), list):
                blockers.append(f"benchmark manifest missing list field: {field}")
        runtime_validation = manifest.get("runtime_envelope_validation")
        if not isinstance(runtime_validation, dict):
            blockers.append("benchmark manifest missing runtime_envelope_validation")
        elif runtime_validation.get("status") != "passed":
            blockers.append("benchmark manifest runtime envelope validation must pass")
    else:
        blockers.append("missing benchmark manifest")

    runtime_envelope_report = validate_runtime_envelopes(repo_root)
    if runtime_envelope_report.get("status") != "passed":
        for blocker in runtime_envelope_report.get("blockers", []):
            blockers.append(f"runtime execution envelope: {blocker}")

    redirects_path = website / "_redirects"
    if redirects_path.exists():
        redirects = redirects_path.read_text(encoding="utf-8")
        for route in EXPECTED_REDIRECTS:
            if route not in redirects:
                blockers.append(f"_redirects missing legacy route: {route}")
        html_redirects = [
            line for line in redirects.splitlines() if line.strip() and ".html" in line.split()[0]
        ]
        if not html_redirects:
            blockers.append("_redirects must canonicalize legacy .html routes")
    else:
        blockers.append("missing _redirects")

    for path in runtime_files(website):
        relative = rel(path, website)
        text = path.read_text(encoding="utf-8", errors="ignore")
        for host in sorted(forbidden_runtime_hosts(text)):
            blockers.append(f"runtime file references forbidden host {host}: {relative}")
        for snippet in sorted(FORBIDDEN_RUNTIME_SNIPPETS):
            if snippet in text:
                blockers.append(f"runtime file references active phase plan queue: {relative}")
        if "pagefind" in text.lower() and "pagefind/" not in relative:
            # Starlight's local Pagefind bundle is an approved static-search asset.
            continue

    css_path = website / "assets/site.css"
    if css_path.exists():
        css = css_path.read_text(encoding="utf-8")
        for required in [
            ":focus-visible",
            "@media (prefers-reduced-motion: reduce)",
            ".status-chip",
            ".filter-count",
        ]:
            if required not in css:
                blockers.append(f"site CSS missing accessibility/readiness marker: {required}")
    js_path = website / "assets/site.js"
    if js_path.exists():
        js = js_path.read_text(encoding="utf-8")
        if "addEventListener" not in js or "[data-filter-scope]" not in js:
            blockers.append("site JS must preserve static filter behavior")

    report: dict[str, Any] = {
        "schema_version": "shardloom.website_readiness.v3",
        "checked_pages": EXPECTED_PAGES,
        "checked_assets": EXPECTED_ASSETS,
        "checked_nav_paths": sorted(EXPECTED_NAV_PATHS),
        "status_vocabulary": sorted(STATUS_VOCABULARY),
        "benchmark_hot_runtime_route_cards_checked": sorted(REQUIRED_BENCHMARK_ROUTE_CARDS),
        "benchmark_timing_surface_strings_checked": sorted(
            REQUIRED_BENCHMARK_TIMING_SURFACE_STRINGS
        ),
        "benchmark_stage_strings_checked": sorted(REQUIRED_BENCHMARK_STAGE_STRINGS),
        "benchmark_route_share_strings_checked": sorted(
            REQUIRED_BENCHMARK_ROUTE_SHARE_STRINGS
        ),
        "benchmark_runtime_strings_checked": sorted(REQUIRED_BENCHMARK_RUNTIME_STRINGS),
        "benchmark_artifact_strings_checked": sorted(REQUIRED_BENCHMARK_ARTIFACT_STRINGS),
        "public_front_door_benchmark_ids_checked": sorted(
            REQUIRED_PUBLIC_FRONT_DOOR_BENCHMARK_IDS
        ),
        "duplicate_suffixed_generated_artifacts_removed": sorted(
            set(duplicate_cleanup_paths)
        ),
        "blockers": blockers,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if blockers:
        for blocker in blockers:
            print(f"website readiness blocker: {blocker}")
        return 1
    print(f"website readiness passed: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
