#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate the ShardLoom public website."""

from __future__ import annotations

import argparse
import json
import re
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from check_runtime_promotion_evidence import validate_runtime_promotion_evidence


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_PAGES = [
    "index.html",
    "start.html",
    "start/index.html",
    "field-guide.html",
    "field-guide/index.html",
    "use-cases.html",
    "use-cases/index.html",
    "benchmarks.html",
    "benchmarks/index.html",
    "architecture.html",
    "architecture/index.html",
    "compute-engine-flow.html",
    "compute-engine-flow/index.html",
    "status.html",
    "status/index.html",
    "docs.html",
    "docs/index.html",
    "404.html",
]
EXPECTED_ASSETS = [
    "assets/logo/shardloom-favicon.png",
    "assets/logo/shardloom-logo.png",
    "assets/logo/shardloom-logo-trim.png",
    "assets/site.css",
    "assets/data/compute-engine-flow-reference.md",
    "assets/data/benchmark-evidence.json",
    "assets/data/runs-today-support-matrix.json",
    "assets/data/use-case-index.json",
    "assets/benchmarks/latest/manifest.json",
    "assets/benchmarks/latest/benchmark-results.json",
    "pagefind/pagefind-entry.json",
]
EXPECTED_REDIRECTS = [
    "/readme",
    "/docs.html",
    "/can-i-use-this",
]
EXPECTED_NAV_PATHS = {
    "/",
    "/start",
    "/field-guide",
    "/use-cases",
    "/benchmarks",
    "/architecture",
    "/status",
    "/docs",
}
STATUS_VOCABULARY = {
    "runtime_supported",
    "smoke_supported",
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
    "future",
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
REMOVED_WEBSITE_SURFACES: list[str] = []
RUNTIME_SUFFIXES = (".html", ".js", ".css", ".xml", ".txt")
RUNTIME_NAMES = {"_headers", "_redirects"}
FORBIDDEN_RUNTIME_HOSTS = {"raw.githubusercontent.com"}
FORBIDDEN_RUNTIME_SNIPPETS = {"docs/architecture/phased-execution-plan.md"}
URL_RE = re.compile(r"https?://[^\s\"'<>)]+")
STATUS_CHIP_RE = re.compile(r'<span class="status-chip[^"]*">([^<]+)</span>')


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


def check_claim_phrases(text: str, label: str, blockers: list[str]) -> None:
    for pattern in [*CLAIM_PHRASES, *PACKAGE_CLAIM_PHRASES]:
        if re.search(pattern, text, re.IGNORECASE):
            blockers.append(f"{label} contains forbidden claim phrase: {pattern}")


def forbidden_runtime_hosts(text: str) -> set[str]:
    hosts: set[str] = set()
    for match in URL_RE.finditer(text):
        hostname = urlsplit(match.group(0)).hostname
        if hostname in FORBIDDEN_RUNTIME_HOSTS:
            hosts.add(hostname)
    return hosts


def validate_html_page(path: Path, root: Path, website: Path, blockers: list[str]) -> None:
    html = path.read_text(encoding="utf-8")
    is_starlight = "Starlight v" in html or "starlight__sidebar" in html
    parser = HtmlRefs()
    parser.feed(html)
    relative = rel(path, website)
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
    if relative in EXPECTED_PAGES and relative != "404.html" and not EXPECTED_NAV_PATHS.issubset(parser.nav_links):
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


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output_path = args.output if args.output.is_absolute() else repo_root / args.output
    website = repo_root / "website"
    blockers: list[str] = []

    for page in EXPECTED_PAGES:
        path = website / page
        if not path.exists():
            blockers.append(f"missing expected page: {page}")
        else:
            validate_html_page(path, repo_root, website, blockers)

    for blocker in validate_runtime_promotion_evidence(repo_root=repo_root):
        blockers.append(f"runtime promotion evidence: {blocker}")

    for page in website.rglob("*.html"):
        if page.name == "validate_static_assets.js":
            continue
        validate_html_page(page, repo_root, website, blockers)

    for asset in EXPECTED_ASSETS:
        if not (website / asset).exists():
            blockers.append(f"missing expected asset: {asset}")

    for removed in REMOVED_WEBSITE_SURFACES:
        if (website / removed).exists():
            blockers.append(f"removed public website surface still exists: {removed}")

    flow_snapshot = website / "assets/data/compute-engine-flow-reference.md"
    canonical_flow = repo_root / "docs/architecture/compute-engine-flow-reference.md"
    if flow_snapshot.exists() and canonical_flow.exists():
        if flow_snapshot.read_text(encoding="utf-8").replace("\r\n", "\n") != canonical_flow.read_text(
            encoding="utf-8"
        ).replace("\r\n", "\n"):
            blockers.append("website compute-flow snapshot does not match canonical architecture doc")

    manifest_path = website / "assets/benchmarks/latest/manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("performance_claim_allowed") is not False:
            blockers.append("benchmark manifest must keep performance_claim_allowed=false")
        for field in ("expected_lanes", "available_lanes", "missing_lanes"):
            if not isinstance(manifest.get(field), list):
                blockers.append(f"benchmark manifest missing list field: {field}")
    else:
        blockers.append("missing benchmark manifest")

    runs_today_path = website / "assets/data/runs-today-support-matrix.json"
    if runs_today_path.exists():
        runs_today = json.loads(runs_today_path.read_text(encoding="utf-8"))
        expected_states = [
            "executable",
            "feature_gated",
            "diagnostic_only",
            "report_only",
            "blocked",
            "future",
        ]
        if runs_today.get("schema_version") != "shardloom.runs_today_support_matrix.v1":
            blockers.append("runs-today matrix has unexpected schema version")
        if runs_today.get("support_state_vocabulary") != expected_states:
            blockers.append("runs-today matrix support-state vocabulary drifted")
        if runs_today.get("all_rows_no_fallback_no_external_engine") is not True:
            blockers.append("runs-today matrix must prove no fallback and no external engine rows")
        if runs_today.get("performance_claim_allowed") is not False:
            blockers.append("runs-today matrix must keep performance_claim_allowed=false")
        rows = runs_today.get("rows")
        if not isinstance(rows, list) or len(rows) < 20:
            blockers.append("runs-today matrix must expose at least 20 support rows")
        else:
            families = {row.get("family") for row in rows}
            required_families = {
                "cli_command",
                "python_api",
                "input_format",
                "output_format",
                "execution_mode",
                "claim_state",
            }
            if not required_families.issubset(families):
                blockers.append("runs-today matrix missing required support-row families")
            for row in rows:
                if row.get("fallback_attempted") is not False:
                    blockers.append(f"runs-today row reports fallback_attempted: {row.get('id')}")
                if row.get("external_engine_invoked") is not False:
                    blockers.append(
                        f"runs-today row reports external_engine_invoked: {row.get('id')}"
                    )
    else:
        blockers.append("missing runs-today support matrix")

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
