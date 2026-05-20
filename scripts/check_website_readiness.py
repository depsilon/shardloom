#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate the minimal ShardLoom public website."""

from __future__ import annotations

import argparse
import json
import re
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_PAGES = [
    "index.html",
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
]
EXPECTED_REDIRECTS = [
    "/field-guide",
    "/field-guide/*",
    "/use-cases",
    "/use-cases/*",
    "/status",
    "/readme",
    "/docs",
]
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
REMOVED_WEBSITE_SURFACES = [
    "field-guide",
    "use-cases",
    "pagefind",
]
RUNTIME_SUFFIXES = (".html", ".js", ".css", ".xml", ".txt")
RUNTIME_NAMES = {"_headers", "_redirects"}
FORBIDDEN_RUNTIME_HOSTS = {"raw.githubusercontent.com"}
URL_RE = re.compile(r"https?://[^\s\"'<>)]+")


class HtmlRefs(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.canonical: str | None = None
        self.og: dict[str, str] = {}
        self.assets: list[str] = []
        self.local_links: list[str] = []
        self.favicon_seen = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {key.lower(): value or "" for key, value in attrs}
        for key in ("href", "src", "content"):
            value = values.get(key)
            if value and "/assets/" in value:
                self.assets.append(value)
        if tag == "a" and values.get("href", "").startswith("/"):
            self.local_links.append(values["href"])
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


def site_path_from_url(value: str) -> str | None:
    if value.startswith("https://shardloom.io/"):
        return value.removeprefix("https://shardloom.io/").strip("/")
    if value.startswith("/"):
        return value.strip("/")
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
    parser = HtmlRefs()
    parser.feed(html)
    relative = rel(path, website)
    if not parser.canonical:
        blockers.append(f"{relative} missing canonical URL")
    elif parser.canonical != expected_canonical_url(relative):
        blockers.append(
            f"{relative} canonical URL mismatch: expected "
            f"{expected_canonical_url(relative)}, got {parser.canonical}"
        )
    if "og:title" not in parser.og or "og:description" not in parser.og:
        blockers.append(f"{relative} missing Open Graph title/description")
    if not parser.favicon_seen:
        blockers.append(f"{relative} missing ShardLoom favicon")
    for asset in parser.assets:
        local = site_path_from_url(asset)
        if local and local.startswith("assets/") and not (website / local).exists():
            blockers.append(f"{relative} references missing asset: {asset}")
    redirects = (website / "_redirects").read_text(encoding="utf-8") if (website / "_redirects").exists() else ""
    for link in parser.local_links:
        local = site_path_from_url(link)
        if not local or local == "":
            continue
        if local in {"benchmarks", "compute-engine-flow"}:
            expected_paths = [website / local / "index.html", website / f"{local}.html"]
        else:
            expected_paths = [website / local]
        if not any(expected.exists() for expected in expected_paths) and f"/{local}" not in redirects:
            blockers.append(f"{relative} links to unresolved local path: {link}")
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
        if "pagefind" in text.lower():
            blockers.append(f"runtime file still references Pagefind: {relative}")

    report: dict[str, Any] = {
        "schema_version": "shardloom.website_readiness.minimal.v1",
        "checked_pages": EXPECTED_PAGES,
        "checked_assets": EXPECTED_ASSETS,
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
