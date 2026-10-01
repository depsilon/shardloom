#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Resolve generated-site links, fragments, assets, and Cloudflare redirects offline."""

from __future__ import annotations

from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit

ORIGIN = "https://shardloom.io"


class PageLinks(HTMLParser):
    def __init__(self, html: str) -> None:
        super().__init__()
        self.ids: set[str] = set()
        self.references: list[tuple[str, str]] = []
        self.canonical: str | None = None
        self.feed(html)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        if values.get("id"):
            self.ids.add(values["id"])
        if tag == "a" and values.get("name"):
            self.ids.add(values["name"])
        if tag in {"a", "link"} and values.get("href"):
            self.references.append(("link", values["href"]))
        if tag in {"script", "img", "source", "video", "audio", "iframe"} and values.get("src"):
            self.references.append(("asset", values["src"]))
        if tag == "link" and values.get("rel") == "canonical":
            self.canonical = values.get("href")


def read_redirects(root: Path) -> list[tuple[str, str]]:
    path = root / "_redirects"
    if not path.exists():
        return []
    return [
        (parts[0], parts[1])
        for line in path.read_text(encoding="utf-8").splitlines()
        if (parts := line.split()) and not parts[0].startswith("#") and len(parts) >= 2
    ]


def redirect_target(path: str, redirects: list[tuple[str, str]]) -> str | None:
    for source, target in redirects:
        if path == source:
            return target
        if "*" in source:
            prefix, suffix = source.split("*", 1)
            if path.startswith(prefix) and path.endswith(suffix):
                end = len(path) - len(suffix) if suffix else len(path)
                return target.replace(":splat", path[len(prefix):end])
    return None


def resolve_reference(
    root: Path, value: str, base: str, redirects: list[tuple[str, str]]
) -> tuple[Path, str] | str | None:
    url = urljoin(base, value)
    seen: set[str] = set()
    for _ in range(16):
        parts = urlsplit(url)
        if parts.scheme not in {"http", "https"} or parts.netloc != "shardloom.io":
            return None
        if url in seen:
            return f"redirect cycle: {value}"
        seen.add(url)
        target = redirect_target(parts.path, redirects)
        if target is None:
            break
        url = urljoin(ORIGIN, target)
        if "#" not in target and parts.fragment:
            url += "#" + parts.fragment
    else:
        return f"redirect chain too long: {value}"
    local = (root / unquote(parts.path).lstrip("/")).resolve()
    if not local.is_relative_to(root.resolve()):
        return f"path escapes site root: {value}"
    candidates = [local, local / "index.html", Path(str(local) + ".html")]
    for candidate in candidates:
        if candidate.is_file():
            return candidate, unquote(parts.fragment)
    return f"missing target: {value}"


def check_site_links(root: Path) -> dict:
    root = root.resolve()
    redirects = read_redirects(root)
    pages = {
        page: PageLinks(page.read_text(encoding="utf-8"))
        for page in sorted(root.rglob("*.html"))
    }
    blockers: list[str] = []
    references_checked = 0
    external_links: set[str] = set()

    def check(value: str, base: str, label: str) -> None:
        nonlocal references_checked
        result = resolve_reference(root, value, base, redirects)
        if result is None:
            absolute = urljoin(base, value)
            if absolute.startswith(("http://", "https://")):
                external_links.add(absolute)
            return
        references_checked += 1
        if isinstance(result, str):
            blockers.append(f"{label}: {result}")
            return
        target, fragment = result
        if fragment and target.suffix == ".html":
            document = pages.get(target)
            if document is None:
                document = PageLinks(target.read_text(encoding="utf-8"))
            if fragment not in document.ids:
                blockers.append(f"{label}: missing fragment #{fragment} in {target.relative_to(root)}")

    for page, document in pages.items():
        relative = page.relative_to(root).as_posix()
        route = "/" if relative == "index.html" else "/" + relative.removesuffix("/index.html")
        base = document.canonical or ORIGIN + route
        for kind, value in document.references:
            check(value, base, f"{relative} {kind} {value}")
    for source, target in redirects:
        if "*" not in source:
            check(target, ORIGIN + "/", f"redirect {source}")

    return {
        "html_pages_checked": len(pages),
        "local_references_checked": references_checked,
        "redirect_rules_checked": sum("*" not in source for source, _ in redirects),
        "external_links": sorted(external_links),
        "blockers": sorted(set(blockers)),
    }
