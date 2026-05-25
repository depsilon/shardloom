#!/usr/bin/env python
# SPDX-License-Identifier: Apache-2.0
"""Validate Use Case Atlas generated pages and reference backlink ledger."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

from check_use_case_index import INDEX_PATH, REPO_ROOT, load_index, validate_index


VAGUE_REFERENCE_PATTERN = re.compile(
    r"\bsee (?:the )?(?:docs|documentation)\b",
    re.IGNORECASE,
)


def values(use_case: dict[str, object], field: str) -> list[str]:
    value = use_case.get(field)
    if isinstance(value, list):
        return [str(item) for item in value]
    if value is None:
        return []
    return [str(value)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--index", type=Path, default=INDEX_PATH)
    return parser.parse_args()


def load_json_entries(path: Path, blockers: list[str], label: str) -> list[dict[str, object]]:
    if not path.exists():
        try:
            display_path = path.relative_to(REPO_ROOT).as_posix()
        except ValueError:
            display_path = path.as_posix()
        blockers.append(f"missing {display_path}")
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        blockers.append(f"{label} is not valid JSON: {exc}")
        return []
    entries = data.get("entries") if isinstance(data, dict) else data
    if not isinstance(entries, list):
        blockers.append(f"{label} must be a list or object with entries")
        return []
    return [entry for entry in entries if isinstance(entry, dict)]


def generated_html_page(root: Path, *parts: str) -> Path:
    directory_page = root.joinpath(*parts, "index.html")
    if directory_page.exists():
        return directory_page
    html_page = root.joinpath(*parts).with_suffix(".html")
    return html_page


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    index_path = args.index if args.index.is_absolute() else repo_root / args.index
    data = load_index(index_path)
    blockers = validate_index(data, repo_root)
    backlinks = repo_root / "docs" / "use-cases" / "reference-backlinks.md"
    generated = repo_root / "docs" / "use-cases" / "generated"
    website_use_cases = repo_root / "website" / "use-cases"
    field_guide_index = repo_root / "website-src" / "src" / "data" / "field-guide.json"

    backlink_text = backlinks.read_text(encoding="utf-8") if backlinks.exists() else ""
    if not backlink_text:
        blockers.append("missing docs/use-cases/reference-backlinks.md")

    field_guide_terms_by_use_case: dict[str, list[tuple[str, str]]] = defaultdict(list)
    field_guide_entries = load_json_entries(field_guide_index, blockers, "Field Guide data")
    for entry in field_guide_entries:
        slug = str(entry.get("slug") or "")
        title = str(entry.get("title") or slug)
        for related_use_case in values(entry, "related_use_cases"):
            if slug:
                field_guide_terms_by_use_case[related_use_case].append((slug, title))

    for entry in field_guide_entries:
        slug = str(entry.get("slug") or "")
        if not slug:
            blockers.append("Field Guide entry missing slug")
            continue
        references = values(entry, "references")
        if not references:
            blockers.append(f"Field Guide entry missing reference files: {slug}")
        page = generated_html_page(repo_root / "website", "field-guide", slug)
        page_text = page.read_text(encoding="utf-8") if page.exists() else ""
        if not page_text:
            blockers.append(f"missing generated Field Guide dossier page: {slug}")
        elif 'data-citation-block="reference-files"' not in page_text:
            blockers.append(f"Field Guide dossier missing citation block: {slug}")
        elif "What this proves:" not in page_text:
            blockers.append(f"Field Guide dossier missing citation proof labels: {slug}")
        if page_text and VAGUE_REFERENCE_PATTERN.search(page_text):
            blockers.append(f"Field Guide dossier uses vague reference wording: {slug}")
        for reference in references:
            if not (repo_root / reference).exists():
                blockers.append(f"Field Guide entry {slug} reference does not exist: {reference}")
            if page_text and f"<code>{reference}</code>" not in page_text:
                blockers.append(f"Field Guide dossier {slug} missing reference: {reference}")

    for use_case in data.get("use_cases", []):
        if not isinstance(use_case, dict):
            continue
        use_case_id_value = use_case.get("id")
        if not use_case_id_value:
            title = use_case.get("title", "<untitled>")
            blockers.append(f"use case is missing id: {title}")
            continue
        use_case_id = str(use_case_id_value)
        page = generated / f"{use_case_id}.md"
        if not page.exists():
            blockers.append(f"missing generated use-case page: {page.relative_to(repo_root).as_posix()}")
            continue
        text = page.read_text(encoding="utf-8")
        if "## Reference Files" not in text:
            blockers.append(f"generated page missing Reference Files block: {use_case_id}")
        if VAGUE_REFERENCE_PATTERN.search(text):
            blockers.append(f"generated page uses vague reference wording: {use_case_id}")
        for reference in values(use_case, "references"):
            if f"`{reference}`" not in text:
                blockers.append(f"generated page {use_case_id} missing reference: {reference}")
            if f"`{reference}` - What this proves:" not in text:
                blockers.append(
                    f"generated page {use_case_id} missing citation proof for reference: {reference}"
                )
            if reference not in backlink_text:
                blockers.append(f"backlink ledger missing reference: {reference}")
        if not re.search(rf"\b{re.escape(use_case_id)}\b", backlink_text):
            blockers.append(f"backlink ledger missing use case id: {use_case_id}")
        related_terms = field_guide_terms_by_use_case.get(use_case_id, [])
        if not related_terms:
            blockers.append(f"use case has no related Field Guide terms: {use_case_id}")
        if "## Related Field Guide Terms" not in text:
            blockers.append(f"generated page missing Related Field Guide Terms block: {use_case_id}")
        website_page = generated_html_page(website_use_cases, use_case_id)
        website_text = website_page.read_text(encoding="utf-8") if website_page.exists() else ""
        if not website_text:
            blockers.append(f"missing generated website use-case page: {use_case_id}")
        elif "Related Field Guide Terms" not in website_text:
            blockers.append(f"website use-case page missing Related Field Guide Terms block: {use_case_id}")
        elif 'data-citation-block="reference-files"' not in website_text:
            blockers.append(f"website use-case page missing citation block: {use_case_id}")
        elif "What this proves:" not in website_text:
            blockers.append(f"website use-case page missing citation proof labels: {use_case_id}")
        if website_text and VAGUE_REFERENCE_PATTERN.search(website_text):
            blockers.append(f"website use-case page uses vague reference wording: {use_case_id}")
        for slug, title in related_terms:
            markdown_ref = f"`website/field-guide/{slug}.html`"
            if markdown_ref not in text:
                blockers.append(
                    f"generated page {use_case_id} missing Field Guide term link: {slug}"
                )
            if website_text and f'href="/field-guide/{slug}"' not in website_text:
                blockers.append(
                    f"website page {use_case_id} missing Field Guide term link: {slug}"
                )

    if blockers:
        print("use-case backlink validation failed:", file=sys.stderr)
        for blocker in blockers:
            print(f"- {blocker}", file=sys.stderr)
        return 1
    print("use-case backlinks ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
