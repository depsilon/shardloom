#!/usr/bin/env python
"""Generate committed static website pages from repo docs and local evidence.

This is a local maintainer helper. Cloudflare still serves committed static
files from website/ and does not run this script during deployment.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import platform
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
WEBSITE = ROOT / "website"
DATA_DIR = WEBSITE / "assets" / "data"
BENCHMARK_LATEST_DIR = WEBSITE / "assets" / "benchmarks" / "latest"
DOC_USE_CASES = ROOT / "docs" / "use-cases"
USE_CASE_PAGES = WEBSITE / "use-cases"
FIELD_GUIDE_INDEX_PATH = WEBSITE / "content" / "field-guide-index.json"
PAGEFIND_HEAD = (
    '<link href="/pagefind/pagefind-component-ui.css" rel="stylesheet">\n'
    '  <script src="/pagefind/pagefind-component-ui.js" type="module"></script>'
)
COMPATIBILITY_SCOREBOARD_DATA = (
    ROOT / "docs" / "architecture" / "universal-compatibility-coverage-scoreboard.json"
)
PACKAGE_CHANNEL_MATRIX_DATA = ROOT / "docs" / "release" / "package-channel-readiness-matrix.json"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
from benchmarks.traditional_analytics.benchmark_registry import (  # noqa: E402
    MANIFEST_SCHEMA_VERSION,
    PROFILES,
    expected_lanes_for_profile,
    lane_required_for_profile,
)
from check_use_case_index import load_index  # noqa: E402


SITE_LASTMOD = "2026-05-17"


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def pagefind_filter_spans(filters: dict[str, Any] | None) -> str:
    if not filters:
        return ""
    spans: list[str] = []
    for key, raw_values in filters.items():
        values = raw_values if isinstance(raw_values, list) else [raw_values]
        for value in values:
            if value is None or str(value) == "":
                continue
            spans.append(
                '<span class="sr-only" data-pagefind-ignore '
                f'data-pagefind-filter="{esc(key)}">{esc(value)}</span>'
            )
    if not spans:
        return ""
    return "\n    " + "\n    ".join(spans)


def status_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return esc(value)


def slug(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "-", value.lower()).strip("-")
    return text or "section"


def normalize_link(href: str) -> str:
    if re.match(r"^(https?:|mailto:|#|/)", href):
        return href
    path = href.removeprefix("./")
    target = "tree" if path.endswith("/") else "blob"
    return f"https://github.com/depsilon/shardloom/{target}/main/{path}"


CITATION_PROOF_BY_PATH = {
    "README.md": "Public technical-preview posture, Vortex-first/no-fallback positioning, and primary repo entrypoints.",
    "AGENTS.md": "Repository execution guardrails for no fallback, external-baseline-only treatment, and claim-safe work.",
    "python/README.md": "Python wrapper posture, local smoke usage, and Python API claim boundaries.",
    "SECURITY.md": "Security reporting and pre-release security posture.",
    "NOTICE": "Third-party notice and generated website asset attribution posture.",
    "docs/architecture/compute-engine-flow-reference.md": "Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.",
    "docs/architecture/phased-execution-plan.md": "Active planned work, claim boundaries, non-goals, and ledger move rules.",
    "docs/architecture/global-architecture-review.md": "Repo-versus-RFC review posture and remaining unsupported or report-only surfaces.",
    "docs/architecture/benchmark-suite-catalog.md": "Benchmark scenario families and evidence coverage expectations.",
    "docs/architecture/canonical-terminology.md": "Canonical terminology for support states, execution modes, and claim language.",
    "docs/architecture/performance-attribution-and-execution-structure.md": "Timing attribution model that separates import, preparation, scan, compute, sink, and evidence costs.",
    "docs/architecture/in-process-session-runtime.md": "Scoped session-runtime model, state reuse posture, and non-daemon boundaries.",
    "docs/architecture/io-reuse-and-fanout-architecture.md": "SourceState, VortexPreparedState, OutputPlan, fanout, and reuse evidence contracts.",
    "docs/architecture/object-store-request-planner.md": "Object-store request planning posture and blocked/runtime admission boundaries.",
    "docs/architecture/operational-evidence-policy-hardening.md": "Evidence policy rules that keep unsupported paths explicit and claim gates closed.",
    "docs/architecture/universal-compatibility-coverage-scoreboard.md": "Compatibility scoreboard status and source/sink support boundaries.",
    "docs/architecture/universal-input-contract.md": "Universal input contract posture and unsupported input-family diagnostics.",
    "docs/architecture/vortex-scan-pushdown-completion.md": "Vortex scan pushdown evidence fields, blockers, and scoped support boundaries.",
    "docs/architecture/runtime-evidence-level-tiering.md": "Evidence-level distinctions for minimal runtime, certified, and full replay paths.",
    "docs/architecture/fused-operator-pipeline.md": "Fused-pipeline plan, supported operator families, and deterministic blocker posture.",
    "docs/architecture/compressed-encoded-kernel-registry.md": "Encoding-specific kernel registry plan and encoded-native claim gates.",
    "docs/architecture/live-hybrid-fabric-freshness-gate.md": "Live/hybrid fabric report-only posture, freshness evidence, and non-production boundary.",
    "docs/architecture/adoption-commercial-readiness-friction-reduction.md": "Commercial-readiness friction plan and package/channel adoption boundaries.",
    "docs/benchmarks/local-taxonomy-benchmark.md": "Local benchmark taxonomy, evidence rows, and workload-scoped interpretation boundaries.",
    "docs/benchmarks/baseline-comparison-boundary.md": "Benchmark comparison boundaries and external-baseline-only policy.",
    "docs/benchmarks/static-benchmark-publishing-runbook.md": "Static benchmark artifact publishing workflow and completeness gate posture.",
    "docs/getting-started/first-10-minutes.md": "Shortest local orientation path for smoke checks and evidence inspection.",
    "docs/getting-started/examples.md": "Current example catalog and local workflow entrypoints.",
    "docs/getting-started/certified-local-workload.md": "Scoped certified local workload path and expected evidence fields.",
    "docs/getting-started/install.md": "Installation posture and package-channel caveats for technical-preview users.",
    "docs/foundry/proof-of-use-certification.md": "Foundry-style local proof boundary and no-production-Foundry claim posture.",
    "docs/foundry/integration-pack-readiness.md": "Foundry integration-pack readiness posture and unresolved proof requirements.",
    "docs/release/public-technical-preview-readiness.md": "Public technical-preview readiness checks and claim-safety posture.",
    "docs/release/hard-release-readiness-gate.md": "Release gate requirements for package publication and public claims.",
    "docs/release/known-unsupported-paths.md": "Known unsupported paths and deterministic blocker expectations.",
    "website/benchmarks.html": "Rendered benchmark evidence interpretation and no-leaderboard public framing.",
    "website/assets/benchmarks/latest/manifest.json": "Static benchmark manifest with expected lanes, availability, environment, and claim boundary.",
    "benchmarks/traditional_analytics/README.md": "Traditional analytics benchmark commands, scenarios, external baselines, and evidence interpretation.",
    "benchmarks/traditional_analytics/benchmark_registry.py": "Benchmark lane/profile registry and external-baseline-only policy metadata.",
    "docs/skills/vortex/vortex-file-io.md": "Vortex file I/O skill notes used to ground native Vortex artifact handling.",
    "docs/skills/vortex/vortex-scan-api.md": "Vortex Scan API skill notes used to ground scan and pushdown terminology.",
    "docs/skills/encoded-execution.md": "Encoded execution skill notes and encoded-native claim boundaries.",
    "docs/use-cases/README.md": "Use Case Atlas overview, audience posture, and non-expert capability grouping.",
    "docs/use-cases/use-case-index.yml": "Machine-readable use-case status, evidence, examples, and claim-boundary index.",
    "docs/use-cases/reference-backlinks.md": "Backlink ledger mapping source-of-truth references to related use cases.",
    "docs/use-cases/recipes/README.md": "Non-expert recipe library and claim-safe workflow examples.",
}


def citation_proof(reference: str) -> str:
    path = reference.strip()
    normalized = path.rstrip("/")
    if normalized in CITATION_PROOF_BY_PATH:
        return CITATION_PROOF_BY_PATH[normalized]
    if normalized.startswith("docs/rfcs/"):
        return "Governing RFC for the feature contract, evidence requirements, and claim boundary."
    if normalized.startswith("examples/"):
        return "Runnable or blocked example posture, expected local command path, and claim boundary."
    if normalized.startswith("docs/use-cases/generated/"):
        return "Generated use-case page produced from the machine-readable use-case index."
    if normalized.startswith("target/"):
        return "Local generated evidence artifact path used for smoke or benchmark verification."
    if normalized.startswith("website/field-guide/"):
        return "Generated Field Guide dossier that explains the linked concept and its current support boundary."
    if normalized.startswith("website/use-cases/"):
        return "Generated Use Case Atlas page that states status, evidence, examples, and claim boundary."
    return "Source-of-truth file for the referenced capability posture, evidence fields, or claim boundary."


def render_citation_links(references: list[str]) -> str:
    if not references:
        return "<p>No source reference attached yet.</p>"
    cards = []
    for reference in references:
        href = normalize_link(reference)
        cards.append(
            '<article class="citation-card">'
            f'<a href="{esc(href)}"><code>{esc(reference)}</code></a>'
            f'<p><strong>What this proves:</strong> {esc(citation_proof(reference))}</p>'
            "</article>"
        )
    return '<div class="citation-list" data-citation-block="reference-files">' + "".join(cards) + "</div>"


def repo_relative_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


def display_path(path: Path) -> str:
    try:
        return repo_relative_path(path)
    except OSError:
        return path.name


def load_json_file(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def resolve_artifact_path(path_text: str, manifest_path: Path) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    root_candidate = ROOT / path
    if root_candidate.exists():
        return root_candidate
    return manifest_path.parent / path


def inline_markdown(value: str) -> str:
    text = esc(value)
    text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(
        r"\[([^\]]+)\]\(([^)]+)\)",
        lambda match: (
            f'<a href="{esc(normalize_link(match.group(2)))}">{match.group(1)}</a>'
        ),
        text,
    )
    return text


def split_table_row(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def render_table(lines: list[str]) -> str:
    rows = [split_table_row(line) for line in lines if line.strip().startswith("|")]
    if len(rows) < 2:
        return ""
    headers = rows[0]
    body = rows[2:]
    output = ['<div class="table-scroll"><table>']
    output.append(
        "<thead><tr>"
        + "".join(f"<th>{inline_markdown(cell)}</th>" for cell in headers)
        + "</tr></thead>"
    )
    output.append("<tbody>")
    for row in body:
        output.append(
            "<tr>"
            + "".join(f"<td>{inline_markdown(cell)}</td>" for cell in row)
            + "</tr>"
        )
    output.append("</tbody></table></div>")
    return "\n".join(output)


def strip_inline_markup(value: str) -> str:
    text = html.unescape(value)
    text = re.sub(r"<br\s*/?>", " - ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def mermaid_node_parts(label: str) -> tuple[str, str]:
    parts = [
        strip_inline_markup(part)
        for part in re.split(r"<br\s*/?>", html.unescape(label), flags=re.IGNORECASE)
        if strip_inline_markup(part)
    ]
    if not parts:
        return ("Unnamed node", "")
    return (parts[0], " - ".join(parts[1:]))


def render_mermaid_static(code: str) -> str:
    """Render a readable static substitute for Mermaid in no-build environments."""

    node_pattern = re.compile(r'^\s*([A-Za-z0-9_]+)\s*[\[{]\s*"([^"]+)"\s*[\]}]', re.MULTILINE)
    nodes: dict[str, tuple[str, str]] = {
        match.group(1): mermaid_node_parts(match.group(2))
        for match in node_pattern.finditer(code)
    }

    groups: list[tuple[str, list[str]]] = []
    active_group: tuple[str, list[str]] | None = None
    subgraph_pattern = re.compile(r'^\s*subgraph\s+[A-Za-z0-9_]+\s*\["([^"]+)"\]')
    node_id_pattern = re.compile(r"^\s*([A-Za-z0-9_]+)\s*[\[{]")

    for line in code.splitlines():
        subgraph = subgraph_pattern.match(line)
        if subgraph:
            active_group = (strip_inline_markup(subgraph.group(1)), [])
            groups.append(active_group)
            continue
        if active_group is not None and line.strip() == "end":
            active_group = None
            continue
        if active_group is not None:
            node = node_id_pattern.match(line)
            if node and node.group(1) in nodes and node.group(1) not in active_group[1]:
                active_group[1].append(node.group(1))

    grouped_ids = {node_id for _, node_ids in groups for node_id in node_ids}
    ungrouped = [node_id for node_id in nodes if node_id not in grouped_ids]
    if ungrouped:
        groups.append(("Flow nodes", ungrouped))

    edge_pattern = re.compile(
        r'^\s*([A-Za-z0-9_]+)\s*(?:[-.=]+>)\s*(?:\|"?([^|"]+)"?\|\s*)?([A-Za-z0-9_]+)',
        re.MULTILINE,
    )
    edges = []
    for match in edge_pattern.finditer(code):
        source = nodes.get(match.group(1), (match.group(1), ""))[0]
        target = nodes.get(match.group(3), (match.group(3), ""))[0]
        label = strip_inline_markup(match.group(2) or "")
        edges.append((source, label, target))

    if not nodes:
        return (
            '<pre class="mermaid-source"><code data-language="mermaid">'
            + esc(code)
            + "</code></pre>"
        )

    html_parts = ['<figure class="mermaid-rendered">']
    html_parts.append(
        '<figcaption><strong>Rendered flowchart</strong><span>Static rendering from the Mermaid source in this repository.</span></figcaption>'
    )
    html_parts.append('<div class="diagram-groups">')
    for group_title, node_ids in groups:
        html_parts.append('<section class="diagram-group">')
        html_parts.append(f"<h4>{esc(group_title)}</h4>")
        html_parts.append('<ol class="diagram-node-list">')
        for node_id in node_ids:
            title, detail = nodes[node_id]
            detail_html = f"<span>{esc(detail)}</span>" if detail else ""
            html_parts.append(
                f"<li><strong>{esc(title)}</strong>{detail_html}</li>"
            )
        html_parts.append("</ol></section>")
    html_parts.append("</div>")
    if edges:
        html_parts.append('<details class="diagram-edges">')
        html_parts.append("<summary>Flow connections</summary><ol>")
        for source, label, target in edges[:80]:
            label_html = f" <em>{esc(label)}</em> " if label else " "
            html_parts.append(
                f"<li><span>{esc(source)}</span>{label_html}<span>{esc(target)}</span></li>"
            )
        html_parts.append("</ol></details>")
    html_parts.append(
        '<details class="diagram-source"><summary>Mermaid source</summary><pre><code data-language="mermaid">'
        + esc(code)
        + "</code></pre></details>"
    )
    html_parts.append("</figure>")
    return "\n".join(html_parts)


def markdown_to_html(markdown: str) -> str:
    lines = markdown.splitlines()
    output: list[str] = []
    paragraph: list[str] = []
    list_items: list[str] = []
    index = 0
    in_code = False
    code_info = ""
    code_lines: list[str] = []

    def flush_paragraph() -> None:
        nonlocal paragraph
        if paragraph:
            output.append(f"<p>{inline_markdown(' '.join(paragraph))}</p>")
            paragraph = []

    def flush_list() -> None:
        nonlocal list_items
        if list_items:
            output.append("<ul>")
            output.extend(f"<li>{inline_markdown(item)}</li>" for item in list_items)
            output.append("</ul>")
            list_items = []

    while index < len(lines):
        line = lines[index]
        stripped = line.strip()

        if stripped.startswith("```"):
            if in_code:
                code = "\n".join(code_lines)
                if code_info.lower() == "mermaid":
                    output.append(render_mermaid_static(code))
                else:
                    output.append(
                        f'<pre><code data-language="{esc(code_info)}">'
                        + esc(code)
                        + "</code></pre>"
                    )
                in_code = False
                code_info = ""
                code_lines = []
            else:
                flush_paragraph()
                flush_list()
                in_code = True
                code_info = stripped.strip("`").strip()
            index += 1
            continue

        if in_code:
            code_lines.append(line)
            index += 1
            continue

        if stripped.startswith("|") and index + 1 < len(lines) and set(
            lines[index + 1].strip().replace("|", "").replace(":", "").replace(" ", "")
        ) <= {"-"}:
            flush_paragraph()
            flush_list()
            table_lines = [line]
            index += 1
            while index < len(lines) and lines[index].strip().startswith("|"):
                table_lines.append(lines[index])
                index += 1
            output.append(render_table(table_lines))
            continue

        if not stripped:
            flush_paragraph()
            flush_list()
            index += 1
            continue

        heading = re.match(r"^(#{1,6})\s+(.*)$", stripped)
        if heading:
            flush_paragraph()
            flush_list()
            level = min(len(heading.group(1)) + 1, 6)
            text = heading.group(2).strip()
            output.append(
                f'<h{level} id="{slug(text)}">{inline_markdown(text)}</h{level}>'
            )
            index += 1
            continue

        if list_items and line[:1].isspace():
            list_items[-1] = f"{list_items[-1]} {stripped}"
            index += 1
            continue

        if stripped.startswith("- "):
            flush_paragraph()
            list_items.append(stripped[2:].strip())
            index += 1
            continue

        paragraph.append(stripped)
        index += 1

    flush_paragraph()
    flush_list()
    return "\n".join(output)


def site_nav(active: str) -> str:
    nav = [
        ("Home", "/", "home"),
        ("Field Guide", "/field-guide/", "field-guide"),
        ("Use Cases", "/use-cases/", "use-cases"),
        ("Telemetry", "/benchmarks", "telemetry"),
        ("Compute Flow", "/compute-engine-flow", "flow"),
        ("Status", "/status", "status"),
        ("Docs", "/readme", "docs"),
        ("GitHub", "https://github.com/depsilon/shardloom", "github"),
    ]
    links = []
    for label, href, key in nav:
        class_name = "active" if key == active else ""
        current_attr = ' aria-current="page"' if key == active else ""
        links.append(f'<a class="{class_name}"{current_attr} href="{href}">{label}</a>')
    return "\n".join(links)


def status_ribbon() -> str:
    return """
  <div class="status-ribbon" role="note" aria-label="Public posture">
    <div class="shell status-ribbon-inner">
      <span><strong>Pre-release</strong> evidence-first local compute foundation</span>
      <span><strong>No fallback</strong> external engines are baselines only</span>
      <span><strong>Claim gate</strong> no performance or production platform claim</span>
    </div>
  </div>
"""


def load_compatibility_scoreboard() -> dict[str, Any]:
    if not COMPATIBILITY_SCOREBOARD_DATA.exists():
        return {"rows": []}
    return json.loads(COMPATIBILITY_SCOREBOARD_DATA.read_text(encoding="utf-8"))


def load_package_channel_matrix() -> dict[str, Any]:
    if not PACKAGE_CHANNEL_MATRIX_DATA.exists():
        return {"channels": []}
    return json.loads(PACKAGE_CHANNEL_MATRIX_DATA.read_text(encoding="utf-8"))


def support_status_class(status: str) -> str:
    return status_class(status.replace("-", "_").replace(" ", "_"))


def select_filter(
    attr_name: str,
    filter_name: str,
    label: str,
    values: list[str],
) -> str:
    options = ['<option value="">All</option>'] + [
        f'<option value="{esc(value)}">{esc(status_label(value))}</option>' for value in values
    ]
    return (
        f'<label><span>{esc(label)}</span><select data-{esc(attr_name)}="{esc(filter_name)}">'
        + "".join(options)
        + "</select></label>"
    )


def render_public_status_scorecard_section(use_cases: list[dict[str, Any]]) -> str:
    scoreboard = load_compatibility_scoreboard()
    package_matrix = load_package_channel_matrix()
    status_order = [
        "runtime-supported",
        "smoke-supported",
        "report-only",
        "blocked",
        "planned",
        "not-planned",
    ]
    rows: list[dict[str, Any]] = []
    for row in scoreboard.get("rows", []):
        if not isinstance(row, dict):
            continue
        rows.append(
            {
                "label": row.get("surface"),
                "family": row.get("surface_family"),
                "status": row.get("support_status"),
                "answer": row.get("claim_boundary"),
                "evidence": (
                    f"{row.get('claim_gate_status')}; "
                    f"fallback_attempted={row.get('fallback_attempted', False)}; "
                    f"external_engine_invoked={row.get('external_engine_invoked', False)}"
                ),
                "reference": "docs/architecture/universal-compatibility-coverage-scoreboard.json",
            }
        )
    ready_channels = [
        row
        for row in package_matrix.get("channels", [])
        if isinstance(row, dict) and row.get("ready") is True
    ]
    rows.append(
        {
            "label": "Public package channels",
            "family": "release_distribution",
            "status": "runtime-supported" if package_matrix.get("status") == "ready" else "blocked",
            "answer": (
                "Public package channels have channel-specific install, uninstall, smoke, "
                "SBOM/checksum/provenance, and rollback evidence."
                if package_matrix.get("status") == "ready"
                else "Public package channels remain blocked until channel-specific install, uninstall, smoke, SBOM/checksum/provenance, and rollback evidence exists."
            ),
            "evidence": (
                f"{len(ready_channels)} ready channels; "
                "public_package_release_claim_allowed="
                f"{str(package_matrix.get('public_package_release_claim_allowed', False)).lower()}"
            ),
            "reference": "docs/release/package-channel-readiness-matrix.json",
        }
    )
    rows.extend(
        [
            {
                "label": "Enterprise evidence export pack",
                "family": "adoption_workflow",
                "status": "report-only",
                "answer": "A report-only enterprise evidence export-pack contract exists for ShardLoom JSON, OpenLineage facet payload previews, OpenTelemetry span/metric payload previews, optional Markdown summaries, and redaction reports. No exporter or backend runs by default.",
                "evidence": "shardloom.enterprise_evidence_export_pack.v1; network_calls_by_default=false; backend_integration_configured=false",
                "reference": "docs/release/enterprise-evidence-export-pack.json",
            },
            {
                "label": "Foundry dev-stack starter",
                "family": "platform_integration",
                "status": "report-only",
                "answer": "A local Foundry-style dev-stack starter exists for CLI/package resolution, staged input posture, source-free generated-output blockers, and local certificate-style evidence. It does not invoke Foundry runtime, Foundry compute, Foundry Spark, or Foundry output APIs.",
                "evidence": "shardloom.foundry_dev_stack_starter_kit.v1; foundry_runtime_invoked=false; foundry_spark_invoked=false",
                "reference": "docs/foundry/dev-stack-starter-kit.json",
            },
            {
                "label": "Workflow recipe library",
                "family": "documentation",
                "status": "report-only",
                "answer": "A claim-safe recipe library now covers common local smokes, prepared/native direction, generated-output smokes, messy-data fixtures, result-sink proof, blocked object-store diagnostics, Foundry-style local proof, and benchmark interpretation. It does not add runtime support by itself.",
                "evidence": "shardloom.workflow_recipe_library.v1; fallback_attempted=false; external_engine_invoked=false",
                "reference": "docs/use-cases/recipes/recipe-index.json",
            },
            {
                "label": "Hidden fallback engine execution",
                "family": "claim_boundary",
                "status": "not-planned",
                "answer": "ShardLoom does not plan to silently run unsupported work through Spark, DuckDB, DataFusion, Polars, pandas, or another fallback engine.",
                "evidence": "fallback_attempted=false; external_engine_invoked=false",
                "reference": "README.md",
            },
            {
                "label": "Spark-displacement claim",
                "family": "claim_boundary",
                "status": "not-planned",
                "answer": "ShardLoom is not presented as an Apache Spark substitute; external engines are baseline context only.",
                "evidence": "claim_gate_status=not_claim_grade",
                "reference": "docs/benchmarks/baseline-comparison-boundary.md",
            },
            {
                "label": "Production SQL/DataFrame, object-store, lakehouse, or Foundry claim",
                "family": "claim_boundary",
                "status": "not-planned",
                "answer": "Production claims for these surfaces are not planned without future workload-scoped runtime evidence and release gates.",
                "evidence": "public claim gates closed",
                "reference": "docs/release/known-unsupported-paths.md",
            },
        ]
    )
    counts = {status: 0 for status in status_order}
    for row in rows:
        status = str(row.get("status", "report-only"))
        counts[status] = counts.get(status, 0) + 1
    metrics = "\n".join(
        f'<div class="metric"><strong>{counts.get(status, 0)}</strong><span>{esc(status.replace("-", " "))}</span></div>'
        for status in status_order
    )
    table_rows = "\n".join(
        "<tr>"
        f"<td><strong>{esc(row.get('label'))}</strong><span>{esc(row.get('family'))}</span></td>"
        f"<td><span class=\"claim-badge {support_status_class(str(row.get('status')))}\">{esc(str(row.get('status')).replace('-', ' '))}</span></td>"
        f"<td>{esc(row.get('answer'))}</td>"
        f"<td>{esc(row.get('evidence'))}</td>"
        f"<td><code>{esc(row.get('reference'))}</code></td>"
        "</tr>"
        for row in rows
    )
    status_key = "\n".join(
        f'<article class="signal-card"><span class="claim-badge {support_status_class(status)}">{esc(status.replace("-", " "))}</span><p>{esc(description)}</p></article>'
        for status, description in [
            ("runtime-supported", "A scoped runtime path exists; claim boundaries still apply."),
            ("smoke-supported", "A narrow local or fixture smoke exists; broad support is not implied."),
            ("report-only", "The surface can be described or diagnosed without execution support."),
            ("blocked", "The surface must remain unavailable or emit deterministic blockers."),
            ("planned", "The phase plan carries a future work item, not current support."),
            ("not-planned", "The public posture intentionally does not make or pursue this claim."),
        ]
    )
    use_case_statuses = sorted({str(use_case["status"]) for use_case in use_cases})
    use_case_inputs = sorted(
        {value for use_case in use_cases for value in value_list(use_case.get("inputs"))}
    )
    use_case_outputs = sorted(
        {value for use_case in use_cases for value in value_list(use_case.get("outputs"))}
    )
    use_case_execution_modes = sorted({str(use_case["execution_mode"]) for use_case in use_cases})
    use_case_evidence_levels = sorted({evidence_level_for_use_case(use_case) for use_case in use_cases})
    use_case_platforms = sorted({platform_for_use_case(use_case) for use_case in use_cases})
    matrix_cards = []
    for use_case in use_cases:
        status = str(use_case["status"])
        use_case_id = str(use_case["id"])
        first_reference = value_list(use_case.get("references"))[0]
        matrix_cards.append(
            f'<article class="status-matrix-row" data-status="{esc(status)}" '
            f'data-inputs="{esc(" ".join(value_list(use_case.get("inputs"))))}" '
            f'data-outputs="{esc(" ".join(value_list(use_case.get("outputs"))))}" '
            f'data-execution-mode="{esc(str(use_case["execution_mode"]))}" '
            f'data-evidence-level="{esc(evidence_level_for_use_case(use_case))}" '
            f'data-platform="{esc(platform_for_use_case(use_case))}">'
            '<div class="status-matrix-primary">'
            f'<span class="claim-badge {status_class(status)}">{esc(status_label(status))}</span>'
            f'<h3><a href="/use-cases/{esc(use_case_id)}">{esc(use_case["title"])}</a></h3>'
            f'<p>{esc(use_case_plain_summary(use_case))}</p>'
            "</div>"
            '<dl class="status-matrix-details">'
            f'<dt>Capability</dt><dd>{esc(str(use_case["capability_family"]))}</dd>'
            f'<dt>Execution</dt><dd><code>{esc(str(use_case["execution_mode"]))}</code></dd>'
            f'<dt>Inputs</dt><dd>{esc(inline_csv(value_list(use_case.get("inputs"))))}</dd>'
            f'<dt>Outputs</dt><dd>{esc(inline_csv(value_list(use_case.get("outputs"))))}</dd>'
            f'<dt>Evidence</dt><dd>{esc(evidence_level_for_use_case(use_case))}</dd>'
            f'<dt>Platform</dt><dd>{esc(platform_for_use_case(use_case))}</dd>'
            f'<dt>Reference</dt><dd><code>{esc(first_reference)}</code></dd>'
            "</dl>"
            f'<p class="status-matrix-boundary">{inline_markdown(str(use_case["claim_boundary"]))}</p>'
            "</article>"
        )
    status_matrix = f"""
        <div class="terminal-panel status-matrix-panel" id="capability-status-matrix">
          <div class="section-header-row">
            <div>
              <p class="eyebrow">Capability status matrix</p>
              <h3>Filter by user question.</h3>
            </div>
            <p>Rows come from <code>docs/use-cases/use-case-index.yml</code> and link to exact use-case pages, so blocked/report-only states stay visible.</p>
          </div>
          <form class="use-case-filters status-matrix-filters" data-status-matrix-filters>
            {select_filter("status-matrix-filter", "status", "Status", use_case_statuses)}
            {select_filter("status-matrix-filter", "input", "Input type", use_case_inputs)}
            {select_filter("status-matrix-filter", "output", "Output type", use_case_outputs)}
            {select_filter("status-matrix-filter", "execution", "Execution mode", use_case_execution_modes)}
            {select_filter("status-matrix-filter", "evidence", "Evidence level", use_case_evidence_levels)}
            {select_filter("status-matrix-filter", "platform", "Platform", use_case_platforms)}
            <button type="reset">Reset</button>
          </form>
          <p class="use-case-filter-count" data-status-matrix-count>{len(use_cases)} status rows shown</p>
          <div class="status-matrix-grid" data-status-matrix-grid>{"".join(matrix_cards)}</div>
        </div>
    """
    status_matrix = "\n".join(line.rstrip() for line in status_matrix.splitlines()).strip()
    return f"""
    <section id="can-i-use-this" class="status-scorecard">
      <div class="shell">
        <p class="eyebrow">Can I use this?</p>
        <h2>Answer common capability questions in under two minutes.</h2>
        <p class="section-lede">This matrix projects the universal compatibility scoreboard, known unsupported paths, and package-channel release boundary into one buyer-facing status view. It is a maturity map, not runtime expansion.</p>
        <div class="metric-row">{metrics}</div>
        <div class="telemetry-signal-grid status-key" aria-label="Status vocabulary">
          {status_key}
        </div>
        {status_matrix}
        <div class="table-scroll">
          <table>
            <thead><tr><th>Surface or claim</th><th>Status</th><th>What this means</th><th>Evidence or blocker</th><th>Reference</th></tr></thead>
            <tbody>{table_rows}</tbody>
          </table>
        </div>
        <p class="section-note">Every supported or smoke-supported row remains scoped. Unsupported and blocked rows stay visible so users do not infer production SQL/DataFrame, object-store/lakehouse, Foundry, package-publication, performance, Spark-replacement, or fallback-engine support.</p>
      </div>
    </section>
    """.strip()


def render_compatibility_scoreboard_section() -> str:
    scoreboard = load_compatibility_scoreboard()
    rows = scoreboard.get("rows", [])
    if not isinstance(rows, list) or not rows:
        return ""
    featured_ids = {
        "csv",
        "parquet",
        "vortex",
        "generated_source_free_outputs",
        "python_rows_dataframe",
        "sql_values_literals",
        "object_store_s3_gcs_adls",
        "table_lakehouse_iceberg_delta_hudi",
        "foundry",
    }
    display_rows = [
        row
        for row in rows
        if isinstance(row, dict) and row.get("surface_id") in featured_ids
    ]
    table_rows = "\n".join(
        "<tr>"
        f"<td><strong>{esc(row.get('surface'))}</strong><span>{esc(row.get('surface_family'))}</span></td>"
        f"<td><span class=\"claim-badge {status_class(str(row.get('support_status', 'report_only')).replace('-', '_'))}\">{esc(row.get('support_status'))}</span></td>"
        f"<td>{esc(row.get('direction'))}</td>"
        f"<td>{esc(row.get('claim_gate_status'))}</td>"
        f"<td>{esc(row.get('claim_boundary'))}</td>"
        "</tr>"
        for row in display_rows
    )
    generated_contract = scoreboard.get("source_free_generated_output_contract", {})
    generated_rows = generated_contract.get("rows", [])
    generated_featured_ids = {
        "no_dataset_smoke",
        "python_ctx_from_rows",
        "python_ctx_range",
        "python_ctx_sequence",
        "python_ctx_literal_table",
        "python_ctx_calendar",
        "local_output_only_generated_source_posture",
        "sql_literal_select",
        "sql_values",
        "dataframe_generated_with_column",
    }
    generated_table_rows = "\n".join(
        "<tr>"
        f"<td><strong>{esc(row.get('row_id'))}</strong><span>{esc(row.get('user_visible_surface'))}</span></td>"
        f"<td><span class=\"claim-badge {status_class(str(row.get('support_status', 'report_only')).replace('-', '_'))}\">{esc(row.get('support_status'))}</span></td>"
        f"<td>{status_value(row.get('runtime_execution'))}</td>"
        f"<td>{status_value(row.get('output_io_performed'))}</td>"
        f"<td>{esc(row.get('claim_boundary'))}</td>"
        "</tr>"
        for row in generated_rows
        if isinstance(row, dict) and row.get("row_id") in generated_featured_ids
    )
    generated_contract_html = ""
    if generated_table_rows:
        generated_contract_html = f"""
        <div class="section-spacer"></div>
        <h3>Source-free generated-output contract</h3>
        <p class="section-lede">This compatibility projection keeps no-dataset smoke, Python generated-output smokes, scoped source-free SQL smokes, SQL/DataFrame report-only rows, and local-output-only sink posture separate. Current generated-output runtime is scoped to local JSONL smokes for <code>ctx.from_rows(...).write(...)</code>, <code>ctx.literal_table(...).write(...)</code>, <code>ctx.calendar(...).write(...)</code>, <code>ctx.range(...).write(...)</code>, <code>ctx.sequence(...).write(...)</code>, <code>ctx.sql_values(...).write(...)</code>, <code>ctx.sql_literal_select(...).write(...)</code>, and <code>ctx.sql(&quot;SELECT * FROM generate_series/range(...)&quot;).write(...)</code>.</p>
        <div class="table-scroll">
          <table>
            <thead><tr><th>Row</th><th>Status</th><th>Runtime</th><th>Output I/O</th><th>Boundary</th></tr></thead>
            <tbody>{generated_table_rows}</tbody>
          </table>
        </div>
        <p class="section-note">Generated-output summary: <code>no_dataset_smoke_separate={status_value(generated_contract.get('no_dataset_smoke_separate'))}</code>, <code>local_output_only={status_value(generated_contract.get('local_output_only'))}</code>, <code>object_store_runtime_supported={status_value(generated_contract.get('object_store_runtime_supported'))}</code>, <code>foundry_runtime_supported={status_value(generated_contract.get('foundry_runtime_supported'))}</code>, <code>broad_sql_dataframe_claim_allowed={status_value(generated_contract.get('broad_sql_dataframe_claim_allowed'))}</code>.</p>
        """
        generated_contract_html = "\n".join(
            line.rstrip() for line in generated_contract_html.splitlines()
        ).strip()
    object_store_ladder = scoreboard.get("object_store_admission_ladder", {})
    object_store_rows = object_store_ladder.get("rows", [])
    object_store_table_rows = "\n".join(
        "<tr>"
        f"<td><strong>{esc(row.get('row_id'))}</strong><span>{esc(row.get('stage'))}</span></td>"
        f"<td><span class=\"claim-badge {status_class(str(row.get('support_status', 'blocked')).replace('-', '_'))}\">{esc(row.get('support_status'))}</span></td>"
        f"<td>{esc(row.get('credential_policy_status'))}</td>"
        f"<td>{status_value(row.get('object_store_io'))}</td>"
        f"<td>{esc(row.get('claim_boundary'))}</td>"
        "</tr>"
        for row in object_store_rows
        if isinstance(row, dict)
    )
    object_store_ladder_html = ""
    if object_store_table_rows:
        object_store_ladder_html = f"""
        <div class="section-spacer"></div>
        <h3>S3/GCS/ADLS admission ladder</h3>
        <p class="section-lede">This report-only ladder separates object-store URI recognition, credential policy, public reads, authenticated reads, byte-range reads, full-file reads, local cache, write staging, and commit protocol. Every current row blocks credential resolution, provider probes, network probes, object-store I/O, writes, commits, fallback, and external engine invocation.</p>
        <div class="table-scroll">
          <table>
            <thead><tr><th>Row</th><th>Status</th><th>Credential policy</th><th>Object-store I/O</th><th>Boundary</th></tr></thead>
            <tbody>{object_store_table_rows}</tbody>
          </table>
        </div>
        <p class="section-note">Object-store summary: <code>runtime_supported={status_value(object_store_ladder.get('runtime_supported'))}</code>, <code>public_no_credential_read_supported={status_value(object_store_ladder.get('public_no_credential_read_supported'))}</code>, <code>authenticated_read_supported={status_value(object_store_ladder.get('authenticated_read_supported'))}</code>, <code>byte_range_read_supported={status_value(object_store_ladder.get('byte_range_read_supported'))}</code>, <code>write_staging_supported={status_value(object_store_ladder.get('write_staging_supported'))}</code>, <code>commit_protocol_supported={status_value(object_store_ladder.get('commit_protocol_supported'))}</code>.</p>
        """
        object_store_ladder_html = "\n".join(
            line.rstrip() for line in object_store_ladder_html.splitlines()
        ).strip()
    table_format_matrix = scoreboard.get("table_format_boundary_matrix", {})
    table_format_rows = table_format_matrix.get("rows", [])
    table_format_table_rows = "\n".join(
        "<tr>"
        f"<td><strong>{esc(row.get('row_id'))}</strong><span>{esc(row.get('behavior'))}</span></td>"
        f"<td><span class=\"claim-badge {status_class(str(row.get('support_status', 'blocked')).replace('-', '_'))}\">{esc(row.get('support_status'))}</span></td>"
        f"<td>{status_value(row.get('local_metadata_smoke_related'))}</td>"
        f"<td>{status_value(row.get('table_data_read_allowed'))}</td>"
        f"<td>{status_value(row.get('commit_allowed'))}</td>"
        f"<td>{esc(row.get('claim_boundary'))}</td>"
        "</tr>"
        for row in table_format_rows
        if isinstance(row, dict)
    )
    table_format_matrix_html = ""
    if table_format_table_rows:
        table_format_matrix_html = f"""
        <div class="section-spacer"></div>
        <h3>Iceberg/Delta/Hudi boundary matrix</h3>
        <p class="section-lede">This report-only matrix separates local manifest metadata smoke from Iceberg, Delta, and Hudi runtime support. Table scans, snapshot/time travel, appends, merge/update/delete, commits, rollbacks, catalog interaction, and object-store-backed table runtime remain blocked.</p>
        <div class="table-scroll">
          <table>
            <thead><tr><th>Row</th><th>Status</th><th>Local smoke</th><th>Data read</th><th>Commit</th><th>Boundary</th></tr></thead>
            <tbody>{table_format_table_rows}</tbody>
          </table>
        </div>
        <p class="section-note">Table-format summary: <code>runtime_supported={status_value(table_format_matrix.get('runtime_supported'))}</code>, <code>local_metadata_smoke_available={status_value(table_format_matrix.get('local_metadata_smoke_available'))}</code>, <code>table_scan_supported={status_value(table_format_matrix.get('table_scan_supported'))}</code>, <code>table_write_supported={status_value(table_format_matrix.get('table_write_supported'))}</code>, <code>table_commit_supported={status_value(table_format_matrix.get('table_commit_supported'))}</code>, <code>object_store_runtime_supported={status_value(table_format_matrix.get('object_store_runtime_supported'))}</code>.</p>
        """
        table_format_matrix_html = "\n".join(
            line.rstrip() for line in table_format_matrix_html.splitlines()
        ).strip()
    database_warehouse_matrix = scoreboard.get("database_warehouse_boundary_matrix", {})
    database_warehouse_rows = database_warehouse_matrix.get("rows", [])
    database_warehouse_table_rows = "\n".join(
        "<tr>"
        f"<td><strong>{esc(row.get('row_id'))}</strong><span>{esc(row.get('connector_type'))}</span></td>"
        f"<td><span class=\"claim-badge {status_class(str(row.get('support_status', 'blocked')).replace('-', '_'))}\">{esc(row.get('support_status'))}</span></td>"
        f"<td>{status_value(row.get('credential_required'))}</td>"
        f"<td>{status_value(row.get('network_required'))}</td>"
        f"<td>{status_value(row.get('query_pushdown_supported'))}</td>"
        f"<td>{esc(row.get('claim_boundary'))}</td>"
        "</tr>"
        for row in database_warehouse_rows
        if isinstance(row, dict)
    )
    database_warehouse_matrix_html = ""
    if database_warehouse_table_rows:
        database_warehouse_matrix_html = f"""
        <div class="section-spacer"></div>
        <h3>Database/warehouse import-export boundary</h3>
        <p class="section-lede">This report-only matrix separates SQLite, Postgres, MySQL, JDBC/ODBC, Snowflake, BigQuery, and Databricks SQL from ShardLoom execution. Current rows do not load drivers, resolve credentials, probe networks, import/export data, push queries down, or use external systems as fallback engines.</p>
        <div class="table-scroll">
          <table>
            <thead><tr><th>Row</th><th>Status</th><th>Credentials</th><th>Network</th><th>Pushdown</th><th>Boundary</th></tr></thead>
            <tbody>{database_warehouse_table_rows}</tbody>
          </table>
        </div>
        <p class="section-note">Database/warehouse summary: <code>runtime_supported={status_value(database_warehouse_matrix.get('runtime_supported'))}</code>, <code>import_runtime_supported={status_value(database_warehouse_matrix.get('import_runtime_supported'))}</code>, <code>export_runtime_supported={status_value(database_warehouse_matrix.get('export_runtime_supported'))}</code>, <code>query_pushdown_supported={status_value(database_warehouse_matrix.get('query_pushdown_supported'))}</code>, <code>credential_resolution_performed={status_value(database_warehouse_matrix.get('credential_resolution_performed'))}</code>, <code>network_probe_performed={status_value(database_warehouse_matrix.get('network_probe_performed'))}</code>.</p>
        """
        database_warehouse_matrix_html = "\n".join(
            line.rstrip() for line in database_warehouse_matrix_html.splitlines()
        ).strip()
    runtime_count = sum(
        1
        for row in rows
        if isinstance(row, dict) and row.get("support_status") == "runtime-supported"
    )
    smoke_count = sum(
        1
        for row in rows
        if isinstance(row, dict) and row.get("support_status") == "smoke-supported"
    )
    report_only_count = sum(
        1
        for row in rows
        if isinstance(row, dict) and row.get("support_status") == "report-only"
    )
    blocked_count = sum(
        1 for row in rows if isinstance(row, dict) and row.get("support_status") == "blocked"
    )
    return f"""
    <section id="compatibility">
      <div class="shell">
        <p class="eyebrow">Compatibility scoreboard</p>
        <h2>Can I use this surface?</h2>
        <p class="section-lede">The universal compatibility scoreboard is a typed capability map, not a support expansion. It keeps runtime-supported, smoke-supported, report-only, and blocked rows visible without treating planned adapters, object stores, table formats, SQL/DataFrame, or Foundry as production support.</p>
        <div class="metric-row">
          <div class="metric"><strong>{runtime_count}</strong><span>runtime-supported rows</span></div>
          <div class="metric"><strong>{smoke_count}</strong><span>smoke-supported rows</span></div>
          <div class="metric"><strong>{report_only_count}</strong><span>report-only rows</span></div>
          <div class="metric"><strong>{blocked_count}</strong><span>blocked rows</span></div>
        </div>
        <div class="table-scroll">
          <table>
            <thead><tr><th>Surface</th><th>Status</th><th>Direction</th><th>Claim gate</th><th>Boundary</th></tr></thead>
            <tbody>{table_rows}</tbody>
          </table>
        </div>
        {generated_contract_html}
        {object_store_ladder_html}
        {table_format_matrix_html}
        {database_warehouse_matrix_html}
        <p class="section-note">Source: <code>docs/architecture/universal-compatibility-coverage-scoreboard.json</code> and <code>docs/architecture/universal-compatibility-coverage-scoreboard.md</code>. All rows preserve <code>fallback_attempted=false</code> and <code>external_engine_invoked=false</code>.</p>
      </div>
    </section>"""


def site_footer() -> str:
    return """
  <footer class="site-footer">
    <div class="shell footer-grid">
      <div>
        <strong>ShardLoom</strong>
        <span>Apache-2.0 project code. Pre-release public evidence surface.</span>
      </div>
      <div>
        <span>ShardLoom name, logo, and icon are brand assets; see <a href="/BRAND.md">BRAND.md</a>.</span>
        <span>Independent downstream Vortex-first workflow layer; not an official Vortex project or Vortex-endorsed.</span>
        <span>No performance, Spark-displacement, production SQL/DataFrame, object-store/lakehouse, Foundry, or package-publication claim.</span>
      </div>
    </div>
  </footer>
"""


def page_header_logo() -> str:
    return '<img class="page-header-logo" src="/assets/logo/shardloom-logo-trim.png" alt="ShardLoom">'


def page(
    title: str,
    description: str,
    body: str,
    active: str,
    canonical_path_override: str | None = None,
    extra_head: str = "",
    pagefind_filters: dict[str, Any] | None = None,
) -> str:
    nav_html = site_nav(active)
    canonical_paths = {
        "home": "",
        "field-guide": "field-guide/",
        "telemetry": "benchmarks",
        "flow": "compute-engine-flow",
        "status": "status",
        "use-cases": "use-cases/",
        "docs": "readme",
    }
    canonical_path = canonical_path_override
    if canonical_path is None:
        canonical_path = canonical_paths.get(active, "")
    canonical_url = f"https://shardloom.io/{canonical_path}"
    head_extra = f"  {extra_head}\n" if extra_head else ""
    pagefind_filters_html = pagefind_filter_spans(pagefind_filters)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{esc(title)}</title>
  <meta name="description" content="{esc(description)}">
  <link rel="canonical" href="{canonical_url}">
  <link rel="icon" type="image/png" href="/assets/logo/shardloom-favicon.png">
  <link rel="apple-touch-icon" href="/assets/logo/shardloom-favicon.png">
  <link rel="stylesheet" href="/assets/site.css">
{head_extra}
  <meta property="og:title" content="{esc(title)}">
  <meta property="og:description" content="{esc(description)}">
  <meta property="og:image" content="https://shardloom.io/assets/logo/shardloom-logo.png">
  <meta property="og:type" content="website">
  <meta property="og:url" content="{canonical_url}">
  <meta name="twitter:card" content="summary_large_image">
</head>
<body class="command-shell">
  <header class="site-header mission-nav">
    <div class="shell nav command-nav">
      <a class="brand" href="/" aria-label="ShardLoom home">
        <img class="brand-icon" src="/assets/logo/shardloom-favicon.png" alt="" width="36" height="36" aria-hidden="true">
        <span class="sr-only">ShardLoom</span>
      </a>
      <nav class="nav-links" aria-label="Primary">
        {nav_html}
      </nav>
    </div>
  </header>
{status_ribbon()}
  <main data-pagefind-body>{pagefind_filters_html}{body}</main>
{site_footer()}
</body>
</html>
"""


def doc_page(source: Path, title: str, description: str, source_label: str, active: str) -> str:
    markdown = source.read_text(encoding="utf-8")
    body = f"""
    <section class="doc-hero">
      <div class="shell">
        {page_header_logo()}
        <p class="eyebrow">Rendered repository document</p>
        <h1>{esc(title)}</h1>
        <p class="lede">{esc(description)}</p>
        <p class="source-note">Source: <code>{esc(source_label)}</code></p>
      </div>
    </section>
    <section class="doc-section">
      <div class="shell doc-layout">
        <article class="doc-body">
          {markdown_to_html(markdown)}
        </article>
      </div>
    </section>
    """
    return page(
        title,
        description,
        body,
        active,
        pagefind_filters={"section": source_label, "status": "reference"},
    )


def compute_flow_page(source: Path) -> str:
    markdown = source.read_text(encoding="utf-8")
    body = f"""
    <section class="doc-hero flow-mission-hero">
      <div class="shell">
        {page_header_logo()}
        <p class="eyebrow">Compute Flow Mission Map</p>
        <h1>From request to evidence to claim gate.</h1>
        <p class="lede">ShardLoom routes each request through policy admission, explicit execution and engine modes, a provider path, result/sink handling, evidence generation, and a claim gate. This page is explanation only; it does not expand runtime support.</p>
        <p class="source-note">Source: <code>docs/architecture/compute-engine-flow-reference.md</code></p>
        <div class="mission-chain" aria-label="ShardLoom execution mission map">
          <article><strong>Access</strong><span>CLI, Python, benchmarks, adapters, and planned REST/event surfaces enter through one typed request envelope.</span></article>
          <article><strong>Admission</strong><span>Policy, capability, semantic profile, requested mode, and output intent are checked before execution.</span></article>
          <article><strong>Execution mode</strong><span>compatibility_import_certified, prepared_vortex, native_vortex, direct_compatibility_transient, or auto with selected reason.</span></article>
          <article><strong>Engine mode</strong><span>batch, live fixture, hybrid fixture, or report-only streaming semantics remain separate from source/preparation lanes.</span></article>
          <article><strong>Provider path</strong><span>Vortex provider, ShardLoom-native kernel, residual-native fixture path, source-backed scan, or deterministic diagnostic.</span></article>
          <article><strong>Result and sink</strong><span>Typed output carries result refs, sink artifacts, replay references, or unsupported status.</span></article>
          <article><strong>Evidence</strong><span>Native I/O, execution certificate, timing, materialization/decode, source/sink, and policy evidence stay visible.</span></article>
          <article><strong>Claim gate</strong><span>claim_grade, fixture_smoke_only, report_only, blocked, or not_claim_grade controls what can be said.</span></article>
        </div>
      </div>
    </section>
    <nav class="page-subnav" aria-label="Compute flow sections">
      <div class="shell">
        <a href="#flow-overview">Overview</a>
        <a href="#route-model">Route model</a>
        <a href="#flow-modes">Mode lanes</a>
        <a href="#engine-fabric">Engine fabric</a>
        <a href="#provider-admission">Provider admission</a>
        <a href="#downstream">Downstream use</a>
        <a href="#canonical-reference">Canonical reference</a>
      </div>
    </nav>
    <section id="flow-overview">
      <div class="shell split">
        <div>
          <p class="eyebrow">Plain-English route</p>
          <h2>One request, one visible route.</h2>
          <p class="section-lede">ShardLoom should never make users infer what happened. Python, SQL, CLI, and future DataFrame surfaces are front doors; the execution route is determined separately from the source, preparation policy, output sink, and evidence level.</p>
        </div>
        <aside class="terminal-panel flow-command-panel">
          <div class="console-row"><code>front door</code><span>Python, SQL, CLI, adapter, benchmark, or future API</span></div>
          <div class="console-row"><code>route</code><span>source -> preparation -> execution -> output -> evidence</span></div>
          <div class="console-row"><code>provider</code><span>Vortex source, prepared artifact, ShardLoom kernel, or blocked path</span></div>
          <div class="console-row"><code>claim</code><span>typed evidence refs, timing, no-fallback fields, claim_gate_status</span></div>
        </aside>
      </div>
    </section>
    <section id="route-model">
      <div class="shell">
        <p class="eyebrow">Workflow vs route</p>
        <h2>SQL and Python express the work. Routes explain how it ran.</h2>
        <p class="section-lede">A normal user story is <code>read/generate -> transform/query -> write</code>. ShardLoom reports the internal path as <code>InputAdapter -> SourceState -> VortexPreparedState -> ExecutionPlan -> OutputPlan -> SinkArtifact</code> when those stages apply.</p>
        <div class="comparison-guidance">
          <article>
            <h3>Common user workflow</h3>
            <ul>
              <li>Enter through Python, SQL, CLI, or a future DataFrame front door.</li>
              <li>Read local data or generate rows without an input dataset.</li>
              <li>Run admitted ShardLoom/Vortex work without external fallback.</li>
              <li>Write local output and inspect evidence.</li>
            </ul>
          </article>
          <article>
            <h3>Engine route evidence</h3>
            <ul>
              <li>Source route: no input, generated rows, local file, existing Vortex, or future platform source.</li>
              <li>Preparation route: direct transient, certified import/stage, prepared Vortex, or native Vortex.</li>
              <li>Output route: scalar/report, local sink, Vortex result, fanout, or deterministic blocker.</li>
              <li>Evidence route: minimal runtime, certified, full replay, and claim gate.</li>
            </ul>
          </article>
        </div>
      </div>
    </section>
    <section id="flow-modes">
      <div class="shell">
        <p class="eyebrow">Execution mode lanes</p>
        <h2>Source and preparation choices stay explicit.</h2>
        <p class="section-lede">These lanes are not interchangeable timing rows. Compatibility import carries ingest/stage/certification work. Prepared and native Vortex lanes are the current runtime-development direction. Direct transient and auto stay constrained by diagnostics and selected-mode reporting.</p>
        <div class="mode-lanes mission-mode-lanes">
          <article class="mode-lane"><span class="lane-tag">Certified import/stage route</span><h3>compatibility_import_certified</h3><p>Reads compatibility input, imports to Vortex, writes/reopens/scans, computes, and certifies the full ingest/stage workflow.</p></article>
          <article class="mode-lane"><span class="lane-tag">Prepared steady-state route</span><h3>prepared_vortex</h3><p>Prepares Vortex once, then runs scoped queries from prepared artifacts with source-backed scan and no-fallback evidence.</p></article>
          <article class="mode-lane"><span class="lane-tag">Already-Vortex route</span><h3>native_vortex</h3><p>Runs from existing Vortex input where the local row carries Native I/O, provider admission, and claim-boundary fields.</p></article>
          <article class="mode-lane"><span class="lane-tag">Direct one-shot route</span><h3>direct_compatibility_transient / auto</h3><p>Direct transient remains narrow and not Vortex-native; auto is only a transparent selector with a selected mode and reason.</p></article>
        </div>
      </div>
    </section>
    <section id="engine-fabric">
      <div class="shell">
        <p class="eyebrow">Engine fabric</p>
        <h2>Batch, live, and hybrid are workload semantics, not hidden fast modes.</h2>
        <div class="status-board compact-status-board">
          <article class="status-column"><span class="claim-badge supported">current foundation</span><h3>Batch</h3><p>Bounded local Vortex analytics are the practical execution foundation for current evidence-backed rows.</p></article>
          <article class="status-column"><span class="claim-badge fixture">fixture-smoke</span><h3>Live</h3><p>Live helpers and in-memory fixture reports exist; durable state, broker adapters, and freshness evidence are not public support claims.</p></article>
          <article class="status-column"><span class="claim-badge fixture">fixture-smoke</span><h3>Hybrid</h3><p>Hybrid overlay helpers and fixture reports exist; durable hot/cold commit semantics remain outside current public support.</p></article>
          <article class="status-column"><span class="claim-badge report-only">report-only</span><h3>Streaming</h3><p>Streaming, zero-copy, and backpressure plans remain capability/report posture unless evidence promotes a workload.</p></article>
        </div>
      </div>
    </section>
    <section id="provider-admission">
      <div class="shell">
        <p class="eyebrow">Provider admission</p>
        <h2>The provider path is admitted before work runs.</h2>
        <div class="provider-admission-grid">
          <article><strong>1. Policy gate</strong><span>No-fallback, credential, external-effect, and release-claim rules are checked first.</span></article>
          <article><strong>2. Capability gate</strong><span>Source, operator, sink, engine mode, and evidence requirements decide whether a path is supported, fixture-only, report-only, or blocked.</span></article>
          <article><strong>3. Provider route</strong><span>Admitted work chooses a Vortex/source-backed path, a ShardLoom-native/residual-native path, or a deterministic diagnostic.</span></article>
          <article><strong>4. Evidence route</strong><span>Outputs carry certificates, timing fields, fallback/external-engine flags, materialization/decode status, and claim_gate_status.</span></article>
        </div>
      </div>
    </section>
    <section id="downstream">
      <div class="shell">
        <p class="eyebrow">I/O and downstream use</p>
        <h2>Downstream consumers receive evidence, not a hidden engine story.</h2>
        <p class="section-lede">CLI, Python, benchmark pages, adapters, and future REST/event consumers must preserve the typed output envelope: result refs, diagnostics, certificates, timing fields, and claim gates. Compatibility files, Vortex artifacts, object-store/table inputs, and stream inputs do not imply support until the capability and evidence rows say so.</p>
      </div>
    </section>
    <section id="canonical-reference" class="doc-section">
      <div class="shell doc-layout">
        <article class="doc-body">
          <p class="eyebrow">Canonical reference</p>
          <h2>Full Compute Engine Flow Reference</h2>
          {markdown_to_html(markdown)}
        </article>
      </div>
    </section>
    """
    return page(
        "Compute Engine Flow",
        "Mission-map view of ShardLoom access surfaces, execution modes, engine modes, provider admission, downstream usage, evidence, and claim gates.",
        body,
        "flow",
        pagefind_filters={"section": "Compute Flow", "status": "reference"},
    )


def status_page(use_cases: list[dict[str, Any]]) -> str:
    body = f"""
    <section class="doc-hero status-hero">
      <div class="shell">
        {page_header_logo()}
        <p class="eyebrow">Launch Status Board</p>
        <h1>Public posture for ShardLoom evidence.</h1>
        <p class="lede">ShardLoom is a pre-release local compute engine foundation. This board separates supported local smoke evidence, fixture-smoke paths, report-only surfaces, blocked areas, planned work, and claims that are not made.</p>
      </div>
    </section>
    <nav class="page-subnav" aria-label="Status sections">
      <div class="shell">
        <a href="#can-i-use-this">Can I use this?</a>
        <a href="#supported">Supported local smoke</a>
        <a href="#fixture">Fixture-smoke</a>
        <a href="#compatibility">Compatibility</a>
        <a href="#report-only">Report-only</a>
        <a href="#blocked">Blocked</a>
        <a href="#planned">Planned</a>
        <a href="#not-claimed">Not claimed</a>
      </div>
    </nav>
{render_public_status_scorecard_section(use_cases)}
    <section id="supported">
      <div class="shell">
        <p class="eyebrow">Evidence-backed local surface</p>
        <h2>Supported local smoke and evidence surfaces.</h2>
        <div class="status-board">
          <article class="status-column"><span class="claim-badge supported">supported local smoke</span><h3>No-fallback local execution evidence</h3><p>Supported rows expose <code>fallback_attempted=false</code> and <code>external_engine_invoked=false</code> for their scoped workload.</p></article>
          <article class="status-column"><span class="claim-badge supported">supported local smoke</span><h3>Prepared/native Vortex batch smoke</h3><p>Scoped prepared/native batch rows preserve source-backed scan, materialization/decode, timing, and claim-gate fields.</p></article>
          <article class="status-column"><span class="claim-badge supported">supported local smoke</span><h3>Benchmark evidence publication</h3><p>The public Telemetry page publishes committed local evidence snapshots and frames them as attribution baseline evidence.</p></article>
        </div>
      </div>
    </section>
    <section id="fixture">
      <div class="shell">
        <p class="eyebrow">Scoped fixture evidence</p>
        <h2>Fixture-smoke paths are useful signals, not broad support.</h2>
        <div class="status-board">
          <article class="status-column"><span class="claim-badge fixture">fixture-smoke</span><h3>Live mode</h3><p>Live helpers and in-memory reports exist, but durable state, brokers, freshness proof, and production claims remain outside scope.</p></article>
          <article class="status-column"><span class="claim-badge fixture">fixture-smoke</span><h3>Hybrid overlay</h3><p>Hybrid fixture reports exist for base-plus-delta reasoning; durable hot/cold commit semantics are not public support.</p></article>
          <article class="status-column"><span class="claim-badge fixture">fixture-smoke</span><h3>Table metadata smoke</h3><p>Table metadata evidence is scoped metadata proof, not a lakehouse/catalog runtime claim.</p></article>
        </div>
      </div>
    </section>
{render_compatibility_scoreboard_section()}
    <section id="report-only">
      <div class="shell">
        <p class="eyebrow">Report-only surfaces</p>
        <h2>These surfaces can be documented or diagnosed without claiming runtime support.</h2>
        <div class="status-board">
          <article class="status-column"><span class="claim-badge report-only">report-only</span><h3>REST/event surfaces</h3><p>Future API surfaces must preserve typed envelopes, selected mode, diagnostics, evidence, and claim gates before promotion.</p></article>
          <article class="status-column"><span class="claim-badge report-only">report-only</span><h3>Adapters and end-user wrappers</h3><p>CLI, Python, and planned adapter access must improve ergonomics without hiding execution mode or fallback status.</p></article>
          <article class="status-column"><span class="claim-badge report-only">report-only</span><h3>Object-store, lakehouse, and Foundry boundaries</h3><p>These areas remain boundary/status documentation unless runtime proof and release gates promote a narrow slice.</p></article>
          <article class="status-column"><span class="claim-badge report-only">report-only</span><h3>Foundry scale proof boundary</h3><p>The local proof emits <code>shardloom.foundry_scale_proof_boundary.v1</code> with Foundry runtime, compute, Spark, evidence-dataset, and public-claim gates closed.</p></article>
        </div>
      </div>
    </section>
    <section id="blocked">
      <div class="shell">
        <p class="eyebrow">Blocked claims and paths</p>
        <h2>Blocked means deterministic diagnostics or no public claim.</h2>
        <div class="status-board">
          <article class="status-column"><span class="claim-badge blocked">blocked</span><h3>Silent external fallback</h3><p>Unsupported work must not run through Spark, DuckDB, DataFusion, Polars, or any other external engine as ShardLoom execution.</p></article>
          <article class="status-column"><span class="claim-badge blocked">blocked</span><h3>Production SQL/DataFrame support</h3><p>SQL/DataFrame surfaces require scoped implementation, tests, and evidence before public support language changes.</p></article>
          <article class="status-column"><span class="claim-badge blocked">blocked</span><h3>Public performance or superiority claims</h3><p>Local benchmark rows remain attribution evidence until workload-scoped claim gates and release checks say otherwise.</p></article>
        </div>
      </div>
    </section>
    <section id="planned">
      <div class="shell split">
        <div>
          <p class="eyebrow">Planned work</p>
          <h2>Current planning posture.</h2>
          <p class="section-lede">The active phase plan remains the source of truth for planned work. The website status board summarizes broad posture, but does not replace, the phase plan and completed ledger.</p>
        </div>
        <aside class="terminal-panel flow-command-panel">
          <div class="console-row"><code>runtime</code><span>prepared/native Vortex path expansion and operator coverage remain the main optimization direction</span></div>
          <div class="console-row"><code>language</code><span>SQL/DataFrame, adapters, REST/event, and notebook surfaces remain gated by scoped proof</span></div>
          <div class="console-row"><code>platform</code><span>object-store, lakehouse, and Foundry paths remain report-only or blocked until evidence exists</span></div>
          <div class="console-row"><code>release</code><span>public claims require workload-scoped evidence and release gates</span></div>
        </aside>
      </div>
    </section>
    <section id="not-claimed">
      <div class="shell">
        <p class="eyebrow">Claim boundary</p>
        <h2>What this public site does not claim.</h2>
        <div class="boundary-grid">
          <article><strong>No performance or superiority claim</strong><span>Telemetry is local attribution evidence, not a public ranking.</span></article>
          <article><strong>No Spark-displacement claim</strong><span>External engines are baseline context only and never fallback execution.</span></article>
          <article><strong>No production SQL/DataFrame claim</strong><span>Language layers and planner surfaces remain gated by scoped evidence.</span></article>
          <article><strong>No production object-store, lakehouse, or Foundry claim</strong><span>Those surfaces remain report-only, blocked, or planned until proof exists.</span></article>
          <article><strong>No package-publication claim</strong><span>Public package readiness requires release gates outside this website page.</span></article>
          <article><strong>No hidden fast mode</strong><span>Auto mode must report selected mode and reason; batch process reuse is not a hidden runtime.</span></article>
        </div>
      </div>
    </section>
    <script src="/assets/use-cases.js" defer></script>
    """
    return page(
        "ShardLoom Status Board",
        "Claim-safe public posture board for ShardLoom supported local smoke, fixture-smoke, report-only, blocked, planned, and not-claimed surfaces.",
        body,
        "status",
        pagefind_filters={"section": "Status", "status": "public-posture"},
    )


FALLBACK_FIELD_GUIDE_CONCEPTS: list[dict[str, Any]] = [
    {
        "slug": "no-fallback",
        "title": "No Fallback",
        "answer": "ShardLoom must not silently delegate unsupported execution to another query engine.",
        "why": "The policy keeps evidence auditable: unsupported work becomes a deterministic diagnostic instead of an invisible execution handoff.",
        "how": "CLI, benchmark, and evidence rows expose fallback and external-engine fields so readers can see whether ShardLoom executed the supported path.",
        "proves": "A supported row can show `fallback_attempted=false` and `external_engine_invoked=false` for its scoped workload.",
        "not_proves": "It does not prove every SQL, DataFrame, object-store, lakehouse, or adapter path is implemented.",
        "evidence": ["fallback_attempted", "external_engine_invoked", "claim_gate_status", "diagnostic_code"],
        "related": ["execution-modes", "claim-gates", "unsupported-diagnostics"],
        "sources": ["AGENTS.md", "docs/rfcs/0002-no-fallback-and-vortex-io.md"],
        "boundary": "No-fallback evidence is workload-scoped and does not create a broad production platform claim.",
    },
    {
        "slug": "execution-modes",
        "title": "Execution Modes",
        "answer": "Execution modes identify the source and preparation lane that a run used.",
        "why": "Mode attribution separates compatibility certification costs from prepared/native Vortex runtime paths.",
        "how": "Benchmark and CLI rows report modes such as `compatibility_import_certified`, `prepared_vortex`, `native_vortex`, `direct_compatibility_transient`, and `auto`.",
        "proves": "A row can identify which lane was selected and which timing fields belong to that lane.",
        "not_proves": "`auto` is not a hidden fast mode, and mode vocabulary is not the same as broad runtime support.",
        "evidence": ["selected_execution_mode", "requested_execution_mode", "mode timing fields", "claim_gate_status"],
        "related": ["prepared-vortex", "native-vortex", "compatibility-import-certified"],
        "sources": ["docs/architecture/compute-engine-flow-reference.md", "docs/rfcs/0042-vortex-runtime-utilization-execution-spine.md"],
        "boundary": "Execution mode labels are explanatory and must not be read as performance claims.",
    },
    {
        "slug": "compatibility-import-certified",
        "title": "Compatibility Import Certified",
        "answer": "The compatibility lane imports local compatibility data into Vortex evidence paths and records certification costs.",
        "why": "It helps prove workflow coverage, but it carries parse, import, write/reopen, scan, sink, and evidence overhead.",
        "how": "The benchmark page decomposes compatibility parse, Vortex import, Vortex write, Vortex scan, operator, sink, and evidence fields.",
        "proves": "It can show that a compatibility input moved through a Vortex-backed evidence workflow without fallback.",
        "not_proves": "It is not pure query speed and should not be ranked against direct-file engine baselines.",
        "evidence": ["compatibility_parse_millis", "compatibility_to_vortex_import_millis", "vortex_write_millis", "vortex_scan_millis"],
        "related": ["prepared-vortex", "benchmark-telemetry", "claim-gates"],
        "sources": ["website/benchmarks.html", "benchmarks/traditional_analytics/README.md"],
        "boundary": "Certification-lane evidence is not public speed, superiority, or best-default proof.",
    },
    {
        "slug": "prepared-vortex",
        "title": "Prepared Vortex",
        "answer": "Prepared Vortex is the current runtime-development lane for already prepared Vortex artifacts.",
        "why": "It removes compatibility import from the timed path and makes prepared/native optimization work easier to interpret.",
        "how": "Prepared rows expose source-backed scan, Native I/O, materialization/decode, no-fallback, and claim-gate fields.",
        "proves": "A prepared row can show scoped local execution over prepared Vortex artifacts with explicit evidence fields.",
        "not_proves": "It does not prove generalized encoded aggregation, production SQL/DataFrame runtime, or superiority over other engines.",
        "evidence": ["prepared_vortex", "source_backed_scan_*", "native_io_certificate_status", "materialization_boundary_report_emitted"],
        "related": ["native-vortex", "materialization-boundary", "benchmark-telemetry"],
        "sources": ["README.md", "docs/architecture/compute-engine-flow-reference.md"],
        "boundary": "Prepared Vortex is the main optimization direction, but current rows remain claim-gated.",
    },
    {
        "slug": "native-vortex",
        "title": "Native Vortex",
        "answer": "Native Vortex is the lane for scoped execution over existing Vortex artifacts.",
        "why": "It keeps Vortex as the native substrate instead of treating Vortex as a temporary translation detail.",
        "how": "Native rows must preserve admission, source-backed scan, Native I/O, materialization, and no-fallback evidence.",
        "proves": "A scoped row can show the selected Vortex-native input path and its evidence posture.",
        "not_proves": "It does not prove every Vortex layout, object-store source, table format, or downstream sink is supported.",
        "evidence": ["native_vortex", "source_backed_scan_provider_kind", "source_backed_scan_native_io_certificate_status"],
        "related": ["prepared-vortex", "native-io-certificate", "execution-modes"],
        "sources": ["docs/rfcs/0005-vortex-native-file-io-output.md", "docs/skills/vortex/vortex-file-io.md"],
        "boundary": "Native Vortex evidence is scoped to the supported artifacts and operators present in the repo.",
    },
    {
        "slug": "native-io-certificate",
        "title": "Native I/O Certificate",
        "answer": "A Native I/O certificate records whether a scoped run used the approved ShardLoom/Vortex-native I/O path.",
        "why": "It gives readers a concrete evidence field instead of asking them to infer native behavior from marketing language.",
        "how": "Prepared/native rows and workflow evidence attach certificate status where the supported path can be reviewed.",
        "proves": "A scoped workload can show that its native I/O evidence was emitted and certified.",
        "not_proves": "It does not certify every source, sink, object-store path, or table-format runtime.",
        "evidence": ["native_io_certificate_status", "source_backed_scan_native_io_certificate_status", "certificate_refs"],
        "related": ["native-vortex", "prepared-vortex", "materialization-boundary"],
        "sources": ["docs/rfcs/0031-universal-native-io-envelope.md", "docs/architecture/compute-engine-flow-reference.md"],
        "boundary": "Native I/O certificates are scoped to the workload and source/sink path that emitted them.",
    },
    {
        "slug": "materialization-boundary",
        "title": "Materialization Boundary",
        "answer": "A materialization boundary records where data was decoded, materialized, converted, or kept native.",
        "why": "It prevents a row from implying encoded-native execution when residual-native or decoded work actually happened.",
        "how": "Prepared/native evidence exposes decode, materialization, row-read, Arrow conversion, and boundary-report fields.",
        "proves": "A row can show whether data crossed a boundary for the scoped scenario.",
        "not_proves": "It does not prove zero-copy, zero-decode, or encoded-native execution unless the row says so with evidence.",
        "evidence": ["data_decoded", "data_materialized", "row_read", "arrow_converted", "materialization_boundary_report_emitted"],
        "related": ["prepared-vortex", "claim-gates", "native-io-certificate"],
        "sources": ["docs/rfcs/0013-streaming-zero-copy-boundary-interoperability.md", "website/benchmarks.html"],
        "boundary": "Boundary fields explain execution posture; they are not performance claims.",
    },
    {
        "slug": "claim-gates",
        "title": "Claim Gates",
        "answer": "Claim gates state whether a row is claim-grade, fixture-smoke, report-only, blocked, unsupported, or not claim-grade.",
        "why": "They keep public interpretation tied to available correctness, benchmark, certificate, and policy evidence.",
        "how": "Website, benchmark, and CLI outputs preserve `claim_gate_status` and explicit not-claimable posture.",
        "proves": "A row can say which claim boundary applies to the current evidence.",
        "not_proves": "A claim gate label does not expand support beyond the named workload and evidence refs.",
        "evidence": ["claim_gate_status", "performance_claim_allowed", "claim_boundary", "support_status"],
        "related": ["benchmark-telemetry", "no-fallback", "unsupported-diagnostics"],
        "sources": ["docs/benchmarks/baseline-comparison-boundary.md", "docs/architecture/phased-execution-plan.md"],
        "boundary": "If evidence is missing, public claims stay closed.",
    },
    {
        "slug": "benchmark-telemetry",
        "title": "Benchmark Telemetry",
        "answer": "Benchmark telemetry is evidence and attribution, not a speed leaderboard.",
        "why": "ShardLoom rows include workflow and evidence costs that direct local engine rows may not carry.",
        "how": "The website separates compatibility import, prepared Vortex, native Vortex, external baselines, freshness labels, and raw tables.",
        "proves": "The current page shows local smoke coverage, timing decomposition, and claim-gate posture from committed artifacts.",
        "not_proves": "It does not prove performance superiority, replacement, production platform readiness, or best-default status.",
        "evidence": ["local fastest count", "local timing context", "source_backed_scan_*", "encoded_predicate_provider_*"],
        "related": ["compatibility-import-certified", "prepared-vortex", "claim-gates"],
        "sources": ["website/benchmarks.html", "target/shardloom-benchmark-evidence/"],
        "boundary": "External engines are baseline context only and never fallback execution.",
    },
    {
        "slug": "unsupported-diagnostics",
        "title": "Unsupported Diagnostics",
        "answer": "Unsupported diagnostics are deterministic reports for paths ShardLoom does not currently execute.",
        "why": "They are part of the no-fallback contract: unsupported work should be clear, stable, and actionable.",
        "how": "Capability views, phase items, and report-only rows expose blockers instead of silently invoking another engine.",
        "proves": "A blocked path can be intentionally documented without pretending it is implemented.",
        "not_proves": "A report-only row is not runtime support.",
        "evidence": ["support_status", "unsupported_reason", "required_evidence", "fallback_attempted"],
        "related": ["no-fallback", "claim-gates", "execution-modes"],
        "sources": ["docs/release/known-unsupported-paths.md", "docs/architecture/global-architecture-review.md"],
        "boundary": "Unsupported/report-only explanations reduce overclaiming risk but do not create new runtime support.",
    },
]


REQUIRED_FIELD_GUIDE_CATEGORIES = [
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
]


def field_guide_values(values: Any) -> list[str]:
    if isinstance(values, list):
        return [str(value) for value in values]
    if values is None:
        return []
    return [str(values)]


def normalize_field_guide_entry(entry: dict[str, Any]) -> dict[str, Any]:
    evidence_fields = field_guide_values(entry.get("evidence_fields"))
    if not evidence_fields:
        evidence_fields = field_guide_values(entry.get("evidence"))
    related_terms = field_guide_values(entry.get("related_terms"))
    if not related_terms:
        related_terms = field_guide_values(entry.get("related"))
    related_use_cases = field_guide_values(entry.get("related_use_cases"))
    reference_files = field_guide_values(entry.get("reference_files"))
    if not reference_files:
        reference_files = field_guide_values(entry.get("sources"))
    status = str(entry.get("status") or "report_only")
    title = str(entry["title"])
    category = str(entry.get("category") or "Evidence And Claims")
    summary = str(entry.get("summary") or entry.get("answer") or title)
    claim_boundary = str(entry.get("claim_boundary") or entry.get("boundary") or summary)
    evidence_sample = ", ".join(evidence_fields[:4]) if evidence_fields else "its evidence fields"
    return {
        "slug": str(entry["slug"]),
        "title": title,
        "category": category,
        "status": status,
        "summary": summary,
        "answer": str(entry.get("answer") or summary),
        "why": str(
            entry.get("why")
            or (
                f"This concept helps users interpret ShardLoom's {category.lower()} "
                "posture without reading the phase plan or RFCs first."
            )
        ),
        "how": str(
            entry.get("how")
            or (
                "ShardLoom surfaces this through docs, capability/status pages, "
                f"use cases, or evidence fields such as {evidence_sample}."
            )
        ),
        "current_support": str(
            entry.get("current_support")
            or (
                f"Current status is `{status}`. Treat that status exactly as "
                "shown; scoped, smoke, report-only, planned, blocked, or unsupported "
                "posture must not be read as broader runtime support."
            )
        ),
        "proves": str(
            entry.get("proves")
            or (
                "It helps identify the exact evidence or documentation surface "
                "that applies to this concept."
            )
        ),
        "not_proves": str(entry.get("not_proves") or claim_boundary),
        "try_it": str(
            entry.get("try_it")
            or (
                "Use the linked use cases when a runnable or blocked workflow is "
                "available for this concept."
                if related_use_cases
                else "No single quick recipe is attached yet; use the reference files as the source of truth."
            )
        ),
        "evidence": evidence_fields,
        "related": related_terms,
        "related_use_cases": related_use_cases,
        "sources": reference_files,
        "boundary": claim_boundary,
    }


def load_field_guide_concepts() -> list[dict[str, Any]]:
    if not FIELD_GUIDE_INDEX_PATH.exists():
        return [normalize_field_guide_entry(entry) for entry in FALLBACK_FIELD_GUIDE_CONCEPTS]
    data = load_json_file(FIELD_GUIDE_INDEX_PATH)
    if data.get("schema_version") != "shardloom.field_guide_index.v1":
        raise ValueError(f"Unsupported Field Guide index schema: {data.get('schema_version')}")
    categories = field_guide_values(data.get("categories"))
    missing_categories = [
        category for category in REQUIRED_FIELD_GUIDE_CATEGORIES if category not in categories
    ]
    if missing_categories:
        raise ValueError(f"Field Guide index missing categories: {', '.join(missing_categories)}")
    entries = data.get("entries")
    if not isinstance(entries, list):
        raise ValueError("Field Guide index must contain an entries list")
    if len(entries) < 50:
        raise ValueError("Field Guide index must contain at least 50 entries")
    concepts = [normalize_field_guide_entry(entry) for entry in entries]
    seen: set[str] = set()
    for concept in concepts:
        slug_value = concept["slug"]
        if slug_value in seen:
            raise ValueError(f"Duplicate Field Guide slug: {slug_value}")
        seen.add(slug_value)
        for key in ("title", "category", "status", "summary", "boundary"):
            if not concept.get(key):
                raise ValueError(f"Field Guide entry {slug_value} missing {key}")
        if concept["category"] not in categories:
            raise ValueError(
                f"Field Guide entry {slug_value} uses unknown category {concept['category']}"
            )
    missing_related = [
        f"{concept['slug']} -> {related_slug}"
        for concept in concepts
        for related_slug in concept["related"]
        if related_slug not in seen
    ]
    if missing_related:
        raise ValueError(
            "Field Guide entries reference unknown related terms: "
            + ", ".join(missing_related)
        )
    return concepts


FIELD_GUIDE_CONCEPTS = load_field_guide_concepts()
FIELD_GUIDE_CATEGORIES = (
    field_guide_values(load_json_file(FIELD_GUIDE_INDEX_PATH).get("categories"))
    if FIELD_GUIDE_INDEX_PATH.exists()
    else REQUIRED_FIELD_GUIDE_CATEGORIES
)


def field_guide_concepts_for_use_case(use_case_id: str) -> list[dict[str, Any]]:
    return [
        concept
        for concept in FIELD_GUIDE_CONCEPTS
        if use_case_id in field_guide_values(concept.get("related_use_cases"))
    ]


def related_field_guide_term_links(use_case_id: str) -> str:
    concepts = field_guide_concepts_for_use_case(use_case_id)
    if not concepts:
        return "<p>No related Field Guide terms are attached yet.</p>"
    return (
        '<div class="related-use-cases related-field-guide-terms">'
        + "".join(
            f'<a class="related-use-case" href="/field-guide/{esc(concept["slug"])}">'
            f'<span>{esc(concept["category"])}</span>'
            f'<strong>{esc(concept["title"])}</strong>'
            f"</a>"
            for concept in concepts
        )
        + "</div>"
    )


def normalize_field_guide_reading_path(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(entry["id"]),
        "title": str(entry["title"]),
        "summary": str(entry["summary"]),
        "status": str(entry["status"]),
        "terms": field_guide_values(entry.get("terms")),
        "use_cases": field_guide_values(entry.get("use_cases")),
        "claim_boundary": str(entry["claim_boundary"]),
    }


def load_field_guide_reading_paths() -> list[dict[str, Any]]:
    if not FIELD_GUIDE_INDEX_PATH.exists():
        return []
    data = load_json_file(FIELD_GUIDE_INDEX_PATH)
    entries = data.get("reading_paths") or []
    if not isinstance(entries, list):
        raise ValueError("Field Guide index reading_paths must be a list")
    paths = [normalize_field_guide_reading_path(entry) for entry in entries]
    concept_slugs = {concept["slug"] for concept in FIELD_GUIDE_CONCEPTS}
    use_case_ids = set(use_case_title_lookup())
    seen: set[str] = set()
    for path in paths:
        path_id = path["id"]
        if path_id in seen:
            raise ValueError(f"Duplicate Field Guide reading path id: {path_id}")
        seen.add(path_id)
        for key in ("title", "summary", "status", "claim_boundary"):
            if not path.get(key):
                raise ValueError(f"Field Guide reading path {path_id} missing {key}")
        for term in path["terms"]:
            if term not in concept_slugs:
                raise ValueError(f"Field Guide reading path {path_id} unknown term: {term}")
        for use_case_id in path["use_cases"]:
            if use_case_id not in use_case_ids:
                raise ValueError(
                    f"Field Guide reading path {path_id} unknown use case: {use_case_id}"
                )
    return paths


def concept_url(slug_value: str) -> str:
    return f"/field-guide/{slug_value}"


def concept_by_slug(slug_value: str) -> dict[str, Any]:
    for concept in FIELD_GUIDE_CONCEPTS:
        if concept["slug"] == slug_value:
            return concept
    raise KeyError(slug_value)


def bullet_list(items: list[str]) -> str:
    return "<ul>" + "".join(f"<li>{inline_markdown(item)}</li>" for item in items) + "</ul>"


_USE_CASE_TITLE_LOOKUP: dict[str, str] | None = None


def use_case_title_lookup() -> dict[str, str]:
    global _USE_CASE_TITLE_LOOKUP
    if _USE_CASE_TITLE_LOOKUP is None:
        data = load_index(DOC_USE_CASES / "use-case-index.yml")
        _USE_CASE_TITLE_LOOKUP = {
            str(use_case["id"]): str(use_case["title"])
            for use_case in data.get("use_cases", [])
        }
    return _USE_CASE_TITLE_LOOKUP


FIELD_GUIDE_READING_PATHS = load_field_guide_reading_paths()


def source_file_links(paths: list[str]) -> str:
    return render_citation_links(paths)


def related_concept_links(slugs: list[str]) -> str:
    if not slugs:
        return "<p>No directly related concept attached yet.</p>"
    links = []
    for slug_value in slugs:
        concept = concept_by_slug(slug_value)
        links.append(
            f'<a class="claim-badge reference-badge" href="{concept_url(slug_value)}">{esc(concept["title"])}</a>'
        )
    return '<div class="related-concepts-rail">' + "".join(links) + "</div>"


def related_use_case_links(ids: list[str]) -> str:
    if not ids:
        return "<p>No related use case page is attached yet.</p>"
    titles = use_case_title_lookup()
    links = []
    for use_case_id in ids:
        label = titles.get(use_case_id, use_case_id.replace("-", " "))
        links.append(
            f'<a class="claim-badge reference-badge" href="/use-cases/{esc(use_case_id)}">{esc(label)}</a>'
        )
    return '<div class="related-concepts-rail">' + "".join(links) + "</div>"


def reading_path_term_links(slugs: list[str]) -> str:
    links = []
    for slug_value in slugs:
        concept = concept_by_slug(slug_value)
        links.append(
            f'<a href="{concept_url(slug_value)}">{esc(concept["title"])}</a>'
        )
    return "<div>" + "".join(links) + "</div>"


def field_guide_concepts_by_category() -> list[tuple[str, list[dict[str, Any]]]]:
    grouped: list[tuple[str, list[dict[str, Any]]]] = []
    for category in FIELD_GUIDE_CATEGORIES:
        concepts = [
            concept for concept in FIELD_GUIDE_CONCEPTS if concept["category"] == category
        ]
        if concepts:
            grouped.append((category, concepts))
    return grouped


def clean_generated_html(text: str) -> str:
    return "\n".join(line.rstrip() for line in text.splitlines()) + "\n"


def field_guide_index_page() -> str:
    grouped_concepts = field_guide_concepts_by_category()
    category_links = "".join(
        f'<a class="reference-badge" href="#{slug(category)}"><span>{esc(category)}</span><strong>{len(concepts)}</strong></a>'
        for category, concepts in grouped_concepts
    )
    total_concepts = len(FIELD_GUIDE_CONCEPTS)
    total_categories = len(grouped_concepts)
    reading_paths = "".join(
        f"""
          <article class="reading-path-card" id="{slug(path['id'])}">
            <div class="reading-path-card-header">
              <span class="claim-badge {status_class(path['status'])}" data-status="{esc(path['status'])}">{esc(status_label(path['status']))}</span>
              <a href="#{slug(path['id'])}">#{esc(path['id'])}</a>
            </div>
            <h3>{esc(path['title'])}</h3>
            <p>{esc(path['summary'])}</p>
            <div class="reading-path-links">
              <strong>Concepts</strong>
              {reading_path_term_links(path['terms'])}
            </div>
            <div class="reading-path-links">
              <strong>Use cases</strong>
              {related_use_case_links(path['use_cases'])}
            </div>
            <p class="reading-path-boundary">{esc(path['claim_boundary'])}</p>
          </article>
        """
        for path in FIELD_GUIDE_READING_PATHS
    )
    category_sections = "".join(
        f"""
        <section class="field-guide-category" id="{slug(category)}">
          <div class="field-guide-category-header">
            <div>
              <p class="eyebrow">Category</p>
              <h2>{esc(category)}</h2>
            </div>
            <span>{len(concepts)} dossier{"s" if len(concepts) != 1 else ""}</span>
          </div>
          <div class="compact-term-list">
            {''.join(
                f'''
                <article class="field-guide-card compact-term-row">
                  <div class="compact-term-main">
                    <span class="claim-badge {status_class(concept['status'])}" data-status="{esc(concept['status'])}">{esc(status_label(concept['status']))}</span>
                    <h3><a href="{concept_url(concept['slug'])}">{esc(concept['title'])}</a></h3>
                    <p>{esc(concept['summary'])}</p>
                  </div>
                  <div class="field-guide-meta">
                    <span class="reference-badge">{esc(concept['category'])}</span>
                    <span class="reference-badge">{len(concept['evidence'])} evidence fields</span>
                    <a class="button compact-action" href="{concept_url(concept['slug'])}">Open dossier</a>
                  </div>
                </article>
                '''
                for concept in concepts
            )}
          </div>
        </section>
        """
        for category, concepts in field_guide_concepts_by_category()
    )
    body = f"""
    <section class="doc-hero field-guide-hero">
      <div class="shell">
        {page_header_logo()}
        <p class="eyebrow">Field Guide</p>
        <h1>Technical dossiers for auditable compute.</h1>
        <p class="lede">A source-linked atlas for reading ShardLoom evidence: execution modes, engine modes, Vortex-native paths, materialization boundaries, workflow recipes, benchmark telemetry, and public claim gates.</p>
      </div>
    </section>
    <section class="doc-section field-guide-toc-section">
      <div class="shell">
        <div class="terminal-panel field-guide-search">
          <p class="eyebrow">Static search</p>
          <h2>Search the atlas.</h2>
          <p>Search runs entirely from committed static assets. Results cover Field Guide dossiers, use cases, status, telemetry, compute flow, and rendered docs.</p>
          <div class="pagefind-controls">
            <pagefind-modal-trigger>Search Field Guide and docs</pagefind-modal-trigger>
            <pagefind-modal>
              <pagefind-modal-header>
                <pagefind-input placeholder="Search terms, evidence fields, workflows..."></pagefind-input>
              </pagefind-modal-header>
              <pagefind-modal-body>
                <div class="pagefind-filter-row">
                  <pagefind-filter-dropdown filter="section" label="Section" single-select sort="alphabetical"></pagefind-filter-dropdown>
                  <pagefind-filter-dropdown filter="status" label="Status" single-select sort="alphabetical"></pagefind-filter-dropdown>
                  <pagefind-filter-dropdown filter="category" label="Category" single-select sort="alphabetical"></pagefind-filter-dropdown>
                </div>
                <pagefind-summary></pagefind-summary>
                <pagefind-results></pagefind-results>
              </pagefind-modal-body>
              <pagefind-modal-footer>
                <pagefind-keyboard-hints></pagefind-keyboard-hints>
              </pagefind-modal-footer>
            </pagefind-modal>
          </div>
        </div>
        <div class="atlas-density-note" aria-label="Field Guide density and status summary">
          <span class="status-chip supported">{total_concepts} dossiers</span>
          <span class="status-chip report-only">{total_categories} concept families</span>
          <span class="status-chip blocked">blocked and report-only states stay visible</span>
        </div>
        <div class="section-header-row">
          <div>
            <p class="eyebrow">Reading paths</p>
            <h2>Start by what you need to understand.</h2>
          </div>
          <p>Each path links to exact dossiers and use cases while keeping support boundaries visible.</p>
        </div>
        <div class="reading-path-grid">{reading_paths}</div>
        <div class="terminal-panel field-guide-toc category-toc-band">
          <p class="eyebrow">Table of contents</p>
          <h2>Jump by concept family.</h2>
          <div>{category_links}</div>
        </div>
      </div>
    </section>
    <section class="doc-section">
      <div class="shell">
        {category_sections}
      </div>
    </section>
    """
    return page(
        "ShardLoom Field Guide",
        "Technical dossiers for interpreting ShardLoom evidence.",
        body,
        "field-guide",
        "field-guide/",
        PAGEFIND_HEAD,
        {"section": "Field Guide", "status": "atlas-index"},
    )


def field_guide_concept_page(
    concept: dict[str, Any],
    previous_concept: dict[str, Any] | None,
    next_concept: dict[str, Any] | None,
) -> str:
    related = related_concept_links(concept["related"])
    related_use_cases = related_use_case_links(concept["related_use_cases"])
    prev_link = (
        f'<a class="button" href="{concept_url(previous_concept["slug"])}">Previous: {esc(previous_concept["title"])}</a>'
        if previous_concept
        else ""
    )
    next_link = (
        f'<a class="button primary" href="{concept_url(next_concept["slug"])}">Next: {esc(next_concept["title"])}</a>'
        if next_concept
        else ""
    )
    source_links = source_file_links(concept["sources"])
    body = f"""
    <section class="doc-hero field-guide-hero">
      <div class="shell">
        {page_header_logo()}
        <p class="eyebrow">Field Guide dossier</p>
        <h1>{esc(concept['title'])}</h1>
        <p class="lede">{esc(concept['answer'])}</p>
        <div class="dossier-status-row">
          <span class="claim-badge {status_class(concept['status'])}" data-status="{esc(concept['status'])}">{esc(status_label(concept['status']))}</span>
          <span>{esc(concept['category'])}</span>
        </div>
      </div>
    </section>
    <section class="doc-section">
      <div class="shell dossier-layout">
        <aside class="dossier-sidebar sticky-in-page-toc">
          <h2>In this dossier</h2>
          <a href="#meaning">Plain-English meaning</a>
          <a href="#why">Why it matters</a>
          <a href="#how">How ShardLoom uses it</a>
          <a href="#support">Current support</a>
          <a href="#evidence">Evidence fields</a>
          <a href="#boundary">Claim boundary</a>
          <a href="#try-it">Try it / use cases</a>
          <a href="#related">Related concepts</a>
          <a href="#sources">Reference files</a>
        </aside>
        <article class="dossier-body">
          <section id="meaning">
            <p class="eyebrow">Plain-English meaning</p>
            <p>{esc(concept['answer'])}</p>
          </section>
          <section id="why">
            <p class="eyebrow">Why it matters</p>
            <p>{esc(concept['why'])}</p>
          </section>
          <section id="how">
            <p class="eyebrow">How ShardLoom uses it</p>
            <p>{esc(concept['how'])}</p>
          </section>
          <section id="support">
            <p class="eyebrow">Current support</p>
            <p>{inline_markdown(concept['current_support'])}</p>
          </section>
          <section id="evidence">
            <p class="eyebrow">Evidence fields</p>
            <p>{esc(concept['proves'])}</p>
            {bullet_list([f"`{field}`" for field in concept['evidence']])}
          </section>
          <section id="boundary">
            <p class="eyebrow">Claim boundary</p>
            <h3>What it does not claim</h3>
            <p>{esc(concept['not_proves'])}</p>
            <p>{esc(concept['boundary'])}</p>
          </section>
          <section id="try-it">
            <p class="eyebrow">Try it / related use cases</p>
            <p>{esc(concept['try_it'])}</p>
            {related_use_cases}
          </section>
          <section id="related" class="related-concepts">
            <p class="eyebrow">Related concepts</p>
            {related}
          </section>
          <section id="sources">
            <p class="eyebrow">Reference files</p>
            {source_links}
          </section>
          <nav class="dossier-nav" aria-label="Dossier navigation">
            <a class="button" href="/field-guide/">Field Guide index</a>
            {prev_link}
            {next_link}
          </nav>
        </article>
      </div>
    </section>
    """
    return page(
        f"{concept['title']} - ShardLoom Field Guide",
        concept["answer"],
        body,
        "field-guide",
        f"field-guide/{concept['slug']}",
        pagefind_filters={
            "section": "Field Guide",
            "category": concept["category"],
            "status": concept["status"],
        },
    )


def value_list(values: Any) -> list[str]:
    if isinstance(values, list):
        return [str(value) for value in values]
    if values is None:
        return []
    return [str(values)]


def status_label(status: str) -> str:
    return status.replace("_", " ").replace("-", " ")


def status_class(status: str) -> str:
    classes = {
        "ready_local": "supported",
        "runtime_supported": "supported",
        "smoke_supported": "fixture",
        "report_only": "report-only",
        "planned": "planned",
        "not_planned": "blocked",
        "blocked": "blocked",
        "unsupported": "blocked",
    }
    return classes.get(status, "report-only")


def list_markup(values: list[str]) -> str:
    if not values:
        return "<span>none</span>"
    return "<ul>" + "".join(f"<li>{inline_markdown(value)}</li>" for value in values) + "</ul>"


def inline_csv(values: list[str]) -> str:
    return ", ".join(values) if values else "none"


def platform_for_use_case(use_case: dict[str, Any]) -> str:
    text = " ".join(
        value_list(use_case.get("inputs"))
        + value_list(use_case.get("outputs"))
        + value_list(use_case.get("references"))
        + [str(use_case.get("title", ""))]
    ).lower()
    if "foundry" in text:
        return "foundry-local"
    if "s3" in text or "object-store" in text or "gcs" in text or "adls" in text:
        return "cloud-boundary"
    if "package" in text or "release" in text:
        return "package-channel"
    return "local"


def evidence_level_for_use_case(use_case: dict[str, Any]) -> str:
    fields = " ".join(value_list(use_case.get("evidence_fields"))).lower()
    if "generated_source" in fields:
        return "generated-source"
    if "result_sink" in fields or "result_replay" in fields:
        return "result-sink"
    if "native_io" in fields:
        return "native-io"
    if "benchmark" in fields or "millis" in fields:
        return "benchmark"
    if "claim_gate" in fields:
        return "claim-gate"
    return "capability"


def use_case_plain_summary(use_case: dict[str, Any]) -> str:
    status = str(use_case.get("status", "unknown"))
    title = str(use_case.get("title", "Use case"))
    if status in {"ready_local", "smoke_supported"}:
        return f"{title} has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary."
    if status == "report_only":
        return f"{title} is inspectable as posture or diagnostics, but it is not broad runtime support."
    if status == "planned":
        return f"{title} is planned. The blocker and evidence requirements are part of the current public posture."
    return f"{title} is blocked or unsupported until the listed evidence exists."


def render_use_case_status_table(use_case: dict[str, Any]) -> str:
    rows = [
        ("Status", status_label(str(use_case.get("status", "")))),
        ("Audience", str(use_case.get("audience", ""))),
        ("Execution mode", str(use_case.get("execution_mode", ""))),
        ("Engine mode", str(use_case.get("engine_mode", ""))),
        ("Platform", platform_for_use_case(use_case)),
        ("Evidence level", evidence_level_for_use_case(use_case)),
        ("Inputs", inline_csv(value_list(use_case.get("inputs")))),
        ("Outputs", inline_csv(value_list(use_case.get("outputs")))),
    ]
    return html_table(["Field", "Value"], rows)


def render_reference_links(references: list[str]) -> str:
    return render_citation_links(references)


def use_case_markdown(use_case: dict[str, Any]) -> str:
    references = value_list(use_case.get("references"))
    related_terms = field_guide_concepts_for_use_case(str(use_case["id"]))
    lines = [
        "<!-- SPDX-License-Identifier: Apache-2.0 -->",
        "",
        f"# {use_case['title']}",
        "",
        "## Quick Answer",
        "",
        f"- **Audience:** {use_case['audience']}",
        f"- **Status:** `{use_case['status']}`",
        f"- **Execution mode:** `{use_case['execution_mode']}`",
        f"- **Engine mode:** `{use_case['engine_mode']}`",
        f"- **Claim boundary:** {use_case['claim_boundary']}",
        "",
        "## Can ShardLoom Do This?",
        "",
        use_case_plain_summary(use_case),
        "",
        "## Claim Boundary",
        "",
        str(use_case["claim_boundary"]),
        "",
    ]
    if use_case.get("runnable_example"):
        lines.extend(
            [
                "## How To Try It",
                "",
                "```powershell",
                str(use_case["runnable_example"]),
                "```",
                "",
            ]
        )
    if use_case.get("blocked_explanation"):
        lines.extend(["## Blocker", "", str(use_case["blocked_explanation"]), ""])
    lines.extend(
        [
            "## Internal Flow",
            "",
            f"`{inline_csv(value_list(use_case.get('inputs')))} -> {use_case['execution_mode']} -> {use_case['engine_mode']} -> {inline_csv(value_list(use_case.get('outputs')))} -> evidence -> claim gate`",
            "",
            "## Evidence You Should See",
            "",
        ]
    )
    lines.extend(f"- `{field}`" for field in value_list(use_case.get("evidence_fields")))
    lines.extend(
        [
            "",
            "## Expected Output Or Evidence",
            "",
            str(use_case["expected_output_evidence"]),
            "",
            "## Common Mistakes",
            "",
        ]
    )
    lines.extend(f"- `{mistake}`" for mistake in value_list(use_case.get("common_mistakes")))
    lines.extend(["", "## Reference Files", ""])
    lines.extend(
        f"- `{reference}` - What this proves: {citation_proof(reference)}"
        for reference in references
    )
    lines.extend(["", "## Related Use Cases", ""])
    lines.extend(f"- `{related}`" for related in value_list(use_case.get("related_use_cases")))
    lines.extend(["", "## Related Field Guide Terms", ""])
    if related_terms:
        lines.extend(
            f"- `website/field-guide/{concept['slug']}.html` - {concept['title']} "
            f"(`{concept['category']}` / `{concept['status']}`)"
            for concept in related_terms
        )
    else:
        lines.append("- No related Field Guide terms are attached yet.")
    lines.append("")
    return "\n".join(lines)


def use_case_page(use_case: dict[str, Any], by_id: dict[str, dict[str, Any]]) -> str:
    use_case_id = str(use_case["id"])
    status = str(use_case["status"])
    related = [
        by_id[related_id]
        for related_id in value_list(use_case.get("related_use_cases"))
        if related_id in by_id
    ]
    related_cards = "".join(
        f'<a class="related-use-case" href="/use-cases/{esc(related_case["id"])}">'
        f'<span>{esc(status_label(str(related_case["status"])))}</span>'
        f'<strong>{esc(related_case["title"])}</strong></a>'
        for related_case in related
    )
    quick_example = ""
    if use_case.get("runnable_example"):
        quick_example = (
            "<h2>Quick Example</h2>"
            f'<pre><code data-language="powershell">{esc(use_case["runnable_example"])}</code></pre>'
        )
    blocker = ""
    if use_case.get("blocked_explanation"):
        blocker = (
            '<div class="notice-panel use-case-blocker">'
            "<strong>Current blocker.</strong>"
            f"<span>{inline_markdown(str(use_case['blocked_explanation']))}</span>"
            "</div>"
        )
    flow_steps = [
        ("Inputs", inline_csv(value_list(use_case.get("inputs")))),
        ("Execution mode", str(use_case.get("execution_mode", ""))),
        ("Engine mode", str(use_case.get("engine_mode", ""))),
        ("Outputs", inline_csv(value_list(use_case.get("outputs")))),
        ("Evidence", inline_csv(value_list(use_case.get("evidence_fields")))),
        ("Claim gate", str(use_case.get("claim_boundary", ""))),
    ]
    flow_html = "".join(
        f"<article><strong>{esc(label)}</strong><span>{inline_markdown(value)}</span></article>"
        for label, value in flow_steps
    )
    body = f"""
    <section class="doc-hero use-case-hero">
      <div class="shell">
        {page_header_logo()}
        <p class="eyebrow">Use Case Atlas</p>
        <h1>{esc(use_case["title"])}</h1>
        <p class="lede">{esc(use_case_plain_summary(use_case))}</p>
        <span class="claim-badge {status_class(status)}">{esc(status_label(status))}</span>
      </div>
    </section>
    <section class="doc-section">
      <div class="shell use-case-layout">
        <article class="doc-body">
          <h2>Plain-English Summary</h2>
          <p>{esc(use_case_plain_summary(use_case))}</p>
          <h2>Status Table</h2>
          {render_use_case_status_table(use_case)}
          {quick_example}
          {blocker}
          <h2>Claim Boundary</h2>
          <p>{inline_markdown(str(use_case["claim_boundary"]))}</p>
          <h2>Internal Flow</h2>
          <div class="use-case-flow">{flow_html}</div>
          <h2>Expected Evidence Fields</h2>
          {list_markup(value_list(use_case.get("evidence_fields")))}
          <h2>Expected Output Or Evidence</h2>
          <p>{inline_markdown(str(use_case["expected_output_evidence"]))}</p>
          <h2>Common Mistakes</h2>
          {list_markup(value_list(use_case.get("common_mistakes")))}
          <h2>Reference Files</h2>
          {render_reference_links(value_list(use_case.get("references")))}
          <h2>Related Field Guide Terms</h2>
          {related_field_guide_term_links(use_case_id)}
          <h2>Related Use Cases</h2>
          <div class="related-use-cases">{related_cards}</div>
        </article>
      </div>
    </section>
    """
    return page(
        f"{use_case['title']} | ShardLoom Use Cases",
        f"ShardLoom use-case posture for {use_case['title']}.",
        body,
        "use-cases",
        canonical_path_override=f"use-cases/{use_case_id}",
        pagefind_filters={
            "section": "Use Case Atlas",
            "status": status,
            "execution_mode": use_case.get("execution_mode"),
            "engine_mode": use_case.get("engine_mode"),
        },
    )


def use_cases_index_page(use_cases: list[dict[str, Any]]) -> str:
    statuses = sorted({str(use_case["status"]) for use_case in use_cases})
    execution_modes = sorted({str(use_case["execution_mode"]) for use_case in use_cases})
    input_types = sorted({value for use_case in use_cases for value in value_list(use_case.get("inputs"))})
    output_types = sorted({value for use_case in use_cases for value in value_list(use_case.get("outputs"))})
    evidence_levels = sorted({evidence_level_for_use_case(use_case) for use_case in use_cases})
    platforms = sorted({platform_for_use_case(use_case) for use_case in use_cases})

    def select(name: str, label: str, values: list[str]) -> str:
        options = ['<option value="">All</option>'] + [
            f'<option value="{esc(value)}">{esc(status_label(value))}</option>' for value in values
        ]
        return (
            f'<label><span>{esc(label)}</span><select data-use-case-filter="{esc(name)}">'
            + "".join(options)
            + "</select></label>"
        )

    cards = []
    for use_case in use_cases:
        status = str(use_case["status"])
        cards.append(
            f'<article class="use-case-card" data-status="{esc(status)}" '
            f'data-inputs="{esc(" ".join(value_list(use_case.get("inputs"))))}" '
            f'data-outputs="{esc(" ".join(value_list(use_case.get("outputs"))))}" '
            f'data-execution-mode="{esc(str(use_case["execution_mode"]))}" '
            f'data-evidence-level="{esc(evidence_level_for_use_case(use_case))}" '
            f'data-platform="{esc(platform_for_use_case(use_case))}">'
            f'<span class="claim-badge {status_class(status)}">{esc(status_label(status))}</span>'
            f'<h3><a href="/use-cases/{esc(use_case["id"])}">{esc(use_case["title"])}</a></h3>'
            f'<p>{esc(use_case_plain_summary(use_case))}</p>'
            f'<dl><dt>Execution</dt><dd><code>{esc(use_case["execution_mode"])}</code></dd>'
            f'<dt>Engine</dt><dd><code>{esc(use_case["engine_mode"])}</code></dd>'
            f'<dt>Evidence</dt><dd>{esc(evidence_level_for_use_case(use_case))}</dd></dl>'
            "</article>"
        )

    body = f"""
    <section class="doc-hero use-case-hero">
      <div class="shell">
        {page_header_logo()}
        <p class="eyebrow">Can I use this?</p>
        <h1>ShardLoom Use Case Atlas.</h1>
        <p class="lede">A non-expert map for what ShardLoom can do locally today, what is smoke-supported, what is report-only, and what remains planned or blocked. This is a technical-preview status surface, not a production or performance claim.</p>
      </div>
    </section>
    <section class="use-case-filter-section">
      <div class="shell">
        <form class="use-case-filters" data-use-case-filters>
          {select("status", "Status", statuses)}
          {select("input", "Input type", input_types)}
          {select("output", "Output type", output_types)}
          {select("execution", "Execution mode", execution_modes)}
          {select("evidence", "Evidence level", evidence_levels)}
          {select("platform", "Platform", platforms)}
          <button type="reset">Reset</button>
        </form>
        <p class="use-case-filter-count" data-use-case-count>{len(use_cases)} use cases shown</p>
      </div>
    </section>
    <section>
      <div class="shell">
        <div class="use-case-grid" data-use-case-grid>{"".join(cards)}</div>
      </div>
    </section>
    <section class="doc-section">
      <div class="shell">
        <h2>Claim Boundary</h2>
        <div class="boundary-grid">
          <article><strong>No performance or superiority claim</strong><span>Rows describe posture and evidence, not a ranking.</span></article>
          <article><strong>No production SQL/DataFrame claim</strong><span>Report-only language stays visible where runtime support is absent.</span></article>
          <article><strong>No object-store/lakehouse/Foundry production claim</strong><span>Blocked and planned cards stay visible instead of being hidden.</span></article>
          <article><strong>No hidden fallback</strong><span>External engines are baseline context only and never ShardLoom fallback execution.</span></article>
        </div>
      </div>
    </section>
    <script src="/assets/use-cases.js" defer></script>
    """
    return page(
        "ShardLoom Use Case Atlas",
        "Non-expert use-case status matrix for ShardLoom technical-preview capabilities.",
        body,
        "use-cases",
        canonical_path_override="use-cases/",
        pagefind_filters={"section": "Use Case Atlas", "status": "status-matrix"},
    )


def write_use_case_pages() -> list[dict[str, Any]]:
    data = load_index(DOC_USE_CASES / "use-case-index.yml")
    use_cases = data["use_cases"]
    by_id = {str(use_case["id"]): use_case for use_case in use_cases}
    generated_docs = DOC_USE_CASES / "generated"
    generated_docs.mkdir(parents=True, exist_ok=True)
    USE_CASE_PAGES.mkdir(parents=True, exist_ok=True)
    for use_case in use_cases:
        use_case_id = str(use_case["id"])
        (generated_docs / f"{use_case_id}.md").write_text(
            use_case_markdown(use_case),
            encoding="utf-8",
        )
        (USE_CASE_PAGES / f"{use_case_id}.html").write_text(
            use_case_page(use_case, by_id),
            encoding="utf-8",
        )
    (USE_CASE_PAGES / "index.html").write_text(
        use_cases_index_page(use_cases),
        encoding="utf-8",
    )
    return use_cases


def write_sitemap(use_cases: list[dict[str, Any]]) -> None:
    paths = [
        ("", "1.0"),
        ("benchmarks", "0.8"),
        ("field-guide/", "0.8"),
        ("use-cases/", "0.9"),
        ("status", "0.8"),
        ("compute-engine-flow", "0.8"),
        ("readme", "0.7"),
    ]
    paths.extend((f"field-guide/{concept['slug']}", "0.6") for concept in FIELD_GUIDE_CONCEPTS)
    paths.extend((f"use-cases/{use_case['id']}", "0.6") for use_case in use_cases)
    urls = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for path, priority in paths:
        loc = "https://shardloom.io/" + path
        urls.extend(
            [
                "  <url>",
                f"    <loc>{esc(loc)}</loc>",
                f"    <lastmod>{SITE_LASTMOD}</lastmod>",
                "    <changefreq>weekly</changefreq>",
                f"    <priority>{priority}</priority>",
                "  </url>",
            ]
        )
    urls.append("</urlset>")
    (WEBSITE / "sitemap.xml").write_text("\n".join(urls) + "\n", encoding="utf-8")


def write_field_guide_pages() -> None:
    target_dir = WEBSITE / "field-guide"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "index.html").write_text(
        clean_generated_html(field_guide_index_page()),
        encoding="utf-8",
    )
    for index, concept in enumerate(FIELD_GUIDE_CONCEPTS):
        previous_concept = FIELD_GUIDE_CONCEPTS[index - 1] if index > 0 else None
        next_concept = (
            FIELD_GUIDE_CONCEPTS[index + 1]
            if index + 1 < len(FIELD_GUIDE_CONCEPTS)
            else None
        )
        (target_dir / f"{concept['slug']}.html").write_text(
            clean_generated_html(
                field_guide_concept_page(concept, previous_concept, next_concept)
            ),
            encoding="utf-8",
        )


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def output_fields(payload: dict[str, Any]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for group in (
        (payload.get("result") or {}).get("fields", []),
        payload.get("fields", []),
    ):
        for row in group:
            if isinstance(row, dict) and "key" in row:
                fields[str(row["key"])] = str(row.get("value"))
    return fields


def batch_fused_pipeline_rows(
    benchmark_dir: Path, artifact_name: str, fields: dict[str, str]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    suffix = "_fused_pipeline_schema_version"
    for key in sorted(fields):
        if not key.startswith("scenario_") or not key.endswith(suffix):
            continue
        prefix = key[: -len(suffix)]
        rows.append(
            {
                "file": repo_relative_path(benchmark_dir / artifact_name),
                "generated_at_utc": "",
                "scenario": fields.get(
                    f"{prefix}_name", prefix.removeprefix("scenario_")
                ),
                "used": fields.get(f"{prefix}_fused_pipeline_used"),
                "family": fields.get(f"{prefix}_fused_operator_family"),
                "rows_scanned": fields.get(f"{prefix}_fused_pipeline_rows_scanned"),
                "rows_selected": fields.get(f"{prefix}_fused_pipeline_rows_selected"),
                "rows_output": fields.get(f"{prefix}_fused_pipeline_rows_output"),
                "materialization_avoided": fields.get(
                    f"{prefix}_intermediate_materialization_avoided"
                ),
                "data_decoded": fields.get(f"{prefix}_fused_pipeline_data_decoded"),
                "data_materialized": fields.get(
                    f"{prefix}_fused_pipeline_data_materialized"
                ),
                "claim_gate": fields.get(f"{prefix}_fused_pipeline_claim_gate_status"),
                "encoded_native_claim": fields.get(
                    f"{prefix}_fused_pipeline_encoded_native_claim_allowed"
                ),
                "fallback_attempted": fields.get(
                    f"{prefix}_fused_pipeline_fallback_attempted"
                ),
                "external_engine_invoked": fields.get(
                    f"{prefix}_fused_pipeline_external_engine_invoked"
                ),
            }
        )
    return rows


def batch_source_backed_scan_rows(
    benchmark_dir: Path, artifact_name: str, fields: dict[str, str]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    suffix = "_source_backed_scan_evidence_schema_version"
    for key in sorted(fields):
        if not key.startswith("scenario_") or not key.endswith(suffix):
            continue
        prefix = key[: -len(suffix)]
        rows.append(
            {
                "file": repo_relative_path(benchmark_dir / artifact_name),
                "generated_at_utc": "",
                "scenario": fields.get(
                    f"{prefix}_name", prefix.removeprefix("scenario_")
                ),
                "provider": fields.get(f"{prefix}_source_backed_scan_provider_kind"),
                "projected_columns": fields.get(
                    f"{prefix}_source_backed_scan_projected_columns"
                ),
                "pushdown_status": fields.get(f"{prefix}_scan_pushdown_status"),
                "scan_filter": fields.get(f"{prefix}_scan_filter_pushed_down"),
                "scan_projection": fields.get(f"{prefix}_scan_projection_pushed_down"),
                "scan_limit": fields.get(f"{prefix}_scan_limit_pushed_down"),
                "filter_columns": fields.get(f"{prefix}_scan_filter_columns_read"),
                "output_columns": fields.get(f"{prefix}_scan_output_columns_read"),
                "filter_only_columns": fields.get(
                    f"{prefix}_scan_filter_only_columns_read"
                ),
                "pushdown_blocker": fields.get(f"{prefix}_scan_pushdown_blocker_id"),
                "rows_scanned": fields.get(f"{prefix}_source_backed_scan_rows_scanned"),
                "data_materialized": fields.get(
                    f"{prefix}_source_backed_scan_data_materialized"
                ),
                "native_io": fields.get(
                    f"{prefix}_source_backed_scan_native_io_certificate_status"
                ),
                "claim_gate": fields.get(
                    f"{prefix}_source_backed_scan_claim_gate_status"
                ),
                "fallback_attempted": fields.get(
                    f"{prefix}_source_backed_scan_fallback_attempted"
                ),
                "external_engine_invoked": fields.get(
                    f"{prefix}_source_backed_scan_external_engine_invoked"
                ),
            }
        )
    return rows


def value_at(mapping: dict[str, Any], key: str) -> Any:
    value = mapping.get(key)
    return "n/a" if value is None else value


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 4)
    return value


def micros_field_to_millis(fields: dict[str, str], key: str) -> float:
    value = fields.get(key)
    if value in (None, ""):
        raise ValueError(f"benchmark batch artifact missing timing field {key}")
    try:
        return round(float(value) / 1000.0, 4)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"benchmark batch artifact field {key} must be numeric microseconds, got {value!r}"
        ) from exc


def benchmark_summary(benchmark_dir: Path) -> dict[str, Any]:
    harness_files = [
        "prepared_native_core.json",
        "prepared_native_dirty_csv.json",
        "prepared_native_nested_json.json",
        "prepared_native_null_heavy.json",
        "prepared_native_cdc_overlay.json",
    ]
    rows: list[dict[str, Any]] = []
    provider_rows: list[dict[str, Any]] = []
    source_rows: list[dict[str, Any]] = []
    fused_rows: list[dict[str, Any]] = []
    materialization_rows: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []

    for name in harness_files:
        path = benchmark_dir / name
        artifact = load_json(path)
        artifacts.append(
            {
                "file": repo_relative_path(path),
                "generated_at_utc": artifact.get("generated_at_utc"),
                "dataset_profile": artifact.get("dataset", {}).get("dataset_profile"),
                "rows": artifact.get("dataset", {}).get("rows"),
                "formats": artifact.get("format_order", []),
                "scenario_count": len(artifact.get("scenario_order", [])),
            }
        )
        for result in artifact.get("results", []):
            if not str(result.get("engine", "")).startswith("shardloom"):
                continue
            evidence = result.get("shardloom_evidence", {})
            metrics = result.get("metrics", {})
            row = {
                "scenario": result.get("scenario_name"),
                "engine": result.get("engine"),
                "storage_format": result.get("storage_format"),
                "status": result.get("status"),
                "selected_execution_mode": result.get("selected_execution_mode"),
                "claim_gate_status": result.get("claim_gate_status"),
                "query_runtime_millis": rounded(metrics.get("query_runtime_millis")),
                "scenario_compute_millis": rounded(metrics.get("scenario_compute_millis")),
                "source_read_millis": rounded(metrics.get("source_read_millis")),
                "compatibility_parse_millis": rounded(
                    metrics.get("compatibility_parse_millis")
                ),
                "compatibility_to_vortex_import_millis": rounded(
                    metrics.get("compatibility_to_vortex_import_millis")
                ),
                "vortex_write_millis": rounded(metrics.get("vortex_write_millis")),
                "vortex_reopen_millis": rounded(metrics.get("vortex_reopen_millis")),
                "vortex_scan_millis": rounded(metrics.get("vortex_scan_millis")),
                "operator_compute_millis": rounded(metrics.get("operator_compute_millis")),
                "result_sink_write_millis": rounded(metrics.get("result_sink_write_millis")),
                "evidence_render_millis": rounded(metrics.get("evidence_render_millis")),
                "total_runtime_millis": rounded(metrics.get("total_runtime_millis")),
                "operator_execution_class": result.get("operator_execution_class"),
                "source_metadata_snapshot_status": evidence.get(
                    "source_metadata_snapshot_status"
                ),
                "source_metadata_snapshot_reused": evidence.get(
                    "source_metadata_snapshot_reused"
                ),
                "source_state_reuse_status": evidence.get("source_state_reuse_status"),
                "source_state_family_count": evidence.get("source_state_family_count"),
                "source_state_prepare_micros": evidence.get("source_state_prepare_micros"),
                "source_state_date_null_metric_reuse_status": evidence.get(
                    "source_state_date_null_metric_reuse_status"
                ),
                "native_io_certificate_status": evidence.get(
                    "native_io_certificate_status"
                ),
                "materialization_boundary_report_emitted": evidence.get(
                    "materialization_boundary_report_emitted"
                ),
                "fallback_attempted": result.get("fallback_attempted", False),
                "external_engine_invoked": result.get("external_engine_invoked", False),
            }
            rows.append(row)
            provider_status = evidence.get("encoded_predicate_provider_status")
            if (
                provider_status
                and provider_status != "not_applicable_no_selective_filter_predicate"
            ):
                provider_rows.append(
                    {
                        "file": repo_relative_path(path),
                        "generated_at_utc": artifact.get("generated_at_utc"),
                        "scenario": result.get("scenario_name"),
                        "status": provider_status,
                        "classification": evidence.get(
                            "encoded_predicate_provider_classification"
                        ),
                        "filter_columns": evidence.get(
                            "encoded_predicate_provider_filter_only_columns"
                        ),
                        "encoding_summary": evidence.get(
                            "encoded_predicate_provider_filter_column_probe_reader_chunk_encoding_summary"
                        ),
                        "selection_vector_consumed": evidence.get(
                            "encoded_predicate_provider_selected_metric_selection_vector_consumed"
                        ),
                        "selected_rows": evidence.get(
                            "encoded_predicate_provider_selected_metric_row_count"
                        ),
                        "selected_metric_sum": evidence.get(
                            "encoded_predicate_provider_selected_metric_sum"
                        ),
                        "data_decoded": evidence.get(
                            "encoded_predicate_provider_selected_metric_data_decoded"
                        ),
                        "data_materialized": evidence.get(
                            "encoded_predicate_provider_selected_metric_data_materialized"
                        ),
                        "claim_allowed": evidence.get(
                            "encoded_predicate_provider_encoded_native_claim_allowed"
                        ),
                    }
                )
            if evidence.get("source_backed_scan_evidence_status"):
                source_rows.append(
                    {
                        "file": repo_relative_path(path),
                        "generated_at_utc": artifact.get("generated_at_utc"),
                        "scenario": result.get("scenario_name"),
                        "provider": evidence.get("source_backed_scan_provider_kind"),
                        "projected_columns": evidence.get(
                            "source_backed_scan_projected_columns"
                        ),
                        "pushdown_status": evidence.get(
                            "scan_pushdown_status", "not_reported_older_artifact"
                        ),
                        "scan_filter": evidence.get(
                            "scan_filter_pushed_down", "not_reported"
                        ),
                        "scan_projection": evidence.get(
                            "scan_projection_pushed_down", "not_reported"
                        ),
                        "scan_limit": evidence.get(
                            "scan_limit_pushed_down", "not_reported"
                        ),
                        "filter_columns": evidence.get(
                            "scan_filter_columns_read", "not_reported"
                        ),
                        "output_columns": evidence.get(
                            "scan_output_columns_read", "not_reported"
                        ),
                        "filter_only_columns": evidence.get(
                            "scan_filter_only_columns_read", "not_reported"
                        ),
                        "pushdown_blocker": evidence.get(
                            "scan_pushdown_blocker_id", "not_reported"
                        ),
                        "rows_scanned": evidence.get("source_backed_scan_rows_scanned"),
                        "data_materialized": evidence.get(
                            "source_backed_scan_data_materialized"
                        ),
                        "native_io": evidence.get(
                            "source_backed_scan_native_io_certificate_status"
                        ),
                        "claim_gate": evidence.get("source_backed_scan_claim_gate_status"),
                        "fallback_attempted": evidence.get(
                            "source_backed_scan_fallback_attempted"
                        ),
                        "external_engine_invoked": evidence.get(
                            "source_backed_scan_external_engine_invoked"
                        ),
                    }
                )
            if evidence.get("fused_pipeline_schema_version"):
                fused_rows.append(
                    {
                        "file": repo_relative_path(path),
                        "generated_at_utc": artifact.get("generated_at_utc"),
                        "scenario": result.get("scenario_name"),
                        "used": evidence.get("fused_pipeline_used"),
                        "family": evidence.get("fused_operator_family"),
                        "rows_scanned": evidence.get("fused_pipeline_rows_scanned"),
                        "rows_selected": evidence.get("fused_pipeline_rows_selected"),
                        "rows_output": evidence.get("fused_pipeline_rows_output"),
                        "materialization_avoided": evidence.get(
                            "intermediate_materialization_avoided"
                        ),
                        "data_decoded": evidence.get("fused_pipeline_data_decoded"),
                        "data_materialized": evidence.get(
                            "fused_pipeline_data_materialized"
                        ),
                        "claim_gate": evidence.get(
                            "fused_pipeline_claim_gate_status"
                        ),
                        "encoded_native_claim": evidence.get(
                            "fused_pipeline_encoded_native_claim_allowed"
                        ),
                        "fallback_attempted": evidence.get(
                            "fused_pipeline_fallback_attempted"
                        ),
                        "external_engine_invoked": evidence.get(
                            "fused_pipeline_external_engine_invoked"
                        ),
                    }
                )
            if result.get("engine") == "shardloom-prepared-vortex":
                materialization_rows.append(
                    {
                        "scenario": result.get("scenario_name"),
                        "data_decoded": evidence.get("data_decoded"),
                        "data_materialized": evidence.get("data_materialized"),
                        "row_read": evidence.get("row_read"),
                        "arrow_converted": evidence.get("arrow_converted"),
                        "boundary": evidence.get(
                            "materialization_boundary_report_emitted"
                        ),
                        "native_io": evidence.get("native_io_certificate_status"),
                        "fallback_attempted": evidence.get("fallback_attempted"),
                        "external_engine_invoked": evidence.get(
                            "external_engine_invoked"
                        ),
                    }
                )

    batch_rows = []
    for name in ("prepared_vortex_batch.json", "native_vortex_batch.json"):
        payload = load_json(benchmark_dir / name)
        fields = output_fields(payload)
        source_rows.extend(batch_source_backed_scan_rows(benchmark_dir, name, fields))
        fused_rows.extend(batch_fused_pipeline_rows(benchmark_dir, name, fields))
        batch_rows.append(
            {
                "file": repo_relative_path(benchmark_dir / name),
                "requested_execution_mode": fields.get("requested_execution_mode"),
                "selected_execution_modes": fields.get("selected_execution_modes"),
                "runner_kind": fields.get("runner_kind"),
                "scenario_count": fields.get("scenario_count"),
                "scenario_order": fields.get("scenario_order"),
                "total_scenario_compute_millis": micros_field_to_millis(
                    fields, "total_scenario_compute_micros"
                ),
                "total_vortex_scan_millis": micros_field_to_millis(
                    fields, "total_vortex_scan_micros"
                ),
                "total_result_sink_write_millis": micros_field_to_millis(
                    fields, "total_result_sink_write_micros"
                ),
                "runtime_evidence_level_schema_version": fields.get(
                    "runtime_evidence_level_schema_version"
                ),
                "requested_evidence_level": fields.get("requested_evidence_level"),
                "selected_evidence_level": fields.get("selected_evidence_level"),
                "evidence_level": fields.get("evidence_level"),
                "evidence_level_supported_levels": fields.get(
                    "evidence_level_supported_levels"
                ),
                "evidence_level_claim_gate_status": fields.get(
                    "evidence_level_claim_gate_status"
                ),
                "evidence_level_result_sink_replay_required": fields.get(
                    "evidence_level_result_sink_replay_required"
                ),
                "evidence_level_result_sink_replay_requested": fields.get(
                    "evidence_level_result_sink_replay_requested"
                ),
                "evidence_level_result_sink_replay_verified": fields.get(
                    "evidence_level_result_sink_replay_verified"
                ),
                "evidence_level_native_io_certificate_required": fields.get(
                    "evidence_level_native_io_certificate_required"
                ),
                "evidence_level_source_state_digest": fields.get(
                    "evidence_level_source_state_digest"
                ),
                "evidence_level_output_digest": fields.get(
                    "evidence_level_output_digest"
                ),
                "evidence_level_claim_boundary": fields.get(
                    "evidence_level_claim_boundary"
                ),
                "source_metadata_snapshot_status": fields.get(
                    "source_metadata_snapshot_status"
                ),
                "source_metadata_snapshot_reused": fields.get(
                    "source_metadata_snapshot_reused"
                ),
                "source_metadata_snapshot_reuse_count": fields.get(
                    "source_metadata_snapshot_reuse_count"
                ),
                "source_metadata_digest_recompute_avoided_count": fields.get(
                    "source_metadata_digest_recompute_avoided_count"
                ),
                "source_state_reuse_status": fields.get("source_state_reuse_status"),
                "source_state_coverage_schema_version": fields.get(
                    "source_state_coverage_schema_version"
                ),
                "source_state_coverage_matrix_ref": fields.get(
                    "source_state_coverage_matrix_ref"
                ),
                "source_state_coverage_status_vocabulary": fields.get(
                    "source_state_coverage_status_vocabulary"
                ),
                "source_state_coverage_all_requested_scenarios_classified": fields.get(
                    "source_state_coverage_all_requested_scenarios_classified"
                ),
                "source_state_coverage_matrix": fields.get(
                    "source_state_coverage_matrix"
                ),
                "source_state_coverage_reused_scenario_count": fields.get(
                    "source_state_coverage_reused_scenario_count"
                ),
                "source_state_coverage_not_needed_scenario_count": fields.get(
                    "source_state_coverage_not_needed_scenario_count"
                ),
                "source_state_coverage_blocked_scenario_count": fields.get(
                    "source_state_coverage_blocked_scenario_count"
                ),
                "source_state_coverage_unsupported_scenario_count": fields.get(
                    "source_state_coverage_unsupported_scenario_count"
                ),
                "source_state_digest_status": fields.get("source_state_digest_status"),
                "source_state_digest_reason": fields.get("source_state_digest_reason"),
                "source_state_reused": fields.get("source_state_reused"),
                "source_state_family_count": fields.get("source_state_family_count"),
                "source_state_reuse_consumer_count": fields.get(
                    "source_state_reuse_consumer_count"
                ),
                "source_state_recompute_avoided_count": fields.get(
                    "source_state_recompute_avoided_count"
                ),
                "source_state_prepare_millis": micros_field_to_millis(
                    fields, "source_state_prepare_micros"
                ),
                "source_state_prepare_timing_scope": fields.get(
                    "source_state_prepare_timing_scope"
                ),
                "source_state_selective_filter_reuse_status": fields.get(
                    "source_state_selective_filter_reuse_status"
                ),
                "source_state_date_null_metric_reuse_status": fields.get(
                    "source_state_date_null_metric_reuse_status"
                ),
                "source_state_claim_boundary": fields.get("source_state_claim_boundary"),
                "session_schema_version": fields.get("session_schema_version"),
                "session_id": fields.get("session_id"),
                "session_runtime_status": fields.get("session_runtime_status"),
                "session_state_scope": fields.get("session_state_scope"),
                "session_close_status": fields.get("session_close_status"),
                "session_prepared_artifact_reuse_count": fields.get(
                    "session_prepared_artifact_reuse_count"
                ),
                "session_source_metadata_cache_hit_count": fields.get(
                    "session_source_metadata_cache_hit_count"
                ),
                "session_source_state_reuse_count": fields.get(
                    "session_source_state_reuse_count"
                ),
                "session_hidden_global_cache": fields.get("session_hidden_global_cache"),
                "session_daemon_or_service": fields.get("session_daemon_or_service"),
                "session_fallback_attempted": fields.get("session_fallback_attempted"),
                "session_external_engine_invoked": fields.get(
                    "session_external_engine_invoked"
                ),
                "session_claim_gate_status": fields.get("session_claim_gate_status"),
                "allocation_profile_schema_version": fields.get(
                    "allocation_profile_schema_version"
                ),
                "allocation_profile_status": fields.get("allocation_profile_status"),
                "allocation_profile_scope": fields.get("allocation_profile_scope"),
                "allocation_profile_family_status": fields.get(
                    "allocation_profile_family_status"
                ),
                "allocation_count": fields.get("allocation_count"),
                "allocation_count_status": fields.get("allocation_count_status"),
                "allocation_bytes": fields.get("allocation_bytes"),
                "allocation_bytes_status": fields.get("allocation_bytes_status"),
                "buffer_pool_enabled": fields.get("buffer_pool_enabled"),
                "buffer_pool_scope": fields.get("buffer_pool_scope"),
                "buffer_reuse_count": fields.get("buffer_reuse_count"),
                "buffer_reuse_family": fields.get("buffer_reuse_family"),
                "buffer_reuse_blocker": fields.get("buffer_reuse_blocker"),
                "peak_rss_delta": fields.get("peak_rss_delta"),
                "peak_rss_delta_status": fields.get("peak_rss_delta_status"),
                "unsafe_lifetime_shortcut_used": fields.get(
                    "unsafe_lifetime_shortcut_used"
                ),
                "allocation_claim_gate_status": fields.get(
                    "allocation_claim_gate_status"
                ),
                "claim_gate_status": fields.get("claim_gate_status"),
                "fallback_attempted": fields.get("fallback_attempted"),
                "external_engine_invoked": fields.get("external_engine_invoked"),
                "performance_claim_allowed": fields.get("performance_claim_allowed"),
            }
        )

    table_fields = output_fields(load_json(benchmark_dir / "local_table_metadata_read_smoke.json"))
    table_metadata = {
        key: table_fields.get(key)
        for key in (
            "schema_version",
            "support_status",
            "claim_gate_status",
            "catalog_kind",
            "dataset_format",
            "declared_row_count",
            "partition_count",
            "fallback_attempted",
            "external_engine_invoked",
            "performance_claim_allowed",
            "claim_boundary",
        )
    }

    return {
        "schema_version": "shardloom.website.benchmark_evidence.v1",
        "source_artifact_dir": repo_relative_path(benchmark_dir),
        "artifacts": artifacts,
        "rows": rows,
        "batch_rows": batch_rows,
        "encoded_predicate_provider_rows": provider_rows,
        "source_backed_scan_rows": source_rows,
        "fused_pipeline_rows": fused_rows,
        "materialization_rows": materialization_rows,
        "table_metadata_smoke": table_metadata,
        "claim_boundary": {
            "performance_claim_allowed": False,
            "spark_replacement_claim_allowed": False,
            "production_sql_dataframe_claim_allowed": False,
            "production_object_store_lakehouse_foundry_claim_allowed": False,
            "scope": "local one-iteration smoke evidence and direct CLI evidence only",
        },
    }


def html_table(headers: list[str], rows: list[list[Any]]) -> str:
    body = ['<div class="table-scroll"><table>']
    body.append(
        "<thead><tr>"
        + "".join(f"<th>{esc(header)}</th>" for header in headers)
        + "</tr></thead><tbody>"
    )
    for row in rows:
        body.append(
            "<tr>" + "".join(f"<td>{esc(cell)}</td>" for cell in row) + "</tr>"
        )
    body.append("</tbody></table></div>")
    return "\n".join(body)


def details_block(summary: str, body: str, class_name: str = "raw-data-drawer") -> str:
    return (
        f'<details class="{esc(class_name)}">'
        f"<summary>{esc(summary)}</summary>"
        f"{body}</details>"
    )


def latest_generated_at(rows: list[dict[str, Any]]) -> str:
    values = [str(row.get("generated_at_utc", "")) for row in rows if row.get("generated_at_utc")]
    return max(values) if values else ""


def latest_artifact_generated_at(summary: dict[str, Any]) -> str:
    return latest_generated_at(summary.get("artifacts", []))


def benchmark_available_lanes(summary: dict[str, Any]) -> list[str]:
    lanes = {
        str(row.get("engine"))
        for row in summary.get("rows", [])
        if row.get("engine")
    }
    for row in summary.get("batch_rows", []):
        selected = str(row.get("selected_execution_modes") or "")
        if "prepared_vortex" in selected:
            lanes.add("shardloom-prepared-vortex")
        if "native_vortex" in selected:
            lanes.add("shardloom-vortex")
            lanes.add("native-vortex")
    return sorted(lanes)


def benchmark_environment_snapshot() -> dict[str, Any]:
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "website_generator": "website/build_static_pages.py",
    }


def benchmark_manifest_from_summary(
    summary: dict[str, Any],
    results_path: Path,
    profile_name: str = "smoke",
) -> dict[str, Any]:
    available = benchmark_available_lanes(summary)
    expected = list(expected_lanes_for_profile(profile_name))
    missing = [lane for lane in expected if lane not in available]
    missing_required = [
        lane
        for lane in missing
        if lane_required_for_profile(profile_name, lane)
    ]
    reasons = {
        lane: "available in committed website benchmark artifact"
        for lane in available
    }
    for lane in missing:
        reasons[lane] = (
            "not present in current committed smoke artifact; run the full benchmark "
            "publishing workflow before treating this as full-local evidence"
        )
    generated_at = latest_artifact_generated_at(summary) or datetime.now(timezone.utc).isoformat()
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generated_at_utc": generated_at,
        "benchmark_profile": profile_name,
        "benchmark_git_sha": None,
        "shardloom_git_sha": None,
        "artifact_status": "incomplete" if missing_required else "complete",
        "expected_lanes": expected,
        "available_lanes": available,
        "missing_lanes": missing,
        "missing_required_lanes": missing_required,
        "lane_versions": {lane: "from committed artifact" for lane in available},
        "lane_availability_reasons": reasons,
        "environment": benchmark_environment_snapshot(),
        "claim_boundary": PROFILES[profile_name].claim_boundary,
        "performance_claim_allowed": False,
        "artifact_paths": {
            "json": repo_relative_path(results_path),
            "markdown": None,
            "html": None,
        },
    }


def write_latest_benchmark_artifacts(summary: dict[str, Any]) -> dict[str, Any]:
    BENCHMARK_LATEST_DIR.mkdir(parents=True, exist_ok=True)
    results_path = BENCHMARK_LATEST_DIR / "benchmark-results.json"
    results_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    manifest = benchmark_manifest_from_summary(
        summary,
        results_path,
        summary.get("benchmark_profile", "smoke"),
    )
    manifest_path = BENCHMARK_LATEST_DIR / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def load_benchmark_summary_from_manifest(manifest_path: Path) -> dict[str, Any]:
    manifest = load_json_file(manifest_path)
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise ValueError(
            "benchmark manifest schema_version must be "
            f"{MANIFEST_SCHEMA_VERSION}, got {manifest.get('schema_version')}"
        )
    artifact_paths = manifest.get("artifact_paths") or {}
    json_ref = artifact_paths.get("json")
    if not json_ref:
        raise ValueError("benchmark manifest artifact_paths.json is required")
    summary_path = resolve_artifact_path(str(json_ref), manifest_path)
    summary = load_json_file(summary_path)
    if not isinstance(summary, dict):
        raise ValueError("benchmark artifact JSON must contain an object")
    summary["benchmark_manifest"] = manifest
    return summary


def benchmark_manifest_panel(summary: dict[str, Any]) -> str:
    manifest = summary.get("benchmark_manifest") or {}
    if not manifest:
        return ""
    expected = manifest.get("expected_lanes") or []
    available = set(manifest.get("available_lanes") or [])
    missing = set(manifest.get("missing_lanes") or [])
    reasons = manifest.get("lane_availability_reasons") or {}
    lane_rows = []
    for lane in expected:
        if lane in available:
            status = "available"
        elif lane in missing:
            status = "missing"
        else:
            status = "unreported"
        lane_rows.append(
            [
                lane,
                status,
                "required" if lane_required_for_profile(manifest["benchmark_profile"], lane) else "optional",
                reasons.get(lane, ""),
            ]
        )
    return f"""
        <div class="notice-panel benchmark-manifest-panel">
          <strong>Published artifact profile: <code>{esc(manifest.get('benchmark_profile'))}</code></strong>
          <span>Artifact status: <code>{esc(manifest.get('artifact_status', 'unknown'))}</code>. The website renders committed benchmark artifacts; it does not import pandas, Polars, DuckDB, Spark, DataFusion, Dask, Java, GPU, or extended optional baseline packages at page-render time.</span>
        </div>
        {details_block('Benchmark lane availability from manifest', html_table(['Expected lane', 'Status', 'Profile policy', 'Version / reason'], lane_rows), 'raw-data-drawer manifest-drawer')}
    """


def strip_html_fragment(fragment: str) -> str:
    text = html.unescape(fragment)
    width_match = re.search(r"width:\s*([0-9.]+%)", text)
    text = re.sub(r"<br\s*/?>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text and width_match:
        return width_match.group(1)
    if width_match and "bar-shell" in fragment:
        return f"{text} ({width_match.group(1)} relative)" if text else width_match.group(1)
    return text


def dashboard_source_label(path: Path) -> str:
    parts = list(path.resolve().parts)
    for marker in ("spark-retire", "shardloom-active"):
        if marker in parts:
            return "/".join(parts[parts.index(marker) :])
    return path.name


def extract_dashboard_table(source: str, heading: str) -> dict[str, Any]:
    match = re.search(
        rf"<h2>{re.escape(heading)}</h2>\s*<table>(.*?)</table>",
        source,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return {"heading": heading, "headers": [], "rows": []}
    table_html = match.group(1)
    raw_rows = re.findall(r"<tr>(.*?)</tr>", table_html, flags=re.IGNORECASE | re.DOTALL)
    parsed_rows = [
        [
            strip_html_fragment(cell)
            for cell in re.findall(
                r"<t[hd][^>]*>(.*?)</t[hd]>",
                row,
                flags=re.IGNORECASE | re.DOTALL,
            )
        ]
        for row in raw_rows
    ]
    return {
        "heading": heading,
        "headers": parsed_rows[0] if parsed_rows else [],
        "rows": parsed_rows[1:] if len(parsed_rows) > 1 else [],
    }


def relabel_dashboard_table(table: dict[str, Any]) -> dict[str, Any]:
    header_replacements = {
        "Fastest rows": "local fastest count",
        "Relative bar": "local timing context",
    }
    heading_replacements = {
        "Engine Timing Overview": "Local Timing Context",
    }
    return {
        **table,
        "heading": heading_replacements.get(table.get("heading"), table.get("heading")),
        "headers": [
            header_replacements.get(header, header)
            for header in table.get("headers", [])
        ],
    }


def comparative_dashboard_summary(path: Path) -> dict[str, Any]:
    source = path.read_text(encoding="utf-8")
    generated = ""
    generated_match = re.search(
        r"<div>Generated\s+([^<]+)</div>",
        source,
        flags=re.IGNORECASE,
    )
    if generated_match:
        generated = strip_html_fragment(generated_match.group(1))

    cards = [
        {"label": strip_html_fragment(label), "value": strip_html_fragment(value)}
        for label, value in re.findall(
            r'<div class="card"><div class="label">(.*?)</div><div class="value">(.*?)</div></div>',
            source,
            flags=re.IGNORECASE | re.DOTALL,
        )
    ]

    missing_baselines: list[str] = []
    missing_match = re.search(
        r"<h2>Missing Baselines</h2>\s*<ul>(.*?)</ul>",
        source,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if missing_match:
        missing_baselines = [
            strip_html_fragment(item)
            for item in re.findall(
                r"<li>(.*?)</li>",
                missing_match.group(1),
                flags=re.IGNORECASE | re.DOTALL,
            )
        ]

    return {
        "source": dashboard_source_label(path),
        "generated": generated,
        "cards": cards,
        "engine_timing_overview": relabel_dashboard_table(
            extract_dashboard_table(source, "Engine Timing Overview")
        ),
        "vortex_oriented_lanes": extract_dashboard_table(
            source,
            "Vortex-Oriented Lanes By Source Format",
        ),
        "claim_gate_distribution": extract_dashboard_table(source, "Claim-Gate Distribution"),
        "missing_baselines": missing_baselines,
        "claim_boundary": "comparative context only; not public performance, superiority, Spark-displacement, or best-default evidence",
    }


def dashboard_card_value(summary: dict[str, Any], label: str) -> str:
    for card in summary.get("cards", []):
        if card.get("label") == label:
            return card.get("value", "n/a")
    return "n/a"


def table_from_dashboard(table: dict[str, Any]) -> str:
    headers = table.get("headers") or []
    rows = table.get("rows") or []
    if not headers or not rows:
        return '<p class="empty-note">No local comparative dashboard table was available for this section.</p>'
    return html_table(headers, rows)


def dashboard_table_value(table: dict[str, Any], row_label: str, column: str) -> str:
    headers = table.get("headers") or []
    rows = table.get("rows") or []
    if column not in headers:
        return "n/a"
    column_index = headers.index(column)
    for row in rows:
        if row and row[0] == row_label and len(row) > column_index:
            return str(row[column_index])
    return "n/a"


def batch_row_value(rows: list[dict[str, Any]], mode: str, key: str) -> str:
    for row in rows:
        if row.get("requested_execution_mode") == mode:
            value = row.get(key)
            return "n/a" if value in (None, "") else str(value)
    return "n/a"


def compact_source_list(rows: list[dict[str, Any]]) -> str:
    files = sorted({str(row.get("file", "")) for row in rows if row.get("file")})
    if not files:
        return "target/shardloom-benchmark-evidence"
    if len(files) == 1:
        return files[0]
    return f"{files[0]} and {len(files) - 1} related artifact(s)"


def freshness_note(label: str, source: str, generated: str = "") -> str:
    generated_text = f" - {esc(generated)}" if generated else ""
    return (
        '<p class="freshness-label">'
        f"<strong>{esc(label)}</strong> Source: <code>{esc(source)}</code>{generated_text}"
        "</p>"
    )


def mode_comparison_visual(
    comparative: dict[str, Any] | None, batch_rows: list[dict[str, Any]]
) -> str:
    timing_table = (comparative or {}).get("engine_timing_overview", {})
    cards = [
        (
            "certified import/stage route geomean",
            dashboard_table_value(timing_table, "shardloom", "Geomean"),
            "certification lane with import, write/reopen, scan, sink, and evidence work",
        ),
        (
            "Vortex-oriented route geomean",
            dashboard_table_value(timing_table, "shardloom-vortex", "Geomean"),
            "Vortex-oriented local lane from the promoted benchmark artifact",
        ),
        (
            "prepared steady-state route geomean",
            dashboard_table_value(timing_table, "shardloom-prepared-vortex", "Geomean"),
            "prepared-artifact lane and current runtime-development direction",
        ),
        (
            "prepared_vortex batch smoke total/compute",
            f"{batch_row_value(batch_rows, 'prepared_vortex', 'total_scenario_compute_millis')} ms",
            "scoped session-backed batch runner structural smoke evidence",
        ),
        (
            "native_vortex batch smoke total/compute",
            f"{batch_row_value(batch_rows, 'native_vortex', 'total_scenario_compute_millis')} ms",
            "native Vortex batch runner structural smoke evidence",
        ),
    ]
    return (
        '<div class="mode-comparison-grid">'
        + "".join(
            '<article class="mode-comparison-card">'
            f"<span>{esc(label)}</span>"
            f"<strong>{esc(value)}</strong>"
            f"<p>{esc(detail)}</p>"
            "</article>"
            for label, value, detail in cards
        )
        + "</div>"
    )


def representative_timing_decomposition(rows: list[dict[str, Any]]) -> str:
    selected: list[dict[str, Any]] = []
    for mode in ("compatibility_import_certified", "prepared_vortex"):
        for row in rows:
            if row.get("selected_execution_mode") == mode:
                selected.append(row)
                break
    if not selected:
        return '<p class="empty-note">No representative timing rows were available.</p>'
    return html_table(
        [
            "Representative row",
            "Mode",
            "Compat parse ms",
            "Vortex import ms",
            "Vortex write ms",
            "Vortex scan ms",
            "Operator ms",
            "Result sink ms",
            "Evidence/render ms",
            "Total ms",
        ],
        [
            [
                row.get("scenario"),
                row.get("selected_execution_mode"),
                value_at(row, "compatibility_parse_millis"),
                value_at(row, "compatibility_to_vortex_import_millis"),
                value_at(row, "vortex_write_millis"),
                value_at(row, "vortex_scan_millis"),
                value_at(row, "operator_compute_millis"),
                value_at(row, "result_sink_write_millis"),
                value_at(row, "evidence_render_millis"),
                value_at(row, "total_runtime_millis"),
            ]
            for row in selected
        ],
    )


def benchmark_page(summary: dict[str, Any]) -> str:
    rows = summary["rows"]
    comparative = summary.get("comparative_dashboard")
    mode_table = html_table(
        [
            "Scenario",
            "Engine",
            "Mode",
            "Claim gate",
            "Query runtime ms",
            "Source read ms",
            "Compat parse ms",
            "Import ms",
            "Vortex write ms",
            "Vortex reopen ms",
            "Vortex scan ms",
            "Operator ms",
            "Sink ms",
            "Evidence ms",
            "Total ms",
            "Operator class",
            "Source metadata",
            "Source-state reuse",
            "Source-state families",
            "Fallback",
            "External engine",
        ],
        [
            [
                row["scenario"],
                row["engine"],
                row["selected_execution_mode"],
                row["claim_gate_status"],
                value_at(row, "query_runtime_millis"),
                value_at(row, "source_read_millis"),
                value_at(row, "compatibility_parse_millis"),
                value_at(row, "compatibility_to_vortex_import_millis"),
                value_at(row, "vortex_write_millis"),
                value_at(row, "vortex_reopen_millis"),
                value_at(row, "vortex_scan_millis"),
                value_at(row, "operator_compute_millis"),
                value_at(row, "result_sink_write_millis"),
                value_at(row, "evidence_render_millis"),
                value_at(row, "total_runtime_millis"),
                row["operator_execution_class"],
                value_at(row, "source_metadata_snapshot_status"),
                value_at(row, "source_state_reuse_status"),
                value_at(row, "source_state_family_count"),
                row["fallback_attempted"],
                row["external_engine_invoked"],
            ]
            for row in rows
        ],
    )
    batch_table = html_table(
        [
            "Requested mode",
            "Selected modes",
            "Runner",
            "Evidence level",
            "Requested evidence",
            "Evidence gate",
            "Replay required",
            "Replay verified",
            "Output digest",
            "Session",
            "Session close",
            "Artifact reuse",
            "Metadata hits",
            "State reuse",
            "Hidden global",
            "Daemon/service",
            "Scenarios",
            "Scenario compute ms",
            "Vortex scan ms",
            "Result sink ms",
            "Source metadata",
            "Source-state reuse",
            "Coverage classified",
            "Coverage reused",
            "Coverage not-needed",
            "Coverage blocked",
            "State digest",
            "Families",
            "Source-state prep ms",
            "Selective-filter reuse",
            "Date/null reuse",
            "Allocation profile",
            "Allocation count",
            "Allocation bytes",
            "Buffer pool",
            "Buffer reuse",
            "Buffer blocker",
            "Peak RSS delta",
            "Unsafe shortcut",
            "Allocation claim gate",
            "Claim gate",
            "Fallback",
            "External engine",
            "Performance claim",
        ],
        [
            [
                row["requested_execution_mode"],
                row["selected_execution_modes"],
                row["runner_kind"],
                value_at(row, "evidence_level"),
                value_at(row, "requested_evidence_level"),
                value_at(row, "evidence_level_claim_gate_status"),
                value_at(row, "evidence_level_result_sink_replay_required"),
                value_at(row, "evidence_level_result_sink_replay_verified"),
                value_at(row, "evidence_level_output_digest"),
                value_at(row, "session_runtime_status"),
                value_at(row, "session_close_status"),
                value_at(row, "session_prepared_artifact_reuse_count"),
                value_at(row, "session_source_metadata_cache_hit_count"),
                value_at(row, "session_source_state_reuse_count"),
                value_at(row, "session_hidden_global_cache"),
                value_at(row, "session_daemon_or_service"),
                row["scenario_count"],
                row["total_scenario_compute_millis"],
                row["total_vortex_scan_millis"],
                row["total_result_sink_write_millis"],
                value_at(row, "source_metadata_snapshot_status"),
                value_at(row, "source_state_reuse_status"),
                value_at(row, "source_state_coverage_all_requested_scenarios_classified"),
                value_at(row, "source_state_coverage_reused_scenario_count"),
                value_at(row, "source_state_coverage_not_needed_scenario_count"),
                value_at(row, "source_state_coverage_blocked_scenario_count"),
                value_at(row, "source_state_digest_status"),
                value_at(row, "source_state_family_count"),
                value_at(row, "source_state_prepare_millis"),
                value_at(row, "source_state_selective_filter_reuse_status"),
                value_at(row, "source_state_date_null_metric_reuse_status"),
                value_at(row, "allocation_profile_status"),
                value_at(row, "allocation_count"),
                value_at(row, "allocation_bytes"),
                value_at(row, "buffer_pool_enabled"),
                value_at(row, "buffer_reuse_count"),
                value_at(row, "buffer_reuse_blocker"),
                value_at(row, "peak_rss_delta"),
                value_at(row, "unsafe_lifetime_shortcut_used"),
                value_at(row, "allocation_claim_gate_status"),
                row["claim_gate_status"],
                row["fallback_attempted"],
                row["external_engine_invoked"],
                row["performance_claim_allowed"],
            ]
            for row in summary["batch_rows"]
        ],
    )
    provider_table = html_table(
        [
            "Scenario",
            "Provider status",
            "Filter columns",
            "Encoding summary",
            "Selection consumed",
            "Selected rows",
            "Decoded",
            "Materialized",
            "Encoded-native claim",
        ],
        [
            [
                row["scenario"],
                row["status"],
                row["filter_columns"],
                row["encoding_summary"],
                row["selection_vector_consumed"],
                row["selected_rows"],
                row["data_decoded"],
                row["data_materialized"],
                row["claim_allowed"],
            ]
            for row in summary["encoded_predicate_provider_rows"]
        ],
    )
    source_table = html_table(
        [
            "Scenario",
            "Provider",
            "Projected columns",
            "Pushdown status",
            "Filter",
            "Projection",
            "Limit",
            "Filter columns",
            "Output columns",
            "Filter-only columns",
            "Blocker",
            "Rows scanned",
            "Materialized",
            "Native I/O",
            "Claim gate",
            "Fallback",
            "External engine",
        ],
        [
            [
                row["scenario"],
                row["provider"],
                row["projected_columns"],
                row["pushdown_status"],
                row["scan_filter"],
                row["scan_projection"],
                row["scan_limit"],
                row["filter_columns"],
                row["output_columns"],
                row["filter_only_columns"],
                row["pushdown_blocker"],
                row["rows_scanned"],
                row["data_materialized"],
                row["native_io"],
                row["claim_gate"],
                row["fallback_attempted"],
                row["external_engine_invoked"],
            ]
            for row in summary["source_backed_scan_rows"]
        ],
    )
    fused_table = html_table(
        [
            "Scenario",
            "Used",
            "Family",
            "Rows selected",
            "Rows output",
            "Materialization avoided",
            "Decoded",
            "Materialized",
            "Claim gate",
            "Encoded-native claim",
        ],
        [
            [
                row["scenario"],
                row["used"],
                row["family"],
                row["rows_selected"],
                row["rows_output"],
                row["materialization_avoided"],
                row["data_decoded"],
                row["data_materialized"],
                row["claim_gate"],
                row["encoded_native_claim"],
            ]
            for row in summary["fused_pipeline_rows"]
        ],
    )
    materialization_table = html_table(
        [
            "Scenario",
            "Decoded",
            "Materialized",
            "Row read",
            "Arrow converted",
            "Boundary report",
            "Native I/O",
            "Fallback",
            "External engine",
        ],
        [
            [
                row["scenario"],
                row["data_decoded"],
                row["data_materialized"],
                row["row_read"],
                row["arrow_converted"],
                row["boundary"],
                row["native_io"],
                row["fallback_attempted"],
                row["external_engine_invoked"],
            ]
            for row in summary["materialization_rows"]
        ],
    )
    latest_artifact_at = latest_artifact_generated_at(summary)
    encoded_generated_at = (
        latest_generated_at(summary["encoded_predicate_provider_rows"])
        or latest_artifact_at
    )
    source_generated_at = (
        latest_generated_at(summary["source_backed_scan_rows"]) or latest_artifact_at
    )
    fused_generated_at = (
        latest_generated_at(summary["fused_pipeline_rows"]) or latest_artifact_at
    )
    encoded_source = compact_source_list(summary["encoded_predicate_provider_rows"])
    source_scan_source = compact_source_list(summary["source_backed_scan_rows"])
    fused_source = compact_source_list(summary["fused_pipeline_rows"])
    batch_source = (
        "target/shardloom-benchmark-evidence/prepared_vortex_batch.json and "
        "native_vortex_batch.json"
    )
    encoded_summaries = sorted(
        {
            str(row.get("encoding_summary"))
            for row in summary["encoded_predicate_provider_rows"]
            if row.get("encoding_summary")
        }
    )
    encoded_summary_text = ", ".join(encoded_summaries) if encoded_summaries else "not recorded"
    freshness_panel = (
        '<div class="freshness-grid">'
        + (
            freshness_note(
                "Comparative dashboard",
                summary.get("comparative_dashboard", {}).get("source", "not imported"),
                summary.get("comparative_dashboard", {}).get("generated", ""),
            )
            if summary.get("comparative_dashboard")
            else freshness_note("Comparative dashboard", "not imported")
        )
        + freshness_note(
            "Prepared/native batch smoke",
            batch_source,
            "batch runner PR #634; source files do not carry generated_at_utc",
        )
        + freshness_note("Encoded predicate evidence", encoded_source, encoded_generated_at)
        + freshness_note("Source-backed scan evidence", source_scan_source, source_generated_at)
        + freshness_note("Fused pipeline evidence", fused_source, fused_generated_at)
        + "</div>"
    )
    user_layer_table = html_table(
        [
            "Scenario family",
            "ShardLoom user path",
            "Lightweight engine path",
            "What ShardLoom simplifies",
            "Current status",
        ],
        [
            [
                "compatibility import to Vortex evidence",
                "Run the compatibility lane and receive import, Vortex write/reopen, scan, sink, evidence, and claim-gate fields together.",
                "Use direct local execution, then assemble any preparation, write, certification, and evidence records separately.",
                "One reported workflow for staging compatibility input into a Vortex evidence path.",
                "certification lane; not pure query speed",
            ],
            [
                "prepared/native Vortex query",
                "Point at prepared Vortex artifacts and read execution mode, Native I/O, source-backed scan, and no-fallback evidence.",
                "Run direct local queries against the engine's supported input path and track external evidence separately.",
                "Keeps the Vortex artifact, execution mode, and claim boundary visible in the same row.",
                "runtime-development lane",
            ],
            [
                "dirty CSV cleanup/write",
                "Use the dirty CSV scenario to exercise clean-cast handling, sink output, and evidence fields.",
                "Load, clean, cast, write, and document evidence as separate user steps.",
                "Combines cleanup workflow evidence with result-sink and no-fallback reporting.",
                "fixture-smoke evidence",
            ],
            [
                "nested JSON field scan",
                "Run the nested JSON path with explicit field-scan, materialization, and claim-gate reporting.",
                "Use the engine's JSON support and maintain field/proof context outside the result.",
                "Keeps nested-field access and evidence posture in the same output surface.",
                "fixture-smoke evidence",
            ],
            [
                "CDC overlay",
                "Run overlay smoke with Vortex-oriented evidence and explicit support boundaries.",
                "Build the overlay workflow and evidence reporting around the direct execution engine.",
                "Surfaces overlay posture without implying production CDC semantics.",
                "fixture-smoke/report-only boundary",
            ],
            [
                "result-sink replay",
                "Read sink/write evidence alongside the run row and claim gate.",
                "Persist results and assemble replay/certification context separately.",
                "Makes result-sink proof part of the reported compute workflow.",
                "scoped local evidence",
            ],
            [
                "unsupported path diagnostics",
                "Receive deterministic unsupported, blocked, or report-only status without fallback execution.",
                "Handle unsupported engine behavior or missing features through engine-specific errors and external policy.",
                "Turns unsupported states into explicit no-fallback diagnostics.",
                "core policy behavior",
            ],
            [
                "object-store/lakehouse/Foundry boundary",
                "Expose public status and claim boundaries without production runtime claims.",
                "Use separate production platform integrations where supported by that engine or system.",
                "Prevents website readers from mistaking roadmap or compatibility posture for supported platform runtime.",
                "not production-claimed",
            ],
        ],
    )
    scorecard_html = """
        <div class="scorecard-grid">
          <article><strong>Pure local speed</strong><span>early / not claimed</span></article>
          <article><strong>Coverage breadth</strong><span>improving</span></article>
          <article><strong>User-layer simplicity</strong><span>core design goal</span></article>
          <article><strong>Evidence/certification</strong><span>current differentiator</span></article>
          <article><strong>Optimization maturity</strong><span>beta</span></article>
        </div>
    """
    mode_visual = mode_comparison_visual(
        summary.get("comparative_dashboard"), summary["batch_rows"]
    )
    manifest_panel = benchmark_manifest_panel(summary)
    timing_decomposition = representative_timing_decomposition(rows)
    metadata = summary["table_metadata_smoke"]
    artifact_list = "\n".join(
        f"<li><code>{esc(row['file'])}</code> - profile <code>{esc(row['dataset_profile'])}</code>, {esc(row['rows'])} rows</li>"
        for row in summary["artifacts"]
    )
    if comparative:
        missing = "".join(
            f"<li>{esc(item)}</li>" for item in comparative.get("missing_baselines", [])
        )
        if not missing:
            missing = "<li>No missing-baseline diagnostics were recorded in the local comparative dashboard.</li>"
        comparative_sections = f"""
    <section id="comparative-context" class="comparison-section">
      <div class="shell">
        <div class="section-header-row">
          <div>
            <p class="eyebrow">Comparative context</p>
            <h2>Local Comparative Dashboard</h2>
            <p class="section-lede">This imports the richer local dashboard snapshot as context only. It helps explain what ran, which baselines were unavailable, and where ShardLoom's compatibility/prepared/native rows sit, but it is not a public performance, superiority, Spark-displacement, or best-default claim.</p>
          </div>
          <p class="source-note">Source: <code>{esc(comparative.get('source'))}</code><br>{esc(comparative.get('generated'))}</p>
        </div>
        <div class="metric-grid comparison-metrics">
          <div class="metric"><strong>{esc(dashboard_card_value(comparative, 'Rows'))}</strong><span>comparative rows</span></div>
          <div class="metric"><strong>{esc(dashboard_card_value(comparative, 'Coverage Rows'))}</strong><span>coverage rows</span></div>
          <div class="metric"><strong>{esc(dashboard_card_value(comparative, 'Formats'))}</strong><span>source formats</span></div>
          <div class="metric"><strong>{esc(dashboard_card_value(comparative, 'Performance Claim'))}</strong><span>performance claim allowed</span></div>
        </div>
        <div class="notice-panel">
          <strong>Read this as attribution, not ranking.</strong>
          <span>Compatibility rows include ingest, stage, write/reopen, scan, result, and evidence work. External engines are baseline rows only and cannot satisfy ShardLoom-native evidence gates.</span>
        </div>
        <div class="comparison-guidance">
          <article>
            <h3>What to compare</h3>
            <ul>
              <li>certified import/stage route vs prepared steady-state route</li>
              <li>warm prepared route vs already-Vortex batch smoke</li>
              <li>source-backed scan fields vs materialized compatibility path fields</li>
            </ul>
          </article>
          <article>
            <h3>What not to compare as a public ranking</h3>
            <ul>
              <li>ShardLoom vs Polars, DuckDB, DataFusion, or Spark</li>
              <li>compatibility import totals vs pure direct-file query time</li>
              <li>fixture-smoke evidence vs production platform runtime claims</li>
            </ul>
          </article>
        </div>
      </div>
    </section>
    <section id="local-timing-context">
      <div class="shell">
        <h2>Local Timing Context</h2>
        <p class="section-lede">The mode view keeps certified import/stage, prepared steady-state, already-Vortex, and batch smoke routes separated. These numbers are local context for attribution before optimization, not a leaderboard.</p>
        {mode_visual}
        <h3>Representative timing decomposition</h3>
        <p class="section-lede">The decomposition highlights why compatibility import totals should not be read as pure query speed: parse/import, Vortex write, scan, operator, sink, and evidence work are separate fields.</p>
        {timing_decomposition}
        {details_block('Raw local timing context table', table_from_dashboard(comparative.get('engine_timing_overview', {})))}
      </div>
    </section>
    <section>
      <div class="shell">
        <h2>Vortex-Oriented Lanes By Source Format</h2>
        <p class="section-lede">This view makes the prepared and Vortex-oriented lanes easier to compare across source formats while keeping the compatibility/import boundary separate.</p>
        {details_block('Raw Vortex-oriented lanes table', table_from_dashboard(comparative.get('vortex_oriented_lanes', {})))}
      </div>
    </section>
    <section>
      <div class="shell split">
        <div>
          <h2>Claim-Gate Distribution</h2>
          <p class="section-lede">The distribution is useful because most rows remain blocked, fixture-only, external-baseline-only, or otherwise not claim-grade. That is expected for the current pre-release posture.</p>
          {details_block('Raw claim-gate distribution table', table_from_dashboard(comparative.get('claim_gate_distribution', {})))}
        </div>
        <aside class="side-panel">
          <h3>Missing baselines</h3>
          <p>Missing optional libraries are shown so readers do not mistake absent baseline rows for ShardLoom evidence.</p>
          <ul>{missing}</ul>
        </aside>
      </div>
    </section>
        """
    else:
        comparative_sections = """
    <section id="comparative-context" class="comparison-section">
      <div class="shell">
        <p class="eyebrow">Comparative context</p>
        <h2>Local Comparative Dashboard</h2>
        <p class="section-lede">No local comparative dashboard snapshot was available to import when this page was generated. The prepared/native evidence below remains available and claim-safe.</p>
      </div>
    </section>
        """
    body = f"""
    <section class="doc-hero benchmark-hero">
      <div class="shell">
        {page_header_logo()}
        <p class="eyebrow">Local evidence snapshot</p>
        <h1>Benchmark Evidence, Not A Leaderboard</h1>
        <p class="lede">Current prepared/native benchmark smoke evidence for ShardLoom. These rows are raw local measurements and evidence fields, not performance, superiority, Spark-displacement, production SQL/DataFrame, object-store, lakehouse, or Foundry claims.</p>
        <div class="metric-grid">
          <div class="metric"><strong>{len(rows)}</strong><span>ShardLoom timing rows</span></div>
          <div class="metric"><strong>{len(summary['source_backed_scan_rows'])}</strong><span>source-backed scan rows</span></div>
          <div class="metric"><strong>{len(summary['encoded_predicate_provider_rows'])}</strong><span>encoded predicate rows</span></div>
          <div class="metric"><strong>{len(summary['fused_pipeline_rows'])}</strong><span>fused pipeline rows</span></div>
          <div class="metric"><strong>{len(summary['batch_rows'])}</strong><span>batch mode smoke rows</span></div>
        </div>
      </div>
    </section>
    <section id="artifact-profile">
      <div class="shell">
        <p class="eyebrow">Publishing artifact</p>
        <h2>Artifact Completeness And Lane Availability</h2>
        <p class="section-lede">This panel prevents missing competitor libraries from silently removing lanes from the public page. Full-local artifacts must list expected, available, and missing lanes before they can be interpreted as full-local benchmark evidence.</p>
        {manifest_panel}
      </div>
    </section>
    <section id="scale-profiles" class="doc-section">
      <div class="shell">
        <p class="eyebrow">Scale profile boundary</p>
        <h2>Scale Profiles Are Evidence Plans, Not Any-Volume Claims</h2>
        <p class="section-lede">The benchmark contract now names scale-oriented profiles such as <code>local_stress</code>, <code>larger_than_memory_local</code>, <code>many_small_files</code>, <code>partitioned_table_metadata</code>, <code>object_store_report_only</code>, <code>table_metadata_report_only</code>, <code>foundry_dev_stack_scale_proof</code>, and <code>distributed_report_only</code>. Current public rows remain local smoke or report-only posture unless real input bytes, correctness proof, declared resource envelope, no-fallback evidence, and the relevant runtime gates are attached.</p>
        <div class="telemetry-signal-grid" aria-label="Scale benchmark profile boundaries">
          <article class="signal-card telemetry-card">
            <span class="claim-badge blocked">not any-volume</span>
            <h3>Actual scale proof requires workload bytes</h3>
            <p>Synthetic metadata can document a plan or blocker, but it cannot become runtime scale evidence.</p>
          </article>
          <article class="signal-card telemetry-card">
            <span class="claim-badge report-only">separate profiles</span>
            <h3>Local smoke stays separate</h3>
            <p>Scale benchmark profiles are not mixed into public leaderboard rows or timing rankings.</p>
          </article>
          <article class="signal-card telemetry-card">
            <span class="claim-badge supported"><code>fallback_attempted=false</code></span>
            <h3>No fallback boundary remains visible</h3>
            <p>External engines can be baselines or oracles only; they cannot satisfy ShardLoom scale evidence.</p>
          </article>
        </div>
      </div>
    </section>
    <nav class="page-subnav" aria-label="Benchmark evidence sections">
      <div class="shell">
        <a href="#artifact-profile">Artifact profile</a>
        <a href="#scale-profiles">Scale profiles</a>
        <a href="#takeaways">Key takeaways</a>
        <a href="#why-raw-speed">Raw speed axis</a>
        <a href="#user-layer">User layer</a>
        <a href="#optimization">Optimization</a>
        <a href="#claim-boundary">Claim boundary</a>
        <a href="#comparative-context">Comparative context</a>
        <a href="#timing">Execution timing</a>
        <a href="#batch">Batch smoke</a>
        <a href="#encoded">Encoded predicate</a>
        <a href="#source-backed">Source-backed scans</a>
        <a href="#materialization">Materialization</a>
      </div>
    </nav>
    <section id="takeaways" class="takeaway-section">
      <div class="shell">
        <p class="eyebrow">Key takeaways</p>
        <h2>What These Results Actually Show</h2>
        <div class="takeaway-grid">
          <article class="telemetry-card">
            <h3>Not a speed leaderboard</h3>
            <p>This benchmark is an attribution baseline before optimization. It records workflow, evidence, and claim gates alongside local timing.</p>
          </article>
          <article class="telemetry-card">
            <h3>ShardLoom is carrying more workflow surface</h3>
            <p>Compatibility rows include mode attribution, Vortex preparation, sink proof, materialization/decode boundaries, no-fallback fields, and claim gates.</p>
          </article>
          <article class="telemetry-card">
            <h3>Prepared/native is the real runtime direction</h3>
            <p>The prepared/native path is the main optimization direction; compatibility import is the certification lane, not pure query speed.</p>
          </article>
          <article class="telemetry-card">
            <h3>Lightweight engines are excellent but narrower at the workflow layer</h3>
            <p>Lightweight engines are excellent on direct local execution paths. ShardLoom is targeting a broader user workflow and evidence layer.</p>
          </article>
          <article class="telemetry-card">
            <h3>This is pre-optimization baseline evidence</h3>
            <p>Pure speed remains early and not claimed. The page shows what is measured before fused encoded/native operator work matures.</p>
          </article>
        </div>
        <div class="telemetry-signal-grid" aria-label="Benchmark claim and policy signals">
          <article class="signal-card telemetry-card">
            <span class="claim-badge supported">local smoke evidence</span>
            <h3>Local workflow coverage</h3>
            <p>Rows cover compatibility import, prepared Vortex, source-backed scan, materialization, result-sink, and batch smoke evidence.</p>
          </article>
          <article class="signal-card telemetry-card">
            <span class="claim-badge blocked">performance claim not allowed</span>
            <h3>Claim gate stays closed</h3>
            <p>Timing is shown for attribution and engineering direction only. It is not a public speed, superiority, or best-default claim.</p>
          </article>
          <article class="signal-card telemetry-card">
            <span class="claim-badge supported"><code>fallback_attempted=false</code></span>
            <h3>No fallback evidence</h3>
            <p>ShardLoom rows preserve explicit no-fallback fields instead of silently delegating unsupported work.</p>
          </article>
          <article class="signal-card telemetry-card">
            <span class="claim-badge blocked">external-baseline-only</span>
            <h3>External engines are context</h3>
            <p>Baseline rows explain the local environment. They do not satisfy ShardLoom-native evidence gates.</p>
          </article>
          <article class="signal-card telemetry-card">
            <span class="claim-badge supported">prepared/native batch smoke</span>
            <h3>Batch runner structural signal</h3>
            <p>The single-process prepared/native runner is visible as smoke evidence, not a hidden fast mode.</p>
          </article>
        </div>
        {freshness_panel}
      </div>
    </section>
    <section id="why-raw-speed">
      <div class="shell split">
        <div>
          <p class="eyebrow">Interpretation</p>
          <h2>Why Raw Speed Is Not The Only Axis</h2>
          <p class="section-lede">Local direct-file speed matters, and it is useful context. ShardLoom also measures the workflow surface that makes a run auditable: execution mode attribution, Vortex preparation, result-sink proof, materialization/decode boundaries, source/sink evidence, no-fallback fields, and claim gates.</p>
          <p class="section-lede">External engines are baseline context only. They do not satisfy ShardLoom-native evidence, no-fallback, or Vortex-native claim gates.</p>
        </div>
        <aside class="side-panel evidence-chain">
          <h3>Evidence axis</h3>
          <ol>
            <li>mode attribution</li>
            <li>Vortex preparation</li>
            <li>source/sink proof</li>
            <li>materialization/decode boundary</li>
            <li>fallback and external-engine fields</li>
            <li>claim gate</li>
          </ol>
        </aside>
      </div>
    </section>
    <section id="user-layer" class="comparison-section">
      <div class="shell">
        <p class="eyebrow">Developer and agent surface</p>
        <h2>User-Layer Simplicity</h2>
        <p class="section-lede">ShardLoom is targeting a broader user workflow and evidence layer. The current evidence is not that ShardLoom is quicker; it is that more of the preparation, execution, sink, diagnostic, and claim-boundary workflow is surfaced in one place.</p>
        {user_layer_table}
      </div>
    </section>
    <section id="optimization">
      <div class="shell split">
        <div>
          <p class="eyebrow">Maturity posture</p>
          <h2>Optimization Maturity</h2>
          <p class="section-lede">Current state is beta/pre-optimization. Compatibility import is still expensive, prepared/native is improving, and the scoped in-process session-backed batch runner is a structural unlock for the next runtime work.</p>
          <ul class="check-list">
            <li>encoded/native/fused operator work remains future optimization.</li>
            <li>prepared/native Vortex is the main optimization direction.</li>
            <li>no performance or superiority claim is made from these rows.</li>
          </ul>
        </div>
        <aside>
          {scorecard_html}
        </aside>
      </div>
    </section>
    <section id="claim-boundary">
      <div class="shell">
        <h2>Claim Boundary</h2>
        <div class="boundary-grid">
          <article><strong>Allowed interpretation</strong><span>{esc(summary['claim_boundary']['scope'])}</span></article>
          <article><strong>Performance claim</strong><span>not allowed</span></article>
          <article><strong>Spark-displacement claim</strong><span>not allowed</span></article>
          <article><strong>Production platform claim</strong><span>not allowed</span></article>
        </div>
      </div>
    </section>
    <section>
      <div class="shell">
        <h2>How To Read The Evidence</h2>
        <p class="section-lede">ShardLoom evidence is grouped by lane and claim posture. The goal is attribution: what ran, what was prepared, what materialized, and which claims remain blocked.</p>
        <div class="benchmark-map">
          <article><h3>Compatibility import</h3><p>Certification lane. Includes parse/import/write/reopen/scan and therefore should not be read as pure query speed.</p></article>
          <article><h3>Prepared Vortex</h3><p>Prepared-artifact lane. Current runtime-development focus with fixture-smoke evidence and no public performance claim.</p></article>
          <article><h3>Native Vortex</h3><p>Existing Vortex input lane. Scoped local rows must carry source-backed scan and Native I/O evidence.</p></article>
          <article><h3>External baselines</h3><p>Comparison rows only. They never satisfy ShardLoom-native, Vortex-native, no-fallback, or claim-grade gates.</p></article>
        </div>
      </div>
    </section>
    {comparative_sections}
    <section id="timing">
      <div class="shell">
        <h2>Execution Mode Timing</h2>
        <p class="section-lede">Compatibility import rows, prepared Vortex rows, and native Vortex batch rows are separated. Compatibility rows include ingest/stage/certification work; prepared/native rows start from prepared Vortex artifacts.</p>
        {details_block('Raw execution mode timing table', mode_table)}
      </div>
    </section>
    <section id="batch">
      <div class="shell">
        <h2>Prepared And Native Batch Smoke</h2>
        <p class="section-lede">Direct CLI smoke rows from `traditional-analytics-vortex-batch-run` keep the scoped in-process session explicit. They now expose evidence level beside execution mode: <code>minimal_runtime</code> is runtime-development evidence and stays <code>not_claim_grade</code>, <code>certified</code> carries normal certificates without replay by default, and <code>full_replay</code> requires result-sink replay proof. They also show prepared-artifact registry reuse, source metadata, source-state reuse, allocation/resource-profile posture, and the GAR-PERF-1B coverage classification separately from scenario compute and scan timing. The allocation fields are visibility and blocker evidence: the buffer pool is disabled, allocation counts and peak RSS are not measured yet, and the rows are not a persistent daemon, hidden fast mode, memory-efficiency claim, or performance claim.</p>
        {batch_table}
      </div>
    </section>
    <section id="encoded">
      <div class="shell">
        <h2>Encoded Predicate Provider Evidence</h2>
        <p class="section-lede">Applicable to the selective-filter prepared/native row. The row records admitted filter-column batches and selected-metric selection-vector consumption, but still blocks encoded-native and performance claims.</p>
        <div class="notice-panel">
          <strong>Encoded-predicate verification note.</strong>
          <span>The current committed artifact reports <code>{esc(encoded_summary_text)}</code>. This is scoped provider evidence only, not an encoded-native claim. If a future artifact or source reports a different value-column encoding, update the generator and source evidence together.</span>
        </div>
        {details_block('Raw encoded predicate provider table', provider_table)}
      </div>
    </section>
    <section id="source-backed">
      <div class="shell">
        <h2>Source-Backed Scan Evidence</h2>
        <p class="section-lede">Prepared rows expose Vortex source-backed scan fields and no-fallback evidence instead of relabeling residual-native operators as encoded-native.</p>
        {details_block('Raw source-backed scan table', source_table)}
      </div>
    </section>
    <section id="fused-pipeline">
      <div class="shell">
        <h2>Fused Pipeline Evidence</h2>
        <p class="section-lede">GAR-PERF-1C rows show scoped residual-native work avoidance for prepared/native filter/projection/limit and selection-vector metric aggregation paths. They are not encoded-native or performance claims.</p>
        {details_block('Raw fused pipeline table', fused_table)}
      </div>
    </section>
    <section id="materialization">
      <div class="shell">
        <h2>Materialization, Decode, And No-Fallback</h2>
        <p class="section-lede">These fields make decode/materialization boundaries explicit for prepared rows.</p>
        {details_block('Raw materialization and decode table', materialization_table)}
      </div>
    </section>
    <section>
      <div class="shell">
        <h2>Table Metadata Smoke</h2>
        <p class="section-lede">The local table metadata smoke is included only as scoped metadata evidence, not as a lakehouse/catalog runtime benchmark.</p>
        {html_table(['Field', 'Value'], [[key, value] for key, value in metadata.items()])}
      </div>
    </section>
    <section>
      <div class="shell">
        <h2>Evidence Artifacts</h2>
        <p class="section-lede">The raw smoke artifacts were generated under <code>{esc(summary['source_artifact_dir'])}</code>. The website commits the summarized, claim-safe snapshot in <code>website/assets/data/benchmark-evidence.json</code>.</p>
        <ul class="artifact-list">{artifact_list}</ul>
      </div>
    </section>
    """
    body = "\n".join(line.rstrip() for line in body.splitlines())
    return page(
        "ShardLoom Benchmark Evidence",
        "Claim-safe prepared/native local benchmark evidence for ShardLoom.",
        body,
        "telemetry",
        pagefind_filters={"section": "Telemetry", "status": "benchmark-evidence"},
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--benchmark-dir",
        type=Path,
        default=ROOT / "target" / "shardloom-benchmark-evidence",
    )
    parser.add_argument(
        "--comparative-dashboard",
        type=Path,
        default=None,
        help=(
            "Optional legacy local comparative dashboard HTML to summarize. "
            "Prefer scripts/promote_benchmark_artifact.py for current publishing."
        ),
    )
    parser.add_argument(
        "--benchmark-profile",
        default="smoke",
        choices=tuple(PROFILES),
        help="Benchmark profile to record when regenerating local smoke website artifacts.",
    )
    parser.add_argument(
        "--benchmark-manifest",
        type=Path,
        default=None,
        help="Optional committed benchmark manifest to render instead of regenerating summary data.",
    )
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    write_field_guide_pages()
    use_cases = write_use_case_pages()
    write_sitemap(use_cases)
    (WEBSITE / "readme.html").write_text(
        doc_page(
            ROOT / "README.md",
            "Repository README",
            "Rendered current README from the ShardLoom repository.",
            "README.md",
            "docs",
        ),
        encoding="utf-8",
    )
    compute_flow_source = ROOT / "docs" / "architecture" / "compute-engine-flow-reference.md"
    (DATA_DIR / "compute-engine-flow-reference.md").write_text(
        compute_flow_source.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (WEBSITE / "compute-engine-flow.html").write_text(
        compute_flow_page(compute_flow_source),
        encoding="utf-8",
    )
    (WEBSITE / "status.html").write_text(status_page(use_cases), encoding="utf-8")
    if args.benchmark_manifest is not None:
        summary = load_benchmark_summary_from_manifest(args.benchmark_manifest)
    else:
        summary = benchmark_summary(args.benchmark_dir)
        summary["benchmark_profile"] = args.benchmark_profile
        comparative_dashboard = args.comparative_dashboard
        if comparative_dashboard and comparative_dashboard.exists():
            summary["comparative_dashboard"] = comparative_dashboard_summary(comparative_dashboard)
        summary["benchmark_manifest"] = write_latest_benchmark_artifacts(summary)
    (DATA_DIR / "benchmark-evidence.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (WEBSITE / "benchmarks.html").write_text(
        benchmark_page(summary),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
