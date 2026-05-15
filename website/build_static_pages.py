#!/usr/bin/env python
"""Generate committed static website pages from repo docs and local evidence.

This is a local maintainer helper. Cloudflare still serves committed static
files from website/ and does not run this script during deployment.
"""

from __future__ import annotations

import argparse
import html
import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
WEBSITE = ROOT / "website"
DATA_DIR = WEBSITE / "assets" / "data"


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def slug(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "-", value.lower()).strip("-")
    return text or "section"


def normalize_link(href: str) -> str:
    if re.match(r"^(https?:|mailto:|#|/)", href):
        return href
    path = href.removeprefix("./")
    target = "tree" if path.endswith("/") else "blob"
    return f"https://github.com/depsilon/shardloom/{target}/main/{path}"


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


def page(title: str, description: str, body: str, active: str) -> str:
    nav = [
        ("Home", "/", "home"),
        ("Benchmarks", "/benchmarks.html", "benchmarks"),
        ("Compute Flow", "/compute-engine-flow.html", "flow"),
        ("README", "/readme.html", "readme"),
        ("GitHub", "https://github.com/depsilon/shardloom", "github"),
    ]
    nav_html = "\n".join(
        f'<a class="{"active" if key == active else ""}" href="{href}">{label}</a>'
        for label, href, key in nav
    )
    canonical_paths = {
        "home": "",
        "benchmarks": "benchmarks.html",
        "flow": "compute-engine-flow.html",
        "readme": "readme.html",
    }
    canonical_path = canonical_paths.get(active, "")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{esc(title)}</title>
  <meta name="description" content="{esc(description)}">
  <link rel="canonical" href="https://shardloom.io/{canonical_path}">
  <link rel="icon" type="image/png" href="/assets/logo/shardloom-favicon.png">
  <link rel="apple-touch-icon" href="/assets/logo/shardloom-favicon.png">
  <link rel="stylesheet" href="/assets/site.css">
</head>
<body>
  <header class="site-header">
    <div class="shell nav">
      <a class="brand" href="/" aria-label="ShardLoom home">
        <img src="/assets/logo/shardloom-favicon.png" alt="" width="36" height="36" aria-hidden="true">
        <span>ShardLoom</span>
      </a>
      <nav class="nav-links" aria-label="Primary">
        {nav_html}
      </nav>
    </div>
  </header>
  <main>{body}</main>
  <footer>
    <div class="shell">Apache-2.0 project code. ShardLoom name, logo, and icon are brand assets; see <a href="/BRAND.md">BRAND.md</a>.</div>
  </footer>
</body>
</html>
"""


def doc_page(source: Path, title: str, description: str, source_label: str, active: str) -> str:
    markdown = source.read_text(encoding="utf-8")
    body = f"""
    <section class="doc-hero">
      <div class="shell">
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
    return page(title, description, body, active)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def output_fields(payload: dict[str, Any]) -> dict[str, str]:
    return {row["key"]: row["value"] for row in payload.get("fields", [])}


def value_at(mapping: dict[str, Any], key: str) -> Any:
    value = mapping.get(key)
    return "n/a" if value is None else value


def rounded(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 4)
    return value


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
                        "scenario": result.get("scenario_name"),
                        "provider": evidence.get("source_backed_scan_provider_kind"),
                        "projected_columns": evidence.get(
                            "source_backed_scan_projected_columns"
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
        batch_rows.append(
            {
                "file": repo_relative_path(benchmark_dir / name),
                "requested_execution_mode": fields.get("requested_execution_mode"),
                "selected_execution_modes": fields.get("selected_execution_modes"),
                "runner_kind": fields.get("runner_kind"),
                "scenario_count": fields.get("scenario_count"),
                "total_scenario_compute_millis": round(
                    float(fields.get("total_scenario_compute_micros", "0")) / 1000.0,
                    4,
                ),
                "total_vortex_scan_millis": round(
                    float(fields.get("total_vortex_scan_micros", "0")) / 1000.0,
                    4,
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
        "engine_timing_overview": extract_dashboard_table(source, "Engine Timing Overview"),
        "vortex_oriented_lanes": extract_dashboard_table(
            source,
            "Vortex-Oriented Lanes By Source Format",
        ),
        "claim_gate_distribution": extract_dashboard_table(source, "Claim-Gate Distribution"),
        "missing_baselines": missing_baselines,
        "claim_boundary": "comparative context only; not public performance, superiority, Spark replacement, or best-default evidence",
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
            "Scenarios",
            "Scenario compute ms",
            "Vortex scan ms",
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
                row["scenario_count"],
                row["total_scenario_compute_millis"],
                row["total_vortex_scan_millis"],
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
            <p class="section-lede">This imports the richer local dashboard snapshot as context only. It helps explain what ran, which baselines were unavailable, and where ShardLoom's compatibility/prepared/native rows sit, but it is not a public performance, superiority, Spark replacement, or best-default claim.</p>
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
      </div>
    </section>
    <section>
      <div class="shell">
        <h2>Engine Timing Overview</h2>
        <p class="section-lede">The local dashboard table is preserved with claim-safe language. Columns named fastest or relative are local snapshot descriptors, not superiority claims.</p>
        {table_from_dashboard(comparative.get('engine_timing_overview', {}))}
      </div>
    </section>
    <section>
      <div class="shell">
        <h2>Vortex-Oriented Lanes By Source Format</h2>
        <p class="section-lede">This view makes the prepared and Vortex-oriented lanes easier to compare across source formats while keeping the compatibility/import boundary separate.</p>
        {table_from_dashboard(comparative.get('vortex_oriented_lanes', {}))}
      </div>
    </section>
    <section>
      <div class="shell split">
        <div>
          <h2>Claim-Gate Distribution</h2>
          <p class="section-lede">The distribution is useful because most rows remain blocked, fixture-only, external-baseline-only, or otherwise not claim-grade. That is expected for the current pre-release posture.</p>
          {table_from_dashboard(comparative.get('claim_gate_distribution', {}))}
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
        <p class="eyebrow">Local evidence snapshot</p>
        <h1>Benchmark Evidence</h1>
        <p class="lede">Current prepared/native benchmark smoke evidence for ShardLoom. These rows are raw local measurements and evidence fields, not performance, superiority, Spark replacement, production SQL/DataFrame, object-store, lakehouse, or Foundry claims.</p>
        <div class="metric-grid">
          <div class="metric"><strong>{len(rows)}</strong><span>ShardLoom timing rows</span></div>
          <div class="metric"><strong>{len(summary['source_backed_scan_rows'])}</strong><span>source-backed scan rows</span></div>
          <div class="metric"><strong>{len(summary['encoded_predicate_provider_rows'])}</strong><span>encoded predicate rows</span></div>
          <div class="metric"><strong>{len(summary['batch_rows'])}</strong><span>batch mode smoke rows</span></div>
        </div>
      </div>
    </section>
    <nav class="page-subnav" aria-label="Benchmark evidence sections">
      <div class="shell">
        <a href="#claim-boundary">Claim boundary</a>
        <a href="#comparative-context">Comparative context</a>
        <a href="#timing">Execution timing</a>
        <a href="#batch">Batch smoke</a>
        <a href="#encoded">Encoded predicate</a>
        <a href="#source-backed">Source-backed scans</a>
        <a href="#materialization">Materialization</a>
      </div>
    </nav>
    <section id="claim-boundary">
      <div class="shell">
        <h2>Claim Boundary</h2>
        <div class="boundary-grid">
          <article><strong>Allowed interpretation</strong><span>{esc(summary['claim_boundary']['scope'])}</span></article>
          <article><strong>Performance claim</strong><span>not allowed</span></article>
          <article><strong>Spark replacement claim</strong><span>not allowed</span></article>
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
        {mode_table}
      </div>
    </section>
    <section id="batch">
      <div class="shell">
        <h2>Prepared And Native Batch Smoke</h2>
        <p class="section-lede">Direct CLI smoke rows from `traditional-analytics-vortex-batch-run` keep the single-process batch runner explicit. They are not a persistent daemon or hidden fast mode.</p>
        {batch_table}
      </div>
    </section>
    <section id="encoded">
      <div class="shell">
        <h2>Encoded Predicate Provider Evidence</h2>
        <p class="section-lede">Applicable to the selective-filter prepared/native row. The row records admitted filter-column batches and selected-metric selection-vector consumption, but still blocks encoded-native and performance claims.</p>
        {provider_table}
      </div>
    </section>
    <section id="source-backed">
      <div class="shell">
        <h2>Source-Backed Scan Evidence</h2>
        <p class="section-lede">Prepared rows expose Vortex source-backed scan fields and no-fallback evidence instead of relabeling residual-native operators as encoded-native.</p>
        {source_table}
      </div>
    </section>
    <section id="materialization">
      <div class="shell">
        <h2>Materialization, Decode, And No-Fallback</h2>
        <p class="section-lede">These fields make decode/materialization boundaries explicit for prepared rows.</p>
        {materialization_table}
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
        "benchmarks",
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
        help="Optional local comparative benchmark dashboard HTML to summarize.",
    )
    args = parser.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (WEBSITE / "readme.html").write_text(
        doc_page(
            ROOT / "README.md",
            "Repository README",
            "Rendered current README from the ShardLoom repository.",
            "README.md",
            "readme",
        ),
        encoding="utf-8",
    )
    (WEBSITE / "compute-engine-flow.html").write_text(
        doc_page(
            ROOT / "docs" / "architecture" / "compute-engine-flow-reference.md",
            "Compute Engine Flow",
            "Rendered canonical compute-engine flow reference, including execution modes, engine modes, access surfaces, and claim gates.",
            "docs/architecture/compute-engine-flow-reference.md",
            "flow",
        ),
        encoding="utf-8",
    )
    summary = benchmark_summary(args.benchmark_dir)
    comparative_dashboard = args.comparative_dashboard
    if comparative_dashboard is None:
        candidate = ROOT.parent / "spark-retire" / "docs" / "shardloom-current-benchmark-dashboard.html"
        if candidate.exists():
            comparative_dashboard = candidate
    if comparative_dashboard and comparative_dashboard.exists():
        summary["comparative_dashboard"] = comparative_dashboard_summary(comparative_dashboard)
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
