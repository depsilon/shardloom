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


def site_nav(active: str) -> str:
    nav = [
        ("Home", "/", "home"),
        ("Field Guide", "/field-guide/", "field-guide"),
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
) -> str:
    nav_html = site_nav(active)
    canonical_paths = {
        "home": "",
        "field-guide": "field-guide/",
        "telemetry": "benchmarks",
        "flow": "compute-engine-flow",
        "status": "status",
        "docs": "readme",
    }
    canonical_path = canonical_path_override
    if canonical_path is None:
        canonical_path = canonical_paths.get(active, "")
    canonical_url = f"https://shardloom.io/{canonical_path}"
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
  <main>{body}</main>
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
    return page(title, description, body, active)


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
          <p class="section-lede">ShardLoom should never make users infer what happened. The public flow separates the user access surface from the runtime contract, the source/preparation lane, the workload engine semantics, the provider path, and the evidence envelope.</p>
        </div>
        <aside class="terminal-panel flow-command-panel">
          <div class="console-row"><code>policy</code><span>no fallback, credentials, governance, capability admission</span></div>
          <div class="console-row"><code>mode</code><span>requested, selected, reason, unsupported diagnostic if needed</span></div>
          <div class="console-row"><code>provider</code><span>Vortex source, prepared artifact, native kernel, or blocked path</span></div>
          <div class="console-row"><code>output</code><span>result ref, evidence refs, timing, claim_gate_status</span></div>
        </aside>
      </div>
    </section>
    <section id="flow-modes">
      <div class="shell">
        <p class="eyebrow">Execution mode lanes</p>
        <h2>Source and preparation choices stay explicit.</h2>
        <p class="section-lede">These lanes are not interchangeable timing rows. Compatibility import carries ingest/stage/certification work. Prepared and native Vortex lanes are the current runtime-development direction. Direct transient and auto stay constrained by diagnostics and selected-mode reporting.</p>
        <div class="mode-lanes mission-mode-lanes">
          <article class="mode-lane"><span class="lane-tag">Certification</span><h3>compatibility_import_certified</h3><p>Reads compatibility input, imports to Vortex, writes/reopens/scans, computes, and certifies the workflow.</p></article>
          <article class="mode-lane"><span class="lane-tag">Runtime direction</span><h3>prepared_vortex</h3><p>Runs scoped scenarios from prepared Vortex artifacts with source-backed scan and no-fallback evidence.</p></article>
          <article class="mode-lane"><span class="lane-tag">Native artifact</span><h3>native_vortex</h3><p>Runs from existing Vortex input where the local row carries Native I/O and claim-boundary fields.</p></article>
          <article class="mode-lane"><span class="lane-tag">Diagnostic</span><h3>direct_compatibility_transient / auto</h3><p>Direct transient remains narrow and not Vortex-native; auto must report selected mode and reason.</p></article>
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
    )


def status_page() -> str:
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
        <a href="#supported">Supported local smoke</a>
        <a href="#fixture">Fixture-smoke</a>
        <a href="#report-only">Report-only</a>
        <a href="#blocked">Blocked</a>
        <a href="#planned">Planned</a>
        <a href="#not-claimed">Not claimed</a>
      </div>
    </nav>
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
    <section id="report-only">
      <div class="shell">
        <p class="eyebrow">Report-only surfaces</p>
        <h2>These surfaces can be documented or diagnosed without claiming runtime support.</h2>
        <div class="status-board">
          <article class="status-column"><span class="claim-badge report-only">report-only</span><h3>REST/event surfaces</h3><p>Future API surfaces must preserve typed envelopes, selected mode, diagnostics, evidence, and claim gates before promotion.</p></article>
          <article class="status-column"><span class="claim-badge report-only">report-only</span><h3>Adapters and end-user wrappers</h3><p>CLI, Python, and planned adapter access must improve ergonomics without hiding execution mode or fallback status.</p></article>
          <article class="status-column"><span class="claim-badge report-only">report-only</span><h3>Object-store, lakehouse, and Foundry boundaries</h3><p>These areas remain boundary/status documentation unless runtime proof and release gates promote a narrow slice.</p></article>
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
    """
    return page(
        "ShardLoom Status Board",
        "Claim-safe public posture board for ShardLoom supported local smoke, fixture-smoke, report-only, blocked, planned, and not-claimed surfaces.",
        body,
        "status",
    )


FIELD_GUIDE_CONCEPTS: list[dict[str, Any]] = [
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


def concept_url(slug_value: str) -> str:
    return f"/field-guide/{slug_value}"


def concept_by_slug(slug_value: str) -> dict[str, Any]:
    for concept in FIELD_GUIDE_CONCEPTS:
        if concept["slug"] == slug_value:
            return concept
    raise KeyError(slug_value)


def bullet_list(items: list[str]) -> str:
    return "<ul>" + "".join(f"<li>{inline_markdown(item)}</li>" for item in items) + "</ul>"


def field_guide_index_page() -> str:
    cards = "".join(
        f"""
          <article class="field-guide-card">
            <span class="claim-badge">Dossier</span>
            <h3>{esc(concept['title'])}</h3>
            <p>{esc(concept['answer'])}</p>
            <div class="action-row"><a class="button" href="{concept_url(concept['slug'])}">Open dossier</a></div>
          </article>
        """
        for concept in FIELD_GUIDE_CONCEPTS
    )
    body = f"""
    <section class="doc-hero field-guide-hero">
      <div class="shell">
        {page_header_logo()}
        <p class="eyebrow">Field Guide</p>
        <h1>Technical dossiers for auditable compute.</h1>
        <p class="lede">Short, source-linked explanations of the terms that matter when reading ShardLoom evidence: execution modes, no-fallback policy, Vortex-native paths, materialization boundaries, benchmark telemetry, and claim gates.</p>
      </div>
    </section>
    <section class="doc-section">
      <div class="shell">
        <div class="field-guide-grid">{cards}</div>
      </div>
    </section>
    """
    return page(
        "ShardLoom Field Guide",
        "Technical dossiers for interpreting ShardLoom evidence.",
        body,
        "field-guide",
        "field-guide/",
    )


def field_guide_concept_page(
    concept: dict[str, Any],
    previous_concept: dict[str, Any] | None,
    next_concept: dict[str, Any] | None,
) -> str:
    related = "".join(
        f'<a class="claim-badge" href="{concept_url(slug_value)}">{esc(concept_by_slug(slug_value)["title"])}</a>'
        for slug_value in concept["related"]
    )
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
    source_links = bullet_list([f"`{source}`" for source in concept["sources"]])
    body = f"""
    <section class="doc-hero field-guide-hero">
      <div class="shell">
        {page_header_logo()}
        <p class="eyebrow">Field Guide dossier</p>
        <h1>{esc(concept['title'])}</h1>
        <p class="lede">{esc(concept['answer'])}</p>
      </div>
    </section>
    <section class="doc-section">
      <div class="shell dossier-layout">
        <aside class="dossier-sidebar">
          <h2>In this dossier</h2>
          <a href="#why">Why it matters</a>
          <a href="#how">How ShardLoom uses it</a>
          <a href="#proof">What it proves</a>
          <a href="#boundary">Claim boundary</a>
          <a href="#sources">Source docs</a>
        </aside>
        <article class="dossier-body">
          <section id="why">
            <p class="eyebrow">Why it matters</p>
            <p>{esc(concept['why'])}</p>
          </section>
          <section id="how">
            <p class="eyebrow">How ShardLoom uses it</p>
            <p>{esc(concept['how'])}</p>
          </section>
          <section id="proof">
            <p class="eyebrow">What it proves</p>
            <p>{esc(concept['proves'])}</p>
            <h3>What it does not prove</h3>
            <p>{esc(concept['not_proves'])}</p>
            <h3>Evidence fields</h3>
            {bullet_list([f"`{field}`" for field in concept['evidence']])}
          </section>
          <section id="boundary">
            <p class="eyebrow">Claim boundary</p>
            <p>{esc(concept['boundary'])}</p>
          </section>
          <section id="sources">
            <p class="eyebrow">Source docs</p>
            {source_links}
          </section>
          <section class="related-concepts">
            <p class="eyebrow">Related concepts</p>
            <div>{related}</div>
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
    )


def write_field_guide_pages() -> None:
    target_dir = WEBSITE / "field-guide"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "index.html").write_text(field_guide_index_page(), encoding="utf-8")
    for index, concept in enumerate(FIELD_GUIDE_CONCEPTS):
        previous_concept = FIELD_GUIDE_CONCEPTS[index - 1] if index > 0 else None
        next_concept = (
            FIELD_GUIDE_CONCEPTS[index + 1]
            if index + 1 < len(FIELD_GUIDE_CONCEPTS)
            else None
        )
        (target_dir / f"{concept['slug']}.html").write_text(
            field_guide_concept_page(concept, previous_concept, next_concept),
            encoding="utf-8",
        )


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
            "compatibility_import_certified geomean",
            dashboard_table_value(timing_table, "shardloom", "Geomean"),
            "certification lane with import, write/reopen, scan, sink, and evidence work",
        ),
        (
            "shardloom-vortex geomean",
            dashboard_table_value(timing_table, "shardloom-vortex", "Geomean"),
            "Vortex-oriented local lane from the comparative dashboard snapshot",
        ),
        (
            "shardloom-prepared-vortex geomean",
            dashboard_table_value(timing_table, "shardloom-prepared-vortex", "Geomean"),
            "prepared-artifact lane and current runtime-development direction",
        ),
        (
            "prepared_vortex batch smoke total/compute",
            f"{batch_row_value(batch_rows, 'prepared_vortex', 'total_scenario_compute_millis')} ms",
            "single-process batch runner structural smoke evidence",
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
    latest_artifact_at = latest_artifact_generated_at(summary)
    encoded_generated_at = (
        latest_generated_at(summary["encoded_predicate_provider_rows"])
        or latest_artifact_at
    )
    source_generated_at = (
        latest_generated_at(summary["source_backed_scan_rows"]) or latest_artifact_at
    )
    encoded_source = compact_source_list(summary["encoded_predicate_provider_rows"])
    source_scan_source = compact_source_list(summary["source_backed_scan_rows"])
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
              <li><code>compatibility_import_certified</code> vs <code>prepared_vortex</code></li>
              <li><code>prepared_vortex</code> vs <code>native_vortex</code> batch smoke</li>
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
        <p class="section-lede">The mode view keeps compatibility certification, Vortex-oriented, prepared Vortex, and batch smoke lanes separated. These numbers are local context for attribution before optimization, not a leaderboard.</p>
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
          <div class="metric"><strong>{len(summary['batch_rows'])}</strong><span>batch mode smoke rows</span></div>
        </div>
      </div>
    </section>
    <nav class="page-subnav" aria-label="Benchmark evidence sections">
      <div class="shell">
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
          <p class="section-lede">Current state is beta/pre-optimization. Compatibility import is still expensive, prepared/native is improving, and the single-process batch runner is a structural unlock for the next runtime work.</p>
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
        <p class="section-lede">Direct CLI smoke rows from `traditional-analytics-vortex-batch-run` keep the single-process batch runner explicit. They are not a persistent daemon or hidden fast mode.</p>
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
    write_field_guide_pages()
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
    (WEBSITE / "compute-engine-flow.html").write_text(
        compute_flow_page(
            ROOT / "docs" / "architecture" / "compute-engine-flow-reference.md",
        ),
        encoding="utf-8",
    )
    (WEBSITE / "status.html").write_text(status_page(), encoding="utf-8")
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
