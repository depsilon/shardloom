#!/usr/bin/env python
"""Build the minimal ShardLoom public website.

The public site is intentionally small: one cohesive home page, one benchmark
evidence page, one compute-flow translation page, and static assets served by
Cloudflare. Repo docs remain in the repository instead of being mirrored into a
large website atlas.
"""

from __future__ import annotations

import argparse
import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
WEBSITE = ROOT / "website"
ASSETS = WEBSITE / "assets"
DATA = ASSETS / "data"
BENCHMARK_LATEST = ASSETS / "benchmarks" / "latest"
FLOW_SOURCE = ROOT / "docs" / "architecture" / "compute-engine-flow-reference.md"
BENCHMARK_MANIFEST = BENCHMARK_LATEST / "manifest.json"
BENCHMARK_RESULTS = BENCHMARK_LATEST / "benchmark-results.json"
SITE_LASTMOD = "2026-05-20"


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def text(value: Any) -> str:
    return "" if value is None else str(value)


def strip_md(value: Any) -> str:
    raw = text(value)
    raw = re.sub(r"<br\s*/?>", " - ", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", raw)
    raw = re.sub(r"`([^`]+)`", r"\1", raw)
    raw = re.sub(r"\*\*([^*]+)\*\*", r"\1", raw)
    raw = re.sub(r"<[^>]+>", "", raw)
    return re.sub(r"\s+", " ", raw).strip()


def compact(value: Any, limit: int = 180) -> str:
    clean = strip_md(value)
    if len(clean) <= limit:
        return clean
    return clean[: limit - 1].rstrip() + "..."


def code(value: Any) -> str:
    return f"<code>{esc(value)}</code>"


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value.rstrip() + "\n", encoding="utf-8")


def nav(active: str) -> str:
    links = [
        ("Home", "/", "home"),
        ("Benchmarks", "/benchmarks", "benchmarks"),
        ("Compute Flow", "/compute-engine-flow", "flow"),
        ("GitHub", "https://github.com/depsilon/shardloom", "github"),
    ]
    rendered = []
    for label, href, key in links:
        class_name = ' class="active" aria-current="page"' if key == active else ""
        rendered.append(f'<a{class_name} href="{href}">{label}</a>')
    return "\n          ".join(rendered)


def page(title: str, description: str, body: str, active: str, canonical_path: str = "") -> str:
    canonical_url = f"https://shardloom.io/{canonical_path}".rstrip("/")
    if canonical_path == "":
        canonical_url = "https://shardloom.io/"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{esc(title)}</title>
  <meta name="description" content="{esc(description)}">
  <meta name="robots" content="index,follow">
  <link rel="canonical" href="{esc(canonical_url)}">
  <link rel="icon" type="image/png" href="/assets/logo/shardloom-favicon.png">
  <link rel="apple-touch-icon" href="/assets/logo/shardloom-favicon.png">
  <link rel="stylesheet" href="/assets/site.css">
  <meta property="og:title" content="{esc(title)}">
  <meta property="og:description" content="{esc(description)}">
  <meta property="og:image" content="https://shardloom.io/assets/logo/shardloom-logo.png">
  <meta property="og:type" content="website">
  <meta property="og:url" content="{esc(canonical_url)}">
  <meta name="twitter:card" content="summary_large_image">
</head>
<body>
  <header class="site-header">
    <a class="brand" href="/" aria-label="ShardLoom home">
      <img src="/assets/logo/shardloom-favicon.png" alt="" width="40" height="40" aria-hidden="true">
      <span>ShardLoom</span>
    </a>
    <nav aria-label="Primary">
      {nav(active)}
    </nav>
  </header>
  <main>
{body}
  </main>
  <footer class="site-footer">
    <img src="/assets/logo/shardloom-logo-trim.png" alt="ShardLoom">
    <p>Pre-release technical preview. Vortex-first. No fallback. Benchmark evidence is not a public speed or production claim.</p>
  </footer>
</body>
</html>"""


def card(title: str, body: str, badge: str | None = None) -> str:
    badge_html = f'<span class="badge">{esc(badge)}</span>' if badge else ""
    return f"""<article class="card">
      {badge_html}
      <h3>{esc(title)}</h3>
      <p>{body}</p>
    </article>"""


def metric(label: str, value: Any, detail: str = "") -> str:
    detail_html = f"<span>{esc(detail)}</span>" if detail else ""
    return f"""<div class="metric">
      <strong>{esc(value)}</strong>
      <span>{esc(label)}</span>
      {detail_html}
    </div>"""


def table(headers: list[str], rows: list[list[Any]], class_name: str = "") -> str:
    head = "".join(f"<th>{esc(header)}</th>" for header in headers)
    body = []
    for row in rows:
        body.append("<tr>" + "".join(f"<td>{esc(cell)}</td>" for cell in row) + "</tr>")
    return (
        f'<div class="table-wrap {class_name}"><table>'
        f"<thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table></div>"
    )


def details(summary: str, inner: str) -> str:
    return f"""<details class="drawer">
      <summary>{esc(summary)}</summary>
      {inner}
    </details>"""


def split_table_row(line: str) -> list[str]:
    return [strip_md(cell) for cell in line.strip().strip("|").split("|")]


def table_after(markdown: str, header_start: str) -> list[list[str]]:
    start = markdown.find(header_start)
    if start < 0:
        return []
    rows: list[list[str]] = []
    for line in markdown[start:].splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            if rows:
                break
            continue
        if re.match(r"^\|\s*-", stripped):
            continue
        rows.append(split_table_row(stripped))
    return rows[1:]


def code_block_after(markdown: str, marker: str) -> str:
    start = markdown.find(marker)
    if start < 0:
        return ""
    fence = markdown.find("```", start)
    if fence < 0:
        return ""
    body_start = markdown.find("\n", fence)
    body_end = markdown.find("```", body_start + 1)
    if body_start < 0 or body_end < 0:
        return ""
    return markdown[body_start + 1 : body_end].strip()


def mermaid_blocks(markdown: str) -> list[tuple[str, str]]:
    blocks: list[tuple[str, str]] = []
    current_heading = "Architecture diagram"
    lines = markdown.splitlines()
    index = 0
    while index < len(lines):
        line = lines[index]
        if line.startswith("## "):
            current_heading = strip_md(line.lstrip("# "))
        if line.strip() == "```mermaid":
            block_lines: list[str] = []
            index += 1
            while index < len(lines) and lines[index].strip() != "```":
                block_lines.append(lines[index])
                index += 1
            blocks.append((current_heading, "\n".join(block_lines).strip()))
        index += 1
    return blocks


def route_steps() -> str:
    steps = [
        ("Front door", "Python, SQL, CLI, benchmarks, or an adapter express the work."),
        ("UniversalIngress", "The source is admitted, classified, or blocked with a reason."),
        ("SourceState", "Schema, fingerprint, adapter status, and source evidence become reusable state."),
        ("vortex_ingest", "Admitted non-Vortex data is prepared into VortexPreparedState."),
        ("Execution", "prepared_vortex, native_vortex, certified cold route, direct one-shot, or generated source."),
        ("OutputPlan", "Result, local sink, Vortex artifact, or future platform sink is planned separately."),
        ("Evidence", "Certificates, no-fallback fields, materialization boundaries, timing, and claim gate."),
    ]
    return "".join(
        f"""<article class="route-step">
          <span>{number:02d}</span>
          <h3>{esc(title)}</h3>
          <p>{esc(detail)}</p>
        </article>"""
        for number, (title, detail) in enumerate(steps, start=1)
    )


def benchmark_summary() -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = load_json(BENCHMARK_MANIFEST)
    results = load_json(BENCHMARK_RESULTS)
    return manifest, results


def lane_rows(manifest: dict[str, Any]) -> list[list[str]]:
    expected = manifest.get("expected_lanes", [])
    available = set(manifest.get("available_lanes", []))
    missing = set(manifest.get("missing_lanes", []))
    versions = manifest.get("lane_versions", {})
    reasons = manifest.get("lane_availability_reasons", {})
    rows = []
    for lane in expected:
        status = "available" if lane in available else "missing" if lane in missing else "not listed"
        version_or_reason = versions.get(lane) or reasons.get(lane) or "not reported"
        rows.append([lane, status, version_or_reason])
    return rows


def comparative_rows(results: dict[str, Any]) -> list[list[Any]]:
    overview = results.get("comparative_dashboard", {}).get("engine_timing_overview", {})
    rows = overview.get("rows", []) if isinstance(overview, dict) else []
    return [[strip_md(cell) for cell in row] for row in rows]


def claim_gate_rows(results: dict[str, Any]) -> list[list[Any]]:
    distribution = results.get("comparative_dashboard", {}).get("claim_gate_distribution", {})
    rows = distribution.get("rows", []) if isinstance(distribution, dict) else []
    return [[strip_md(cell) for cell in row] for row in rows]


def timing_rows(results: dict[str, Any]) -> list[list[Any]]:
    rows = []
    for row in results.get("rows", []):
        rows.append(
            [
                row.get("scenario", ""),
                row.get("selected_execution_mode", ""),
                row.get("storage_format", ""),
                row.get("total_runtime_millis", ""),
                row.get("vortex_scan_millis", ""),
                row.get("operator_compute_millis", ""),
                row.get("claim_gate_status", ""),
            ]
        )
    return rows


def source_state_coverage_rows(results: dict[str, Any]) -> list[list[Any]]:
    rows = []
    for batch in results.get("batch_rows", []):
        rows.append(
            [
                batch.get("scenario", "prepared/native batch"),
                batch.get("source_state_coverage_all_requested_scenarios_classified", ""),
                batch.get("source_state_coverage_reused_scenario_count", ""),
                batch.get("source_state_coverage_not_needed_scenario_count", ""),
                batch.get("source_state_digest_status", ""),
                batch.get("source_state_coverage_matrix_ref", ""),
            ]
        )
    return rows


def home_page(manifest: dict[str, Any], results: dict[str, Any]) -> str:
    available_count = len(manifest.get("available_lanes", []))
    expected_count = len(manifest.get("expected_lanes", []))
    generated = manifest.get("generated_at_utc", "unknown")
    body = f"""
    <section class="hero">
      <div class="hero-copy">
        <p class="eyebrow">Pre-release technical preview</p>
        <h1>Evidence-first compute over Vortex data.</h1>
        <p class="lede">ShardLoom is a Vortex-first, no-fallback local compute engine foundation. The public site is intentionally simple: benchmark evidence, a human-readable compute-flow map, and the repository.</p>
        <div class="actions">
          <a class="button primary" href="/benchmarks">Read benchmark evidence</a>
          <a class="button" href="/compute-engine-flow">Understand compute flow</a>
          <a class="button ghost" href="https://github.com/depsilon/shardloom">Open GitHub</a>
        </div>
      </div>
      <div class="hero-mark" aria-label="ShardLoom logo">
        <img src="/assets/logo/shardloom-logo.png" alt="ShardLoom">
      </div>
    </section>

    <section class="strip">
      {metric("Benchmark lanes", f"{available_count} of {expected_count}", "full_local artifact")}
      {metric("Performance claim", "none", "evidence only")}
      {metric("Fallback policy", "no fallback", "external engines are baselines")}
      {metric("Artifact refreshed", generated[:10], "UTC")}
    </section>

    <section class="section-grid">
      <div>
        <p class="eyebrow">What matters</p>
        <h2>One public story, three surfaces.</h2>
        <p>ShardLoom should not require a visitor to learn the phase plan before understanding the project. The site now keeps the public story tight: what the evidence says, how execution routes work, and where the repo lives.</p>
      </div>
      <div class="card-grid">
        {card("Benchmark evidence", "Local timing context, expected lanes, missing lanes, and claim gates stay visible without becoming a ranking.", "Evidence")}
        {card("Compute-flow translation", "SQL, Python, CLI, and adapters are front doors. Source, preparation, execution, output, and evidence define the route.", "Architecture")}
        {card("Repository first", "Detailed docs, phase plans, RFCs, and implementation history stay in GitHub instead of becoming a sprawling website.", "Source")}
      </div>
    </section>

    <section class="section-grid">
      <div>
        <p class="eyebrow">Claim boundary</p>
        <h2>What the site will not imply.</h2>
        <p>ShardLoom is not presented as a production platform, an Apache Spark substitute, a broad SQL/DataFrame platform, an object-store/lakehouse runtime, a Foundry product, or a public performance winner.</p>
      </div>
      <div class="boundary-list">
        <span>No performance or superiority claim</span>
        <span>No Apache Spark substitute claim</span>
        <span>No production SQL/DataFrame claim</span>
        <span>No production object-store/lakehouse/Foundry claim</span>
        <span>No hidden fallback engine</span>
      </div>
    </section>

    <section class="section-grid">
      <div>
        <p class="eyebrow">Compute route</p>
        <h2>Front door is not the execution route.</h2>
        <p>A user can enter through Python, SQL, CLI, or benchmarks. ShardLoom still records the route through ingress, preparation, execution, output, and evidence.</p>
      </div>
      <div class="route compact-route">{route_steps()}</div>
    </section>
"""
    return page(
        "ShardLoom",
        "Pre-release Vortex-first, no-fallback compute engine foundation with benchmark evidence and a human-readable compute-flow map.",
        body,
        "home",
    )


def benchmarks_page(manifest: dict[str, Any], results: dict[str, Any]) -> str:
    raw_timing = comparative_rows(results)
    raw_timing_table = table(
        ["Engine", "Available", "Success / total", "Geomean", "CSV/Parquet", "local fastest count", "local timing context"],
        raw_timing,
    )
    claim_distribution = table(["Claim gate", "Rows", "Share"], claim_gate_rows(results))
    lane_table = table(["Expected lane", "Status", "Version / reason"], lane_rows(manifest))
    source_state_table = table(
        [
            "Scenario",
            "source_state_coverage_all_requested_scenarios_classified",
            "source_state_coverage_reused_scenario_count",
            "source-state-not-needed",
            "source_state_digest_status",
            "Reference",
        ],
        source_state_coverage_rows(results),
    )
    shardloom_rows = table(
        ["Scenario", "Mode", "Format", "Total ms", "Scan ms", "Compute ms", "Claim gate"],
        timing_rows(results),
    )
    source_rows = table(
        ["Scenario", "Provider", "Rows scanned", "Projected columns", "Materialized", "Native I/O", "Claim"],
        [
            [
                row.get("scenario", ""),
                row.get("provider", ""),
                row.get("rows_scanned", ""),
                row.get("projected_columns", ""),
                row.get("data_materialized", ""),
                row.get("native_io", ""),
                row.get("claim_gate", ""),
            ]
            for row in results.get("source_backed_scan_rows", [])
        ],
    )
    encoded_rows = table(
        ["Scenario", "Status", "Encoding summary", "Selected rows", "Claim allowed"],
        [
            [
                row.get("scenario", ""),
                row.get("status", ""),
                row.get("encoding_summary", ""),
                row.get("selected_rows", ""),
                row.get("claim_allowed", ""),
            ]
            for row in results.get("encoded_predicate_provider_rows", [])
        ],
    )
    body = f"""
    <section class="page-hero">
      <p class="eyebrow">Benchmark evidence</p>
      <h1>Evidence, not a leaderboard.</h1>
      <p class="lede">These artifacts explain what ShardLoom measured locally: workflow coverage, prepared/native direction, no-fallback evidence, and claim boundaries. They do not claim public speed, superiority, production readiness, or Apache Spark substitution.</p>
    </section>

    <section class="strip">
      {metric("Profile", manifest.get("benchmark_profile", "unknown"), manifest.get("artifact_status", ""))}
      {metric("Available lanes", len(manifest.get("available_lanes", [])), f"of {len(manifest.get('expected_lanes', []))} expected")}
      {metric("Missing lanes", len(manifest.get("missing_lanes", [])), "shown, never hidden")}
      {metric("Performance claim", "not allowed", "claim gate closed")}
    </section>

    <section class="section-grid">
      <div>
        <h2>How to read this page</h2>
        <p>Compare ShardLoom routes with each other first: certified cold route, prepared warm route, native Vortex route, and source-backed scan evidence. External engines provide local baseline context only.</p>
      </div>
      <div class="card-grid">
        {card("Certified cold route", "Compatibility import rows include ingress, parse, Vortex ingest, write/reopen, scan, result sink, and evidence work.", "Do compare")}
        {card("Prepared warm route", "Prepared/native rows are the runtime-development direction after Vortex preparation exists.", "Do compare")}
        {card("External engines", "Pandas, Polars, DuckDB, DataFusion, Dask, and Spark rows are baseline context, not ShardLoom evidence gates.", "Do not rank")}
      </div>
    </section>

    <section>
      <h2>Artifact lane availability</h2>
      <p class="narrow">The website renders a committed artifact. It does not discover installed Python libraries during page render.</p>
      {lane_table}
    </section>

    <section>
      <h2>Claim-gate distribution</h2>
      {claim_distribution}
    </section>

    <section>
      <h2>Prepared/native source-state coverage</h2>
      <p class="narrow">The committed artifact keeps source-state reuse evidence visible for the prepared/native batch path. This remains evidence context, not a performance claim.</p>
      {source_state_table}
    </section>

    <section>
      <h2>Local timing context</h2>
      <p class="narrow">This table is timing context for engineering interpretation. It is not a public ranking.</p>
      {raw_timing_table}
    </section>

    <section>
      <h2>ShardLoom timing rows</h2>
      {details("Open scoped ShardLoom timing rows", shardloom_rows)}
      {details("Open source-backed scan evidence", source_rows)}
      {details("Open encoded predicate evidence", encoded_rows)}
    </section>
"""
    return page(
        "ShardLoom Benchmark Evidence",
        "Claim-safe local benchmark evidence for ShardLoom, framed as evidence rather than a leaderboard.",
        body,
        "benchmarks",
        "benchmarks",
    )


def compute_flow_page(markdown: str) -> str:
    mode_rows = table_after(markdown, "| Mode | User-facing label | What it means | Primary use |")
    mode_table = table(
        ["Mode", "Label", "Meaning", "Primary use", "Vortex-native claim?", "Claim posture"],
        mode_rows,
    )
    timing_fields = [
        line.strip()
        for line in code_block_after(markdown, "Mode timing fields must stay visible:").splitlines()
        if line.strip()
    ]
    timing_list = "".join(f"<li>{code(field)}</li>" for field in timing_fields)
    never_block = code_block_after(markdown, "## What Should Never Happen")
    never_items = [strip_md(line) for line in never_block.splitlines() if line.strip()][:8]
    never_list = "".join(f"<li>{esc(item)}</li>" for item in never_items)
    diagram_drawers = "".join(
        details(f"Raw Mermaid source: {heading}", f"<pre><code>{esc(block)}</code></pre>")
        for heading, block in mermaid_blocks(markdown)[:8]
    )
    body = f"""
    <section class="page-hero">
      <p class="eyebrow">Compute-flow translation</p>
      <h1>SQL and Python are front doors. The route is the contract.</h1>
      <p class="lede">ShardLoom separates user surface from execution route: source admission, Vortex preparation, execution mode, output route, and evidence policy are all explicit.</p>
    </section>

    <section class="route">{route_steps()}</section>

    <section class="section-grid">
      <div>
        <h2>Prepared Vortex means prepared state.</h2>
        <p><code>prepared_vortex</code> executes from <code>VortexPreparedState</code>. Non-Vortex data reaches it only after <code>UniversalIngress</code> and <code>vortex_ingest</code>. Compatibility import is the certified cold route, not pure query speed.</p>
      </div>
      <div class="card-grid">
        {card("Source route", "What kind of input is this, and is it admitted or blocked?", "Ingress")}
        {card("Preparation route", "Does this create or reuse SourceState and VortexPreparedState?", "vortex_ingest")}
        {card("Execution route", "Which explicit mode ran, and what timing scope applies?", "Mode")}
        {card("Evidence route", "Which certificates, no-fallback fields, and claim gate came out?", "Claim")}
      </div>
    </section>

    <section>
      <h2>Execution modes</h2>
      {mode_table}
    </section>

    <section class="section-grid">
      <div>
        <h2>Timing fields stay visible.</h2>
        <p>Compatibility rows must not be read as pure query speed. They include source, parse, ingest, Vortex write/reopen, scan, operator, sink, and evidence timing.</p>
      </div>
      <ul class="check-list">{timing_list}</ul>
    </section>

    <section class="section-grid">
      <div>
        <h2>What must never happen</h2>
        <p>The compute-flow contract exists so unsupported work is blocked or diagnosed instead of becoming hidden fallback execution.</p>
      </div>
      <ul class="boundary-list as-list">{never_list}</ul>
    </section>

    <section>
      <h2>Raw diagram source</h2>
      <p class="narrow">Mermaid remains available as source text, but the public page leads with human-readable route structure.</p>
      {diagram_drawers}
      <p class="source-link"><a href="https://github.com/depsilon/shardloom/blob/main/docs/architecture/compute-engine-flow-reference.md">Open canonical Markdown on GitHub</a></p>
    </section>
"""
    return page(
        "ShardLoom Compute Flow",
        "Human-readable ShardLoom compute-flow route map and execution-mode translation.",
        body,
        "flow",
        "compute-engine-flow",
    )


def not_found_page() -> str:
    body = """
    <section class="page-hero">
      <p class="eyebrow">404</p>
      <h1>This page is not part of the public surface.</h1>
      <p class="lede">The website has been simplified around benchmark evidence, compute-flow translation, and the repository.</p>
      <div class="actions">
        <a class="button primary" href="/">Return home</a>
        <a class="button" href="https://github.com/depsilon/shardloom">Open GitHub</a>
      </div>
    </section>
"""
    return page("ShardLoom 404", "ShardLoom page not found.", body, "home", "404")


def sitemap() -> str:
    paths = ["", "benchmarks", "compute-engine-flow"]
    urls = ['<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for path in paths:
        loc = f"https://shardloom.io/{path}".rstrip("/") + ("/" if path == "" else "")
        priority = "1.0" if path == "" else "0.9"
        urls.append(
            f"  <url><loc>{esc(loc)}</loc><lastmod>{SITE_LASTMOD}</lastmod><priority>{priority}</priority></url>"
        )
    urls.append("</urlset>")
    return "\n".join(urls)


def write_support_files() -> None:
    write(
        WEBSITE / "_redirects",
        """
/home /
/index /
/index.html /
/telemetry /benchmarks
/benchmark /benchmarks
/benchmarks.html /benchmarks
/flow /compute-engine-flow
/compute-flow /compute-engine-flow
/compute-engine-flow.html /compute-engine-flow
/field-guide /
/field-guide/* /
/use-cases /
/use-cases/* /
/can-i-use-this /
/status /
/status.html /
/docs https://github.com/depsilon/shardloom
/readme https://github.com/depsilon/shardloom#readme
/readme.html https://github.com/depsilon/shardloom#readme
""",
    )
    write(
        WEBSITE / "_headers",
        """
/*
  X-Content-Type-Options: nosniff
  X-Frame-Options: DENY
  Referrer-Policy: strict-origin-when-cross-origin
  Permissions-Policy: camera=(), microphone=(), geolocation=()
  Content-Security-Policy: default-src 'self'; script-src 'self'; worker-src 'self'; style-src 'self'; img-src 'self' data:; font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'

/assets/*
  Cache-Control: public, max-age=3600

/assets/data/*
  Cache-Control: public, max-age=300

/*.html
  Cache-Control: public, max-age=300

/robots.txt
  Cache-Control: public, max-age=3600

/sitemap.xml
  Cache-Control: public, max-age=3600
""",
    )
    write(WEBSITE / "robots.txt", "User-agent: *\nAllow: /\nSitemap: https://shardloom.io/sitemap.xml")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--benchmark-manifest",
        type=Path,
        default=BENCHMARK_MANIFEST,
        help="Committed benchmark manifest used for website rendering.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest_path = args.benchmark_manifest
    if manifest_path != BENCHMARK_MANIFEST:
        raise SystemExit("custom benchmark manifests are not supported by the minimal website reset")
    manifest, results = benchmark_summary()
    flow_markdown = FLOW_SOURCE.read_text(encoding="utf-8")

    DATA.mkdir(parents=True, exist_ok=True)
    write(DATA / "compute-engine-flow-reference.md", flow_markdown)
    write(DATA / "benchmark-evidence.json", json.dumps(results, indent=2, sort_keys=True))
    home_html = home_page(manifest, results)
    benchmark_html = benchmarks_page(manifest, results)
    flow_html = compute_flow_page(flow_markdown)
    write(WEBSITE / "index.html", home_html)
    write(WEBSITE / "benchmarks.html", benchmark_html)
    write(WEBSITE / "benchmarks" / "index.html", benchmark_html)
    write(WEBSITE / "compute-engine-flow.html", flow_html)
    write(WEBSITE / "compute-engine-flow" / "index.html", flow_html)
    write(WEBSITE / "404.html", not_found_page())
    write(WEBSITE / "sitemap.xml", sitemap())
    write_support_files()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
