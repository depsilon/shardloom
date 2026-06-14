const fs = require("fs");
const path = require("path");

const root = __dirname;
const repoRoot = path.resolve(root, "..");
const cloudflareStaticAssetMaxBytes = 25 * 1024 * 1024;

const requiredFiles = [
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
  "field-guide/no-fallback/index.html",
  "field-guide/vortex-ingest/index.html",
  "benchmarks.html",
  "benchmarks/index.html",
  "compute-engine-flow.html",
  "compute-engine-flow/index.html",
  "404.html",
  "robots.txt",
  "sitemap.xml",
  "_headers",
  "_redirects",
  "assets/site.css",
  "assets/site.js",
  "assets/logo/shardloom-favicon.png",
  "assets/logo/shardloom-logo.png",
  "assets/logo/shardloom-logo-trim.png",
  "assets/data/compute-engine-flow-reference.md",
  "assets/data/benchmark-evidence.json",
  "assets/benchmarks/latest/manifest.json",
  "assets/benchmarks/latest/benchmark-results.json",
  "pagefind/pagefind-entry.json",
];

const removedWebsiteSurfaces = [
  "architecture.html",
  "architecture/index.html",
  "docs.html",
  "docs/index.html",
  "status.html",
  "status/index.html",
  "use-cases.html",
  "use-cases/index.html",
];

const forbiddenRuntimeText = [
  "raw.githubusercontent.com",
  "docs/architecture/phased-execution-plan.md",
];
const statusVocabulary = new Set([
  "runtime_supported",
  "scoped_runtime_supported",
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
  "claim_grade",
  "not_claim_grade",
  "external_baseline_only",
  "future",
  "current",
  "not reported",
  "stale_or_dirty",
  "stale or dirty",
  "optimization_ready",
  "not_optimization_ready",
]);

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function exists(relativePath) {
  return fs.existsSync(path.join(root, relativePath));
}

function read(relativePath) {
  return fs.readFileSync(path.join(root, relativePath), "utf8");
}

function collectFiles(directory, prefix = "") {
  const entries = fs.readdirSync(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const relativePath = path.join(prefix, entry.name).replace(/\\/g, "/");
    const absolutePath = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      files.push(...collectFiles(absolutePath, relativePath));
    } else if (relativePath !== "validate_static_assets.js") {
      files.push(relativePath);
    }
  }
  return files;
}

for (const file of requiredFiles) {
  assert(exists(file), `Missing required website file: ${file}`);
}
for (const file of removedWebsiteSurfaces) {
  assert(!exists(file), `Removed website surface still exists: ${file}`);
}
for (const file of collectFiles(root)) {
  const size = fs.statSync(path.join(root, file)).size;
  assert(
    size <= cloudflareStaticAssetMaxBytes,
    `Cloudflare Workers static asset exceeds 25 MiB: ${file} (${size} bytes)`,
  );
}

const canonicalFlow = fs
  .readFileSync(path.join(repoRoot, "docs/architecture/compute-engine-flow-reference.md"), "utf8")
  .replace(/\r\n/g, "\n");
const websiteFlow = read("assets/data/compute-engine-flow-reference.md").replace(/\r\n/g, "\n");
assert(
  canonicalFlow === websiteFlow,
  "assets/data/compute-engine-flow-reference.md must match docs/architecture/compute-engine-flow-reference.md",
);

const wranglerToml = fs.readFileSync(path.join(repoRoot, "wrangler.toml"), "utf8");
assert(
  /\[assets\][\s\S]*directory\s*=\s*["']\.\/website["']/.test(wranglerToml),
  'wrangler.toml must serve static assets from "./website"',
);

const runtimeFiles = collectFiles(root).filter((file) =>
  [".html", ".css", ".js", ".xml", ".txt"].includes(path.extname(file)) ||
  ["_headers", "_redirects"].includes(file),
);
for (const file of runtimeFiles) {
  const content = read(file);
  for (const forbidden of forbiddenRuntimeText) {
    assert(!content.includes(forbidden), `Runtime file still contains ${forbidden}: ${file}`);
  }
}

const htmlFiles = [
  "index.html",
  "about.html",
  "start.html",
  "field-guide.html",
  "field-guide/start-local-proof/index.html",
  "field-guide/python-surface/index.html",
  "field-guide/benchmark-methodology/index.html",
  "field-guide/limitations/index.html",
  "field-guide/no-fallback/index.html",
  "benchmarks.html",
  "benchmarks/index.html",
  "compute-engine-flow.html",
  "compute-engine-flow/index.html",
  "404.html",
];
for (const file of htmlFiles) {
  const content = read(file);
  assert(content.includes('/assets/logo/shardloom-favicon.png'), `${file} must use favicon asset`);
  assert(content.includes('/assets/site.css'), `${file} must use shared CSS`);
  assert(content.includes('<link rel="canonical"'), `${file} must include canonical URL`);
  assert(content.includes('property="og:title"'), `${file} must include OG metadata`);
}
for (const file of collectFiles(root).filter((candidate) => candidate.endsWith(".html"))) {
  const content = read(file);
  const isStarlight = content.includes("Starlight v") || content.includes("starlight__sidebar");
  assert(/<html\b[^>]*\blang="en"/.test(content), `${file} must declare language`);
  assert(/<title>[^<]+<\/title>/.test(content), `${file} must include a document title`);
  assert(/<meta name="viewport" content="width=device-width, initial-scale=1"/.test(content), `${file} must include responsive viewport metadata`);
  assert(/<meta name="description" content="[^"]+"/.test(content), `${file} must include meta description`);
  assert((content.match(/<h1[ >]/g) || []).length === 1, `${file} must include exactly one h1`);
  if (!isStarlight) {
    assert(!content.includes("<details open"), `${file} must keep drawers collapsed by default`);
  }
  for (const image of content.match(/<img\b[^>]*>/g) || []) {
    assert(/\salt=/.test(image), `${file} image missing alt text: ${image}`);
    assert(/\swidth="\d+"/.test(image), `${file} image missing stable width: ${image}`);
    assert(/\sheight="\d+"/.test(image), `${file} image missing stable height: ${image}`);
  }
  for (const match of content.matchAll(/<span class="status-chip[^"]*">([^<]+)<\/span>/g)) {
    assert(statusVocabulary.has(match[1]), `${file} has unexpected status chip text: ${match[1]}`);
  }
}

const css = read("assets/site.css");
for (const required of [
  ":focus-visible",
  "input:focus-visible",
  "@media (prefers-reduced-motion: reduce)",
  ".status-chip",
  ".filter-count",
]) {
  assert(css.includes(required), `site CSS missing ${required}`);
}

const index = read("index.html");
assert(
  index.includes("A standalone encoded-columnar engine for Vortex-native routes"),
  "home page hero must use current Vortex-native route language",
);
assert(
  index.includes("Hot runtime is not publication proof."),
  "home page hero must separate hot runtime from publication proof",
);
for (const required of [
  "UniversalIngress",
  "vortex_ingest",
  "VortexPreparedState",
  "fallback_attempted",
  "external_engine_invoked",
  "claim_gate_status",
  "Start local proof",
  "Read Field Guide",
  "View benchmark evidence",
]) {
  assert(index.includes(required), `home page product console missing ${required}`);
}
assert(index.includes("Open GitHub"), "home page must link to GitHub");

const benchmarks = read("benchmarks.html");
for (const required of [
  "Benchmark Evidence, Not a Leaderboard",
  "Route timing dashboard",
  "Route lanes are the comparison surface.",
  "data-route-timing-surface-dashboard",
  "ShardLoom Prepare-Once First Query",
  "ShardLoom Cold Certified Route",
  "ShardLoom Prepare-Once Batch",
  "ShardLoom Warm Prepared Query",
  "ShardLoom Native Vortex Query",
  "External Baseline End-to-End",
  "hot_runtime",
  "publication_proof",
  "Publication-proof route geomean",
  "Hot route geomean",
  "result-sink and evidence-render work",
  "timing_surface=hot_runtime",
  "timing_surface=publication_proof",
  "Stage attribution",
  "Included hot runtime",
  "Included publication proof",
  "Diagnostic only",
  "Optimization direction",
  "Route-share attribution",
  "diagnostic_absent_or_retired",
  "target_disappearance_policy=diagnostic_absent_or_retired_not_release_blocker",
  "retired optimization targets",
  "Runtime support is separate from claim readiness.",
  "ShardLoom unsupported rows",
  "External baseline unsupported rows",
  "Benchmark data ownership",
  "Static mirrors have one canonical artifact.",
  "website-public/assets/benchmarks/latest/benchmark-results.json",
  "mirror digest checks",
  "Artifact lane availability",
  "full_local",
  "Public front doors",
  "Route rows name the user-facing prepared paths.",
  "Benchmark ETL scenarios from Python",
  "clean-cast-filter-write.jsonl",
  "benchmark route owns the current top-N-per-group scenario",
  "nested_payload",
  "not_timing_row_route_identity_only",
  "SourceState",
  "GeneratedSourceState",
  "VortexPreparedState",
  "Format coverage rows",
  "Claim-grade closeout",
  "Prepared/native source-state coverage",
  "source_state_coverage_all_requested_scenarios_classified",
  "Raw timing tables",
  "Route timing surfaces",
  "Performance claim allowed",
]) {
  assert(benchmarks.includes(required), `benchmarks page missing ${required}`);
}
assert(
  !benchmarks.includes("Current artifact profile: <strong>full_local_plus_spark</strong>"),
  "benchmarks page must not show full_local_plus_spark as the current published profile",
);
const benchmarkEvidence = JSON.parse(read("assets/benchmarks/latest/benchmark-results.json"));
assert(
  benchmarkEvidence.published_benchmark_rows_inlined === "summary_only",
  "benchmark-results.json must inline only summary rows for deployable asset safety",
);
assert(
  Array.isArray(benchmarkEvidence.published_benchmark_row_chunks) &&
    benchmarkEvidence.published_benchmark_row_chunks.length > 0,
  "benchmark-results.json must reference full benchmark row chunks",
);
const summaryRows = Array.isArray(benchmarkEvidence.published_benchmark_rows)
  ? benchmarkEvidence.published_benchmark_rows
  : [];
const shardloomSummaryRows = summaryRows.filter((row) => String(row.engine ?? "").startsWith("shardloom"));
for (const field of [
  "route_runtime_status",
  "route_lane_id",
  "route_display_name",
  "start_state",
  "end_state",
  "includes_preparation",
  "includes_query",
  "includes_output",
  "includes_evidence",
  "route_comparable_to_external_end_to_end",
  "performance_claim_allowed",
  "production_claim_allowed",
  "spark_replacement_claim_allowed",
  "vortex_scan_millis",
  "operator_compute_millis",
  "result_sink_write_millis",
  "fast_path_attribution_schema_version",
  "runtime_execution_ms",
  "output_delivery_ms",
  "evidence_capture_ms",
  "evidence_render_ms",
  "certificate_link_ms",
  "certificate_link_status",
  "evidence_required_for_claim",
  "evidence_render_included_in_route_total",
  "operator_mode_inventory_schema_version",
  "operator_execution_mode",
  "encoded_native_operators",
  "residual_native_operators",
  "materialized_temporary_operators",
  "operator_blocker_code",
  "operator_hot_path_candidate",
  "operator_hot_path_candidate_status",
]) {
  assert(
    shardloomSummaryRows.every((row) => Object.prototype.hasOwnProperty.call(row, field)),
    `summary ShardLoom benchmark rows must retain ${field} for detailed timing tables`,
  );
}
assert(
  shardloomSummaryRows.every((row) => row.route_runtime_status !== "external_baseline_only"),
  "ShardLoom summary rows must not be labeled external_baseline_only",
);
assert(
  shardloomSummaryRows.filter((row) => row.status === "unsupported" || row.route_runtime_status === "unsupported")
    .length === 0,
  "published ShardLoom summary rows must not contain unsupported route gaps",
);
const externalSummaryRows = summaryRows.filter((row) => !String(row.engine ?? "").startsWith("shardloom"));
assert(
  externalSummaryRows.every((row) => row.route_runtime_status === "external_baseline_only"),
  "external summary rows must be labeled route_runtime_status=external_baseline_only",
);
for (const chunk of benchmarkEvidence.published_benchmark_row_chunks) {
  assert(chunk.path, "benchmark row chunk missing path");
  const chunkPath = chunk.path.replace(/^website\//, "");
  assert(exists(chunkPath), `Missing benchmark row chunk: ${chunkPath}`);
}

const flow = read("compute-engine-flow.html");
for (const required of [
  "SQL and Python are front doors.",
  "prepared_vortex",
  "VortexPreparedState",
  "UniversalIngress",
  "Rendered architecture diagrams",
  "data-rendered-diagram",
  "Raw Mermaid source",
]) {
  assert(flow.includes(required), `compute-flow page missing ${required}`);
}

const fieldGuide = read("field-guide.html");
for (const required of [
  "A compact Starlight docs shell",
  "UniversalIngress",
  "vortex_ingest",
  "VortexPreparedState",
  "No fallback",
  "claim_gate_status",
]) {
  assert(fieldGuide.includes(required), `field guide missing ${required}`);
}

const startLocalProof = read("field-guide/start-local-proof/index.html");
for (const required of [
  "Start local proof",
  "fallback_attempted=false",
  "external_engine_invoked=false",
  "claim_gate_status",
]) {
  assert(startLocalProof.includes(required), `start local proof doc missing ${required}`);
}

const pythonSurface = read("field-guide/python-surface/index.html");
for (const required of [
  "Python surface",
  "ctx = context",
  "read_csv",
  "nested_payload",
]) {
  assert(pythonSurface.includes(required), `python surface doc missing ${required}`);
}

const benchmarkMethodology = read("field-guide/benchmark-methodology/index.html");
for (const required of [
  "Benchmark methodology",
  "hot_runtime",
  "publication_proof",
  "external_baseline",
]) {
  assert(benchmarkMethodology.includes(required), `benchmark methodology doc missing ${required}`);
}

const limitations = read("field-guide/limitations/index.html");
for (const required of [
  "Limitations",
  "production support",
  "Spark displacement",
  "fallback engine",
]) {
  assert(limitations.includes(required), `limitations doc missing ${required}`);
}

const redirects = read("_redirects");
for (const legacy of [
  "/architecture",
  "/architecture.html",
  "/use-cases",
  "/use-cases.html",
  "/status",
  "/status.html",
  "/docs",
  "/docs.html",
  "/can-i-use-this",
  "/readme",
]) {
  assert(redirects.includes(legacy), `_redirects must preserve legacy route: ${legacy}`);
}

const manifest = JSON.parse(read("assets/benchmarks/latest/manifest.json"));
assert(manifest.performance_claim_allowed === false, "benchmark manifest must block performance claims");
assert(Array.isArray(manifest.expected_lanes), "benchmark manifest must expose expected_lanes");
assert(Array.isArray(manifest.available_lanes), "benchmark manifest must expose available_lanes");
assert(Array.isArray(manifest.missing_lanes), "benchmark manifest must expose missing_lanes");

console.log("website static asset validation passed");
