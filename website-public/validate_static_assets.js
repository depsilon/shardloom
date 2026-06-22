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
  "global_runtime_supported",
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

function collectDuplicateSuffixedArtifacts(directory, prefix = "") {
  const entries = fs.readdirSync(directory, { withFileTypes: true });
  const duplicates = [];
  for (const entry of entries) {
    const relativePath = path.join(prefix, entry.name).replace(/\\/g, "/");
    const absolutePath = path.join(directory, entry.name);
    if (/ \d+(?:\.[^.]+)?$/.test(entry.name)) {
      duplicates.push(relativePath);
      continue;
    }
    if (entry.isDirectory()) {
      duplicates.push(...collectDuplicateSuffixedArtifacts(absolutePath, relativePath));
    }
  }
  return duplicates;
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
const duplicateSuffixedArtifacts = collectDuplicateSuffixedArtifacts(root);
assert(
  duplicateSuffixedArtifacts.length === 0,
  `Duplicate-suffixed generated website artifacts remain: ${duplicateSuffixedArtifacts.join(", ")}`,
);

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
  index.includes("Route totals name their surface."),
  "home page must preserve explicit route timing surface language",
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
  "Open benchmark comparison",
]) {
  assert(index.includes(required), `home page product console missing ${required}`);
}
assert(index.includes("Open GitHub"), "home page must link to GitHub");

const benchmarks = read("benchmarks.html");
for (const required of [
  "ClickBench",
  "Open ClickBench",
  "https://benchmark.clickhouse.com/",
  "Use ClickBench as the public comparison surface.",
  "Local benchmark artifacts remain useful for engineering validation",
  "Public comparison belongs on ClickBench",
]) {
  assert(benchmarks.includes(required), `benchmarks page missing ${required}`);
}
assert(
  !benchmarks.includes("data-route-timing-surface-dashboard"),
  "benchmarks page must not render the retired internal dashboard",
);

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
  "Normal Package Shape",
  "ctx = sl.context()",
  "ctx.read(path)",
  "prepared = ctx.prepare_vortex(",
  "scenario_selective-filter_fallback_attempted",
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

console.log("website static asset validation passed");
