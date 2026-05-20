const fs = require("fs");
const path = require("path");

const root = __dirname;
const repoRoot = path.resolve(root, "..");

const requiredFiles = [
  "index.html",
  "start.html",
  "start/index.html",
  "field-guide.html",
  "field-guide/index.html",
  "field-guide/no-fallback/index.html",
  "field-guide/vortex-ingest/index.html",
  "use-cases.html",
  "use-cases/index.html",
  "use-cases/first-10-minutes-local-smoke/index.html",
  "use-cases/compatibility-import-certified-local/index.html",
  "benchmarks.html",
  "benchmarks/index.html",
  "architecture.html",
  "architecture/index.html",
  "compute-engine-flow.html",
  "compute-engine-flow/index.html",
  "status.html",
  "status/index.html",
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
  "assets/data/use-case-index.json",
  "assets/benchmarks/latest/manifest.json",
  "assets/benchmarks/latest/benchmark-results.json",
];

const forbiddenRuntimeText = [
  "raw.githubusercontent.com",
  "pagefind",
  "pagefind-modal",
];

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
  "start.html",
  "field-guide.html",
  "field-guide/no-fallback/index.html",
  "use-cases.html",
  "use-cases/first-10-minutes-local-smoke/index.html",
  "benchmarks.html",
  "benchmarks/index.html",
  "architecture.html",
  "compute-engine-flow.html",
  "compute-engine-flow/index.html",
  "status.html",
  "404.html",
];
for (const file of htmlFiles) {
  const content = read(file);
  assert(content.includes('/assets/logo/shardloom-favicon.png'), `${file} must use favicon asset`);
  assert(content.includes('/assets/site.css'), `${file} must use shared CSS`);
  assert(content.includes('<link rel="canonical"'), `${file} must include canonical URL`);
  assert(content.includes('property="og:title"'), `${file} must include OG metadata`);
}

const index = read("index.html");
assert(
  index.includes("Evidence-gated compute over Vortex-prepared data."),
  "home page hero must use evidence-gated route language",
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
  "Certified cold ingest/stage route",
  "Prepared warm query route",
  "Artifact lane availability",
  "Claim-gate distribution",
  "Prepared/native source-state coverage",
  "source_state_coverage_all_requested_scenarios_classified",
  "Raw timing tables",
  "Performance claim",
]) {
  assert(benchmarks.includes(required), `benchmarks page missing ${required}`);
}

const flow = read("compute-engine-flow.html");
for (const required of [
  "SQL and Python are front doors.",
  "prepared_vortex",
  "VortexPreparedState",
  "UniversalIngress",
  "Raw Mermaid source",
]) {
  assert(flow.includes(required), `compute-flow page missing ${required}`);
}

const fieldGuide = read("field-guide.html");
for (const required of [
  "A compact atlas for ShardLoom vocabulary.",
  "UniversalIngress",
  "vortex_ingest",
  "VortexPreparedState",
  "No fallback",
  "claim_gate_status",
]) {
  assert(fieldGuide.includes(required), `field guide missing ${required}`);
}

const useCases = read("use-cases.html");
for (const required of [
  "Can ShardLoom do my thing?",
  "compatibility_import_certified",
  "fallback_attempted=false",
  "claim_gate_status",
]) {
  assert(useCases.includes(required), `use cases page missing ${required}`);
}

const status = read("status.html");
for (const required of [
  "Support status stays visible.",
  "Local CSV",
  "Local JSONL / NDJSON",
  "S3 / GCS / ADLS",
  "Iceberg / Delta / Hudi",
  "Foundry",
  "Package / release",
]) {
  assert(status.includes(required), `status page missing ${required}`);
}

const redirects = read("_redirects");
for (const legacy of ["/can-i-use-this", "/status.html", "/readme", "/docs"]) {
  assert(redirects.includes(legacy), `_redirects must preserve legacy route: ${legacy}`);
}

const manifest = JSON.parse(read("assets/benchmarks/latest/manifest.json"));
assert(manifest.performance_claim_allowed === false, "benchmark manifest must block performance claims");
assert(Array.isArray(manifest.expected_lanes), "benchmark manifest must expose expected_lanes");
assert(Array.isArray(manifest.available_lanes), "benchmark manifest must expose available_lanes");
assert(Array.isArray(manifest.missing_lanes), "benchmark manifest must expose missing_lanes");

console.log("website static asset validation passed");
