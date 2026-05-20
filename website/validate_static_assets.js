const fs = require("fs");
const path = require("path");

const root = __dirname;
const repoRoot = path.resolve(root, "..");

const requiredFiles = [
  "index.html",
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
  "assets/logo/shardloom-favicon.png",
  "assets/logo/shardloom-logo.png",
  "assets/logo/shardloom-logo-trim.png",
  "assets/data/compute-engine-flow-reference.md",
  "assets/data/benchmark-evidence.json",
  "assets/benchmarks/latest/manifest.json",
  "assets/benchmarks/latest/benchmark-results.json",
];

const forbiddenRuntimeText = [
  "raw.githubusercontent.com",
  "pagefind",
  "pagefind-modal",
  "Field Guide",
  "Use Case Atlas",
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

const index = read("index.html");
assert(index.includes("Evidence-first compute over Vortex data."), "home page hero must stay concise");
assert(index.includes("Open GitHub"), "home page must link to GitHub");

const benchmarks = read("benchmarks.html");
for (const required of [
  "Evidence, not a leaderboard.",
  "Artifact lane availability",
  "Claim-gate distribution",
  "Prepared/native source-state coverage",
  "source_state_coverage_all_requested_scenarios_classified",
  "Local timing context",
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

const redirects = read("_redirects");
for (const legacy of ["/field-guide", "/use-cases", "/status", "/readme"]) {
  assert(redirects.includes(legacy), `_redirects must preserve legacy route: ${legacy}`);
}

const manifest = JSON.parse(read("assets/benchmarks/latest/manifest.json"));
assert(manifest.performance_claim_allowed === false, "benchmark manifest must block performance claims");
assert(Array.isArray(manifest.expected_lanes), "benchmark manifest must expose expected_lanes");
assert(Array.isArray(manifest.available_lanes), "benchmark manifest must expose available_lanes");
assert(Array.isArray(manifest.missing_lanes), "benchmark manifest must expose missing_lanes");

console.log("website static asset validation passed");
