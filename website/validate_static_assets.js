const fs = require("fs");
const path = require("path");

const root = __dirname;
const repoRoot = path.resolve(root, "..");
const requiredFiles = [
  "assets/compute-flow.js",
  "assets/data/compute-engine-flow-reference.md",
  "assets/benchmarks/latest/manifest.json",
  "assets/benchmarks/latest/benchmark-results.json",
  "pagefind/pagefind-component-ui.css",
  "pagefind/pagefind-component-ui.js",
  "pagefind/pagefind-entry.json",
  "pagefind/pagefind.js",
  "pagefind/pagefind-worker.js",
  "pagefind/wasm.en.pagefind",
  "index.html",
  "status.html",
];

const runtimeFiles = [
  "index.html",
  "404.html",
  "benchmarks.html",
  "compute-engine-flow.html",
  "status.html",
  "use-cases/index.html",
  "readme.html",
  "_headers",
  "_redirects",
  "robots.txt",
  "sitemap.xml",
  "assets/compute-flow.js",
  "assets/use-cases.js",
  "assets/site.css",
  "assets/benchmarks/latest/manifest.json",
  "assets/benchmarks/latest/benchmark-results.json",
  "pagefind/pagefind-component-ui.css",
  "pagefind/pagefind-component-ui.js",
  "pagefind/pagefind.js",
  "pagefind/pagefind-worker.js",
];
const blockedGitHubRawHost = "raw." + "githubusercontent.com";

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

function readFromRepoRoot(relativePath) {
  return fs.readFileSync(path.join(repoRoot, relativePath), "utf8");
}

function collectFiles(directory, prefix = "") {
  const entries = fs.readdirSync(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    if (entry.name === "__pycache__") {
      continue;
    }
    const relativePath = path.join(prefix, entry.name);
    const absolutePath = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      files.push(...collectFiles(absolutePath, relativePath));
    } else {
      files.push(relativePath.replace(/\\/g, "/"));
    }
  }
  return files;
}

for (const relativePath of requiredFiles) {
  assert(exists(relativePath), `Missing required website file: ${relativePath}`);
}

const canonicalComputeFlow = readFromRepoRoot(
  "docs/architecture/compute-engine-flow-reference.md",
).replace(/\r\n/g, "\n");
const websiteComputeFlowSnapshot = read(
  "assets/data/compute-engine-flow-reference.md",
).replace(/\r\n/g, "\n");
assert(
  websiteComputeFlowSnapshot === canonicalComputeFlow,
  "website/assets/data/compute-engine-flow-reference.md must match docs/architecture/compute-engine-flow-reference.md",
);

const wranglerToml = readFromRepoRoot("wrangler.toml");
assert(
  /\[assets\][\s\S]*directory\s*=\s*["']\.\/website["']/.test(wranglerToml),
  'wrangler.toml must serve static assets from [assets] directory = "./website"',
);
assert(
  /\[assets\][\s\S]*html_handling\s*=\s*["']auto-trailing-slash["']/.test(wranglerToml),
  'wrangler.toml must set [assets] html_handling = "auto-trailing-slash" so root and directory index routes work',
);
assert(
  /\[assets\][\s\S]*not_found_handling\s*=\s*["']404-page["']/.test(wranglerToml),
  'wrangler.toml must set [assets] not_found_handling = "404-page"',
);

const htmlRuntimeFiles = collectFiles(root).filter((relativePath) =>
  relativePath.endsWith(".html"),
);
const filesToScanForRuntimeRefs = Array.from(
  new Set([...runtimeFiles, ...htmlRuntimeFiles]),
);

const redirects = read("_redirects")
  .split(/\r?\n/)
  .map((line) => line.trim())
  .filter((line) => line && !line.startsWith("#"))
  .map((line) => line.split(/\s+/));
const htmlRedirectTargets = redirects.filter((parts) => parts[1]?.endsWith(".html"));
assert(
  htmlRedirectTargets.length === 0,
  `_redirects must point aliases at extensionless canonical pages, not .html files: ${htmlRedirectTargets
    .map((parts) => parts.join(" "))
    .join(", ")}`,
);

for (const relativePath of filesToScanForRuntimeRefs) {
  if (!exists(relativePath)) {
    continue;
  }
  assert(
    !read(relativePath).includes(blockedGitHubRawHost),
    `Runtime file must not reference ${blockedGitHubRawHost}: ${relativePath}`,
  );
}

const useCasesJs = read("assets/use-cases.js");
assert(
  !useCasesJs.includes(blockedGitHubRawHost),
  "use-cases.js must not depend on GitHub raw runtime fetches",
);

const computeFlowJs = read("assets/compute-flow.js");
assert(
  !computeFlowJs.includes('cache: "no-store"'),
  "compute-flow.js must not bypass the short static cache for the local markdown snapshot",
);

const benchmarkManifest = JSON.parse(read("assets/benchmarks/latest/manifest.json"));
assert(
  benchmarkManifest.schema_version === "shardloom.website_benchmark_manifest.v1",
  "benchmark manifest must use shardloom.website_benchmark_manifest.v1",
);
assert(
  benchmarkManifest.performance_claim_allowed === false,
  "benchmark manifest must keep performance_claim_allowed=false",
);
assert(
  Array.isArray(benchmarkManifest.expected_lanes) &&
    Array.isArray(benchmarkManifest.available_lanes) &&
    Array.isArray(benchmarkManifest.missing_lanes),
  "benchmark manifest must expose expected_lanes, available_lanes, and missing_lanes",
);
if (benchmarkManifest.artifact_paths?.json) {
  assert(
    fs.existsSync(path.join(repoRoot, benchmarkManifest.artifact_paths.json)),
    `benchmark manifest artifact_paths.json must exist: ${benchmarkManifest.artifact_paths.json}`,
  );
}

const indexHtml = read("index.html");
assert(
  /<img class="brand-icon" src="\/assets\/logo\/shardloom-favicon\.png"/.test(indexHtml),
  "The global nav corner must use the favicon/icon asset",
);
assert(
  /<img class="hero-logo" src="\/assets\/logo\/shardloom-logo-trim\.png"/.test(indexHtml),
  "The home hero must use the trimmed ShardLoom logo asset",
);

const fieldGuideIndexHtml = read("field-guide/index.html");
for (const required of [
  "/pagefind/pagefind-component-ui.css",
  "/pagefind/pagefind-component-ui.js",
  "pagefind-modal-trigger",
  "pagefind-filter-dropdown",
  "data-pagefind-filter=\"section\"",
  "data-pagefind-filter=\"status\"",
  "atlas-density-note",
  "category-toc-band",
  "compact-term-row",
  "reference-badge",
]) {
  assert(
    fieldGuideIndexHtml.includes(required),
    `Field Guide search page must include ${required}`,
  );
}

const statusHtml = read("status.html");
for (const required of [
  "Capability status matrix",
  "data-status-matrix-filters",
  "data-status-matrix-filter=\"status\"",
  "data-status-matrix-filter=\"input\"",
  "data-status-matrix-filter=\"output\"",
  "data-status-matrix-filter=\"execution\"",
  "data-status-matrix-filter=\"evidence\"",
  "data-status-matrix-filter=\"platform\"",
  "data-status-matrix-grid",
  "data-status-matrix-count",
  "docs/use-cases/use-case-index.yml",
]) {
  assert(statusHtml.includes(required), `Status page must include ${required}`);
}

for (const useCasePage of htmlRuntimeFiles.filter((relativePath) =>
  relativePath.startsWith("use-cases/") && relativePath !== "use-cases/index.html"
)) {
  const source = read(useCasePage);
  assert(
    source.includes("Related Field Guide Terms"),
    `${useCasePage} must render reverse Field Guide term links`,
  );
  assert(
    /href="\/field-guide\/[^"]+"/.test(source),
    `${useCasePage} must link back to at least one Field Guide term`,
  );
  assert(
    source.includes('data-citation-block="reference-files"'),
    `${useCasePage} must render a source-linked citation block`,
  );
  assert(
    source.includes("What this proves:"),
    `${useCasePage} citation block must explain what each source proves`,
  );
}

for (const dossierPage of htmlRuntimeFiles.filter((relativePath) =>
  relativePath.startsWith("field-guide/") && relativePath !== "field-guide/index.html"
)) {
  const source = read(dossierPage);
  assert(
    source.includes('data-citation-block="reference-files"'),
    `${dossierPage} must render a source-linked citation block`,
  );
  assert(
    source.includes("What this proves:"),
    `${dossierPage} citation block must explain what each source proves`,
  );
}

const pagefindEntry = JSON.parse(read("pagefind/pagefind-entry.json"));
assert(
  pagefindEntry.version === "1.5.2",
  "Committed Pagefind static bundle must be generated with Pagefind 1.5.2",
);
assert(
  pagefindEntry.languages?.en?.page_count >= 90,
  "Committed Pagefind index must cover generated website pages",
);

for (const headerLogoFile of htmlRuntimeFiles.filter((relativePath) => !["404.html", "index.html"].includes(relativePath))) {
  const source = read(headerLogoFile);
  assert(
    /<img class="brand-icon" src="\/assets\/logo\/shardloom-favicon\.png"/.test(source),
    `${headerLogoFile} global nav corner must use the favicon/icon asset`,
  );
  assert(
    /<img class="page-header-logo" src="\/assets\/logo\/shardloom-logo-trim\.png"/.test(source),
    `${headerLogoFile} page header must use the trimmed ShardLoom logo asset`,
  );
}

const assetPattern = /\b(?:src|href|content)=["']([^"']*\/assets\/[^"']+)["']/g;
const missingAssets = [];
let match;
while ((match = assetPattern.exec(indexHtml)) !== null) {
  let assetPath = match[1];
  if (/^https:\/\/shardloom\.io\//.test(assetPath)) {
    assetPath = assetPath.replace(/^https:\/\/shardloom\.io\//, "/");
  }
  if (!assetPath.startsWith("/assets/")) {
    continue;
  }
  const relativePath = assetPath.replace(/^\//, "");
  if (!exists(relativePath)) {
    missingAssets.push(relativePath);
  }
}

assert(
  missingAssets.length === 0,
  `index.html references missing committed assets: ${missingAssets.join(", ")}`,
);

function localFileForPath(sitePath) {
  const pathWithoutQuery = sitePath.split("?")[0];
  const [pathname] = pathWithoutQuery.split("#");
  if (pathname === "" || pathname === "/") {
    return "index.html";
  }
  if (!pathname.startsWith("/")) {
    return null;
  }
  const relativePath = pathname.replace(/^\//, "");
  if (relativePath.endsWith("/")) {
    return `${relativePath}index.html`;
  }
  if (exists(relativePath)) {
    const stats = fs.statSync(path.join(root, relativePath));
    if (stats.isDirectory()) {
      return `${relativePath}/index.html`;
    }
    return relativePath;
  }
  if (exists(`${relativePath}/index.html`)) {
    return `${relativePath}/index.html`;
  }
  if (exists(`${relativePath}.html`)) {
    return `${relativePath}.html`;
  }
  return relativePath;
}

function fragmentForPath(sitePath) {
  const hashIndex = sitePath.indexOf("#");
  if (hashIndex === -1) {
    return "";
  }
  return sitePath.slice(hashIndex + 1);
}

const missingLocalRefs = [];
const missingAnchors = [];
const localRefPattern = /\b(?:src|href)=["']([^"']+)["']/g;

for (const relativePath of htmlRuntimeFiles) {
  const source = read(relativePath);
  while ((match = localRefPattern.exec(source)) !== null) {
    const target = match[1];
    if (
      target.startsWith("http:") ||
      target.startsWith("https:") ||
      target.startsWith("mailto:")
    ) {
      continue;
    }
    const localFile = target.startsWith("#")
      ? relativePath
      : localFileForPath(target);
    if (!localFile) {
      continue;
    }
    if (!exists(localFile)) {
      missingLocalRefs.push(`${relativePath} -> ${target}`);
      continue;
    }
    const fragment = target.startsWith("#")
      ? target.slice(1)
      : fragmentForPath(target);
    if (fragment) {
      const targetSource = read(localFile);
      const idPattern = new RegExp(
        `\\b(?:id|name)=["']${fragment.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}["']`,
      );
      if (!idPattern.test(targetSource)) {
        missingAnchors.push(`${relativePath} -> ${target}`);
      }
    }
  }
}

assert(
  missingLocalRefs.length === 0,
  `Website runtime files reference missing local files: ${missingLocalRefs.join(", ")}`,
);

assert(
  missingAnchors.length === 0,
  `Website runtime files reference missing anchors: ${missingAnchors.join(", ")}`,
);

console.log("website static asset validation passed");
