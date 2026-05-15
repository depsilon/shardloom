const fs = require("fs");
const path = require("path");

const root = __dirname;
const requiredFiles = [
  "assets/compute-flow.js",
  "assets/data/compute-engine-flow-reference.md",
  "index.html",
];

const runtimeFiles = [
  "index.html",
  "404.html",
  "benchmarks.html",
  "compute-engine-flow.html",
  "readme.html",
  "_headers",
  "_redirects",
  "robots.txt",
  "sitemap.xml",
  "assets/compute-flow.js",
  "assets/site.css",
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

for (const relativePath of requiredFiles) {
  assert(exists(relativePath), `Missing required website file: ${relativePath}`);
}

for (const relativePath of runtimeFiles) {
  if (!exists(relativePath)) {
    continue;
  }
  assert(
    !read(relativePath).includes(blockedGitHubRawHost),
    `Runtime file must not reference ${blockedGitHubRawHost}: ${relativePath}`,
  );
}

const computeFlowJs = read("assets/compute-flow.js");
assert(
  !computeFlowJs.includes('cache: "no-store"'),
  "compute-flow.js must not bypass the short static cache for the local markdown snapshot",
);

const indexHtml = read("index.html");
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

console.log("website static asset validation passed");
