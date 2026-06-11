import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const out = path.resolve(root, "..", "website");
const publicRoot = path.resolve(root, "..", "website-public");
const canonicalLegacyRoutes = new Set(["field-guide"]);

function copyPublicPath(relativePath) {
  const source = path.join(publicRoot, relativePath);
  const target = path.join(out, relativePath);
  if (!fs.existsSync(source)) {
    throw new Error(`missing public asset path ${relativePath}: ${source}`);
  }
  fs.mkdirSync(path.dirname(target), { recursive: true });
  if (fs.statSync(source).isDirectory() && fs.existsSync(target)) {
    fs.rmSync(target, { recursive: true, force: true });
  }
  fs.cpSync(source, target, { recursive: true, force: true });
}

function removeDuplicateSuffixedArtifacts(directory) {
  if (!fs.existsSync(directory)) return;
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const child = path.join(directory, entry.name);
    if (/ \d+(?:\.[^.]+)?$/.test(entry.name)) {
      fs.rmSync(child, { recursive: true, force: true });
      continue;
    }
    if (entry.isDirectory()) removeDuplicateSuffixedArtifacts(child);
  }
}

function copyLegacyHtml(route) {
  const legacyDirectory = path.join(out, `${route}.html`);
  const customSource = path.join(legacyDirectory, "index.html");
  const canonicalSource = path.join(out, route, "index.html");
  const source = canonicalLegacyRoutes.has(route)
    ? canonicalSource
    : fs.existsSync(customSource)
      ? customSource
      : canonicalSource;
  const target = path.join(out, `${route}.html`);
  if (!fs.existsSync(source)) {
    throw new Error(`missing source for legacy route ${route}: ${source}`);
  }
  const html = fs.readFileSync(source, "utf8");
  if (fs.existsSync(legacyDirectory)) fs.rmSync(legacyDirectory, { recursive: true, force: true });
  fs.writeFileSync(target, html, "utf8");
}

for (const route of [
  "about",
  "start",
  "field-guide",
  "use-cases",
  "benchmarks",
  "architecture",
  "compute-engine-flow",
  "status",
  "docs",
]) {
  copyLegacyHtml(route);
}

for (const relativePath of [
  "_headers",
  "_redirects",
  "robots.txt",
  "validate_static_assets.js",
  "assets/site.css",
  "assets/site.js",
  "assets/logo",
  "assets/data",
  "assets/benchmarks",
]) {
  copyPublicPath(relativePath);
}

removeDuplicateSuffixedArtifacts(out);

console.log("wrote legacy .html compatibility pages and refreshed public assets");
