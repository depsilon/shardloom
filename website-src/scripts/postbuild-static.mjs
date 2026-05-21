import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const out = path.resolve(root, "..", "website");

function copyLegacyHtml(route) {
  const legacyDirectory = path.join(out, `${route}.html`);
  const customSource = path.join(legacyDirectory, "index.html");
  const canonicalSource = path.join(out, route, "index.html");
  const source = fs.existsSync(customSource) ? customSource : canonicalSource;
  const target = path.join(out, `${route}.html`);
  if (!fs.existsSync(source)) {
    throw new Error(`missing source for legacy route ${route}: ${source}`);
  }
  const html = fs.readFileSync(source, "utf8");
  if (fs.existsSync(legacyDirectory)) fs.rmSync(legacyDirectory, { recursive: true, force: true });
  fs.writeFileSync(target, html, "utf8");
}

for (const route of [
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

console.log("wrote legacy .html compatibility pages");
