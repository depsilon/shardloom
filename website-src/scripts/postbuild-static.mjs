import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  assertNoDuplicateSuffixedArtifacts,
  duplicateSettleOptions,
  removeDuplicateSuffixedArtifacts,
  settleDuplicateSuffixedArtifacts,
} from "./static-artifact-hygiene.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const out = path.resolve(root, "..", "website");
const publicRoot = path.resolve(root, "..", "website-public");

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

const publicRootPreCopyRemoved = removeDuplicateSuffixedArtifacts(publicRoot);

function copyLegacyHtml(route, canonicalRoute = route) {
  const legacyDirectory = path.join(out, `${route}.html`);
  // Old bookmarks must serve the current page, never a separate legacy implementation.
  const source = path.join(out, canonicalRoute, "index.html");
  const target = path.join(out, `${route}.html`);
  if (!fs.existsSync(source)) {
    throw new Error(`missing source for legacy route ${route}: ${source}`);
  }
  const html = fs.readFileSync(source, "utf8");
  if (fs.existsSync(legacyDirectory)) fs.rmSync(legacyDirectory, { recursive: true, force: true });
  fs.writeFileSync(target, html, "utf8");
  if (route !== canonicalRoute) {
    const directory = path.join(out, route);
    fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, "index.html"), html, "utf8");
  }
}

for (const route of [
  "about",
  "start",
  "field-guide",
]) {
  copyLegacyHtml(route);
}
copyLegacyHtml("benchmarks", "field-guide/benchmark-methodology");
copyLegacyHtml("compute-engine-flow", "field-guide/compute-flow");

for (const relativePath of [
  "_headers",
  "_redirects",
  "robots.txt",
  "assets/parallax-home.css",
  "assets/parallax-home.js",
  "assets/site.css",
  "assets/logo",
  "assets/data",
]) {
  copyPublicPath(relativePath);
}

const settleOptions = duplicateSettleOptions();
const finalRemoved = await settleDuplicateSuffixedArtifacts([out, publicRoot], settleOptions);
assertNoDuplicateSuffixedArtifacts([out, publicRoot]);

console.log(
  [
    "wrote canonical .html compatibility pages and refreshed public assets",
    `duplicate_suffixed_removed=${publicRootPreCopyRemoved.length + finalRemoved.length}`,
    `settle_passes=${settleOptions.passes}`,
    `settle_delay_ms=${settleOptions.delayMs}`,
  ].join("; "),
);
