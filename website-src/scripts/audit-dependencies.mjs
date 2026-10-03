import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const reviewedPaths = [
  "website-src/package-lock.json",
  "website-src/astro.config.mjs",
  "wrangler.jsonc",
  "wrangler.toml",
  "website-src/node_modules/astro/dist/assets/build/remote.js",
  "website-src/node_modules/http-cache-semantics/index.js",
].sort();

/** An exception applies to one advisory and its dependency-chain reports only. */
export function evaluateAudit(report, exception, {
  now = new Date(),
  readPinnedFile = (path) => readFileSync(resolve(root, path)),
} = {}) {
  if (report?.error || report?.auditReportVersion !== 2
      || !report.vulnerabilities || typeof report.vulnerabilities !== "object"
      || Array.isArray(report.vulnerabilities)) {
    throw new Error("npm audit did not return a complete version-2 report");
  }
  const entries = Object.entries(report.vulnerabilities);
  if (report.metadata?.vulnerabilities?.total !== entries.length) {
    throw new Error("npm audit vulnerability total does not match its findings");
  }
  if (entries.length === 0) return { status: "clean", findings: 0 };
  if (exception?.approved !== true) {
    throw new Error("Website advisory exception requires explicit maintainer approval");
  }
  const expiry = Date.parse(exception.expires);
  if (!Number.isFinite(expiry) || !Number.isFinite(now.getTime()) || now.getTime() >= expiry) {
    throw new Error("Website advisory exception expired; review upstream remediation");
  }
  const roots = new Set();
  function checkCauses(name, active = new Set()) {
    const entry = report.vulnerabilities[name];
    if (!entry || entry.name !== name || !Array.isArray(entry.via)
        || entry.via.length === 0 || active.has(name)) {
      throw new Error(`Unresolved npm audit dependency chain: ${name}`);
    }
    const next = new Set(active).add(name);
    for (const cause of entry.via) {
      if (typeof cause === "string") {
        checkCauses(cause, next);
      } else if (cause?.url === exception.advisory
          && cause.name === exception.package
          && cause.dependency === exception.package
          && name === exception.package) {
        roots.add(cause.url);
      } else {
        throw new Error(`Unexcepted npm advisory in ${name}: ${cause?.url ?? "unknown"}`);
      }
    }
  }
  for (const [name] of entries) checkCauses(name);
  if (roots.size !== 1
      || JSON.stringify(Object.keys(exception.sha256 ?? {}).sort()) !== JSON.stringify(reviewedPaths)) {
    throw new Error("Website advisory exception is missing its reviewed scope");
  }
  for (const [path, expected] of Object.entries(exception.sha256)) {
    if (path.startsWith("/") || path.split("/").includes("..")
        || !/^[a-f0-9]{64}$/.test(expected)) {
      throw new Error("Invalid website advisory source fingerprint");
    }
    const actual = createHash("sha256").update(readPinnedFile(path)).digest("hex");
    if (actual !== expected) {
      throw new Error(`Website advisory scope changed: ${path}; review before renewing`);
    }
  }
  return {
    status: "reviewed_exception",
    advisory: exception.advisory,
    expires: exception.expires,
    findings: entries.length,
    dependency_vulnerability_fixed: false,
  };
}

function main() {
  const audit = spawnSync("npm", ["audit", "--audit-level=low", "--json"], {
    cwd: resolve(root, "website-src"), encoding: "utf8", timeout: 60_000,
    maxBuffer: 8 * 1024 * 1024,
  });
  if (audit.error || ![0, 1].includes(audit.status)) {
    throw new Error(`npm audit failed: ${audit.error?.message ?? audit.stderr}`);
  }
  const report = JSON.parse(audit.stdout);
  const exception = JSON.parse(readFileSync(resolve(root, "website-src/dependency-audit-exception.json")));
  const result = evaluateAudit(report, exception);
  if ((result.findings === 0) !== (audit.status === 0)) {
    throw new Error("npm audit exit status does not match its report");
  }
  console.log(JSON.stringify(result, null, 2));
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { main(); } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
