import fs from "node:fs";
import path from "node:path";

const duplicateSuffixedArtifactPattern = / \d+(?:\.[^.]+)?$/;

function positiveIntegerFromEnv(name) {
  const value = process.env[name];
  if (value === undefined || value === "") return undefined;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer when set`);
  }
  return parsed;
}

export function duplicateSettleOptions(defaults = {}) {
  const macOSDefaults =
    process.platform === "darwin"
      ? {
          passes: 90,
          delayMs: 500,
        }
      : {
          passes: 6,
          delayMs: 100,
        };
  return {
    passes:
      positiveIntegerFromEnv("SHARDLOOM_WEBSITE_DUPLICATE_SETTLE_PASSES") ??
      defaults.passes ??
      macOSDefaults.passes,
    delayMs:
      positiveIntegerFromEnv("SHARDLOOM_WEBSITE_DUPLICATE_SETTLE_DELAY_MS") ??
      defaults.delayMs ??
      macOSDefaults.delayMs,
  };
}

export function collectDuplicateSuffixedArtifacts(directory, prefix = "") {
  if (!fs.existsSync(directory)) return [];
  const duplicates = [];
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const relativePath = path.join(prefix, entry.name).replace(/\\/g, "/");
    const absolutePath = path.join(directory, entry.name);
    if (duplicateSuffixedArtifactPattern.test(entry.name)) {
      duplicates.push(relativePath);
      continue;
    }
    if (entry.isDirectory()) {
      duplicates.push(...collectDuplicateSuffixedArtifacts(absolutePath, relativePath));
    }
  }
  return duplicates;
}

export function removeDuplicateSuffixedArtifacts(directory) {
  if (!fs.existsSync(directory)) return [];
  const removed = [];
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const child = path.join(directory, entry.name);
    if (duplicateSuffixedArtifactPattern.test(entry.name)) {
      fs.rmSync(child, { recursive: true, force: true });
      removed.push(path.relative(directory, child).replace(/\\/g, "/"));
      continue;
    }
    if (entry.isDirectory()) {
      removed.push(
        ...removeDuplicateSuffixedArtifacts(child).map((nested) =>
          path.join(entry.name, nested).replace(/\\/g, "/"),
        ),
      );
    }
  }
  return removed;
}

export async function settleDuplicateSuffixedArtifacts(directories, options = {}) {
  const passes = options.passes ?? 5;
  const delayMs = options.delayMs ?? 100;
  const removed = [];
  for (let pass = 0; pass < passes; pass += 1) {
    for (const directory of directories) {
      for (const artifact of removeDuplicateSuffixedArtifacts(directory)) {
        removed.push(`${path.basename(directory)}:${artifact}`);
      }
    }
    if (pass + 1 < passes) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
  return removed;
}

export function assertNoDuplicateSuffixedArtifacts(directories) {
  const duplicates = [];
  for (const directory of directories) {
    for (const artifact of collectDuplicateSuffixedArtifacts(directory)) {
      duplicates.push(`${path.basename(directory)}:${artifact}`);
    }
  }
  if (duplicates.length > 0) {
    throw new Error(
      `duplicate-suffixed generated website artifacts remain: ${duplicates.join(", ")}`,
    );
  }
}
