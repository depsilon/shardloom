import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  assertNoDuplicateSuffixedArtifacts,
  duplicateSettleOptions,
  settleDuplicateSuffixedArtifacts,
} from "./static-artifact-hygiene.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const out = path.resolve(root, "..", "website");
const publicRoot = path.resolve(root, "..", "website-public");
const resetOutput = process.argv.includes("--reset-output");
const settleOutput = process.argv.includes("--settle-output");

if (resetOutput) {
  fs.rmSync(out, { recursive: true, force: true });
  fs.mkdirSync(out, { recursive: true });
}

const settleOptions = settleOutput
  ? duplicateSettleOptions()
  : {
      passes: 3,
      delayMs: 50,
    };
const removed = await settleDuplicateSuffixedArtifacts([out, publicRoot], settleOptions);
assertNoDuplicateSuffixedArtifacts([out, publicRoot]);

console.log(
  [
    "prepared website output directories",
    `reset_output=${resetOutput}`,
    `settle_output=${settleOutput}`,
    `duplicate_suffixed_removed=${removed.length}`,
    `settle_passes=${settleOptions.passes}`,
    `settle_delay_ms=${settleOptions.delayMs}`,
  ].join("; "),
);
