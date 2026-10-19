import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parse as parseYaml } from "yaml";
import { projectRelease, projectExamples } from "./public-guide-data.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = path.resolve(root, "..");
const dataRoot = path.join(root, "src", "data");
const docsUseCaseGeneratedRoot = path.join(repoRoot, "docs", "use-cases", "generated");
const legacyWebsiteDataRoot = path.join(repoRoot, "website", "assets", "data");
const publicDataRoot = path.join(repoRoot, "website-public", "assets", "data");
const packageMatrix = JSON.parse(fs.readFileSync(path.join(repoRoot, "docs/release/package-channel-readiness-matrix.json"), "utf8"));
const guideChannel = packageMatrix.channels.find((row) => row.channel_id === packageMatrix.user_guide.artifact_channel);
const release = projectRelease(packageMatrix, JSON.parse(fs.readFileSync(path.join(repoRoot, guideChannel.install_transcript_ref), "utf8")));
const examples = projectExamples(fs.readFileSync(path.join(repoRoot, "examples/paid-orders/quickstart.py"), "utf8"));
examples.developmentExample = fs.readFileSync(path.join(repoRoot, "examples/paid-orders/context-result.py"), "utf8").trim();
const { pythonExample, expectedRows } = examples;
write(path.join(dataRoot, "public-guide.json"), JSON.stringify({ release, examples }, null, 2) + "\n");
function readJson(file) {
  return JSON.parse(fs.readFileSync(path.join(dataRoot, file), "utf8"));
}

function write(file, content) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, content, "utf8");
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

function prepareGenerated(directory) {
  fs.mkdirSync(directory, { recursive: true });
  removeDuplicateSuffixedArtifacts(directory);
}

function pruneGenerated(directory, expectedNames) {
  if (!fs.existsSync(directory)) return;
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const child = path.join(directory, entry.name);
    if (/ \d+(?:\.[^.]+)?$/.test(entry.name) || !expectedNames.has(entry.name)) {
      fs.rmSync(child, { recursive: true, force: true });
    }
  }
}

function syncSourceOfTruthData() {
  const canonicalFlow = fs.readFileSync(
    path.join(repoRoot, "docs", "architecture", "compute-engine-flow-reference.md"),
    "utf8",
  );
  write(path.join(legacyWebsiteDataRoot, "compute-engine-flow-reference.md"), canonicalFlow);
  write(path.join(publicDataRoot, "compute-engine-flow-reference.md"), canonicalFlow);
}

const REFERENCE_PROOFS = {
  "README.md": "Published local engine, operational maturity, Vortex-first positioning, and no-fallback boundaries.",
  "python/README.md": "Python wrapper scope, local smoke usage, and Python API claim boundaries.",
  "docs/architecture/compute-engine-flow-reference.md":
    "Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.",
  "docs/architecture/effect-budget-plan.md":
    "Deny-by-default effect budget policy and the local fixture exceptions for the current effectful-operation slice.",
  "docs/architecture/effectful-operation-admission-matrix.md":
    "Effectful-operation admission rows for local SQLite, extension metadata, deterministic UDF fixture, and blocked external effects.",
  "docs/architecture/extension-manifest-effect-capability-matrix.md":
    "Extension manifest inspection posture and blockers for dynamic loading, plugin execution, and arbitrary UDF execution.",
  "docs/architecture/object-store-request-planner.md":
    "Object-store route admission, local-emulator evidence, and remote-provider blockers.",
  "docs/architecture/table-intelligence-layer.md":
    "Table maintenance execution posture and lakehouse/table claim boundaries.",
  "docs/architecture/phased-execution-completed-ledger.md":
    "Completed runtime provenance and historical phase evidence for this use case.",
  "docs/architecture/universal-compatibility-coverage-scoreboard.md":
    "Compatibility scoreboard status and source/sink support boundaries.",
  "docs/architecture/universal-input-contract.md":
    "Universal input contract posture and unsupported input-family diagnostics.",
  "docs/architecture/universal-ingress-route-taxonomy.md":
    "UniversalIngress, Vortex ingest, prepared-state, and route-timing contract boundaries.",
  "docs/status/cli-command-registry.md":
    "CLI registry status, public route facade command discovery, user-surface posture, and no-fallback metadata.",
  "docs/benchmarks/local-taxonomy-benchmark.md":
    "Local benchmark taxonomy, evidence rows, and workload-scoped interpretation boundaries.",
  "docs/benchmarks/baseline-comparison-boundary.md":
    "Benchmark comparison boundaries and external-baseline-only policy.",
};

function referenceProof(reference) {
  return REFERENCE_PROOFS[reference] ?? "This source anchors the page claim boundary, evidence fields, and support posture.";
}

function markdownList(values) {
  return (values ?? []).map((value) => `- \`${String(value)}\``).join("\n") || "- Not reported.";
}

function runnableBlock(command) {
  if (!command) return "No runnable example is published for this report-only or blocked path.";
  const info = String(command).includes("python -c") || String(command).includes("New-Item")
    ? "powershell"
    : "text";
  return `\`\`\`${info}\n${command}\n\`\`\``;
}

function canShardLoomDoThis(useCase) {
  if (useCase.status === "ready_local" || useCase.status === "smoke_supported") {
    return `${useCase.title} has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.`;
  }
  if (useCase.status === "report_only") {
    return `${useCase.title} is inspectable as posture or diagnostics, but it is not broad runtime support.`;
  }
  return `${useCase.title} is not admitted runtime support yet. Use the blocker and evidence requirements to understand what remains.`;
}

function docsUseCasePage(useCase, fieldGuideTerms) {
  const relatedTerms = fieldGuideTerms.filter((term) => (term.related_use_cases ?? []).includes(useCase.id));
  return `<!-- SPDX-License-Identifier: Apache-2.0 -->

# ${useCase.title}

## Quick Answer

- **Audience:** ${useCase.audience}
- **Status:** \`${useCase.status}\`
- **Execution mode:** \`${useCase.execution_mode}\`
- **Engine mode:** \`${useCase.engine_mode}\`
- **Claim boundary:** ${useCase.claim_boundary}

## Can ShardLoom Do This?

${canShardLoomDoThis(useCase)}

## Claim Boundary

${useCase.claim_boundary}

## How To Try It

${runnableBlock(useCase.runnable_example)}

## Blocker

${useCase.blocked_explanation ?? "No current blocker is attached to this supported local smoke path beyond the claim boundary above."}

## Internal Flow

\`${useCase.internal_flow ?? `${(useCase.inputs ?? []).join(", ")} -> ${useCase.execution_mode} -> ${useCase.engine_mode} -> ${(useCase.outputs ?? []).join(", ")} -> evidence -> claim gate`}\`

## Evidence You Should See

${markdownList(useCase.evidence_fields)}

## Expected Output Or Evidence

${useCase.expected_output_evidence}

## Common Mistakes

${markdownList(useCase.common_mistakes)}

## Reference Files

${(useCase.references ?? []).map((ref) => `- \`${ref}\` - What this proves: ${referenceProof(ref)}`).join("\n") || "- Reference not yet attached."}

## Related Use Cases

${markdownList(useCase.related_use_cases)}

## Related Field Guide Terms

${relatedTerms
  .map((term) => `- [${term.title}](https://shardloom.io/field-guide/${term.slug}) (\`${term.category}\` / \`${term.status}\`)`)
  .join("\n") || "- No related field-guide terms yet."}
`;
}

syncSourceOfTruthData();

// Only reusable facts and executable code are generated into authored READMEs.
const readmeBlocks = {
  RELEASE: [
    `Published **${release.version}**. [Install and platform requirements](https://shardloom.io/field-guide/start-local-proof).`,
    `\`\`\`sh\n${release.installCommand}\n\`\`\``,
    ...release.platforms.map((row) => `- ${row.platform}; ${row.python}; ${row.backends.join(" or ")}.`),
    'The example selects native execution. Use `transport="subprocess"` for the Windows CLI-backed package.',
  ].join("\n\n"),
  QUICKSTART: `\`\`\`python\n${pythonExample}\n\`\`\`\n\nExpected values:\n\n\`\`\`python\n${expectedRows}\n\`\`\``,
};
for (const relative of ["README.md", "python/README.md"]) {
  const target = path.join(repoRoot, relative);
  let content = fs.readFileSync(target, "utf8");
  for (const [name, block] of Object.entries(readmeBlocks)) {
    const start = `<!-- BEGIN SHARDLOOM ${name} -->`;
    const end = `<!-- END SHARDLOOM ${name} -->`;
    if (content.split(start).length !== 2 || content.split(end).length !== 2) {
      throw new Error(`Missing or repeated ${name} block in ${relative}`);
    }
    const before = content.slice(0, content.indexOf(start) + start.length);
    const after = content.slice(content.indexOf(end));
    content = `${before}\n${block}\n${after}`;
  }
  write(target, content);
}

const fieldGuide = readJson("field-guide.json");
const useCaseIndex = parseYaml(
  fs.readFileSync(path.join(repoRoot, "docs", "use-cases", "use-case-index.yml"), "utf8"),
);

prepareGenerated(docsUseCaseGeneratedRoot);
const expectedDocsUseCaseFiles = new Set();
for (const useCase of useCaseIndex.use_cases ?? []) {
  const fileName = `${useCase.id}.md`;
  expectedDocsUseCaseFiles.add(fileName);
  write(path.join(docsUseCaseGeneratedRoot, fileName), docsUseCasePage(useCase, fieldGuide));
}
pruneGenerated(docsUseCaseGeneratedRoot, expectedDocsUseCaseFiles);

// This retired Starlight source would create a public /docs page that conflicts
// with the clean-slate redirect to /field-guide. Remove only this known legacy file.
const legacyStarlightDocsIndex = path.join(root, "src", "content", "docs", "docs.mdx");
if (fs.existsSync(legacyStarlightDocsIndex)) fs.rmSync(legacyStarlightDocsIndex);

// Retired vocabulary URLs resolve to useful sections, without adding pages to search.
const redirectsPath = path.join(repoRoot, "website-public", "_redirects");
const redirectMarker = "# Consolidated Field Guide links";
const authoredRedirects = fs.readFileSync(redirectsPath, "utf8").split(redirectMarker)[0].trimEnd();
const retiredRedirects = fieldGuide.flatMap((term) => {
  if (!term.redirect?.startsWith("/field-guide/")) throw new Error(`missing redirect for ${term.slug}`);
  return ["", "/"].map((suffix) => `/field-guide/${term.slug}${suffix} ${term.redirect} 301`);
});
write(redirectsPath, `${authoredRedirects}\n\n${redirectMarker}\n${retiredRedirects.join("\n")}\n`);

console.log(`synced ${fieldGuide.length} vocabulary redirects and ${(useCaseIndex.use_cases ?? []).length} repository use-case records`);
