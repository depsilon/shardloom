import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parse as parseYaml } from "yaml";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = path.resolve(root, "..");
const dataRoot = path.join(root, "src", "data");
const docsRoot = path.join(root, "src", "content", "docs");
const useCaseRoot = path.join(root, "src", "content", "use-cases");
const statusRoot = path.join(root, "src", "content", "status");
const docsUseCaseGeneratedRoot = path.join(repoRoot, "docs", "use-cases", "generated");
const legacyWebsiteDataRoot = path.join(repoRoot, "website", "assets", "data");
const legacyWebsiteBenchmarkRoot = path.join(repoRoot, "website", "assets", "benchmarks", "latest");
const publicDataRoot = path.join(repoRoot, "website-public", "assets", "data");
const publicBenchmarkRoot = path.join(repoRoot, "website-public", "assets", "benchmarks", "latest");

function readJson(file) {
  return JSON.parse(fs.readFileSync(path.join(dataRoot, file), "utf8"));
}

function slug(value) {
  return String(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/(^-|-$)/g, "") || "item";
}

function write(file, content) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, content, "utf8");
}

function cleanGenerated(directory) {
  if (fs.existsSync(directory)) fs.rmSync(directory, { recursive: true, force: true });
  fs.mkdirSync(directory, { recursive: true });
}

function syncBenchmarkRowChunks() {
  fs.mkdirSync(legacyWebsiteBenchmarkRoot, { recursive: true });
  for (const entry of fs.readdirSync(legacyWebsiteBenchmarkRoot, { withFileTypes: true })) {
    if (entry.isFile() && /^published-benchmark-rows-\d+\.json$/.test(entry.name)) {
      fs.rmSync(path.join(legacyWebsiteBenchmarkRoot, entry.name), { force: true });
    }
  }
  for (const entry of fs.readdirSync(publicBenchmarkRoot, { withFileTypes: true })) {
    if (entry.isFile() && /^published-benchmark-rows-\d+\.json$/.test(entry.name)) {
      fs.copyFileSync(
        path.join(publicBenchmarkRoot, entry.name),
        path.join(legacyWebsiteBenchmarkRoot, entry.name),
      );
    }
  }
  const admissionManifest = "benchmark-row-admission-manifest.json";
  const publicAdmissionManifest = path.join(publicBenchmarkRoot, admissionManifest);
  const legacyAdmissionManifest = path.join(legacyWebsiteBenchmarkRoot, admissionManifest);
  if (fs.existsSync(publicAdmissionManifest)) {
    fs.copyFileSync(publicAdmissionManifest, legacyAdmissionManifest);
  } else if (fs.existsSync(legacyAdmissionManifest)) {
    fs.rmSync(legacyAdmissionManifest, { force: true });
  }
  const runDirectory = "published-row-runs";
  const publicRunDirectory = path.join(publicBenchmarkRoot, runDirectory);
  const legacyRunDirectory = path.join(legacyWebsiteBenchmarkRoot, runDirectory);
  if (fs.existsSync(publicRunDirectory)) {
    if (fs.existsSync(legacyRunDirectory)) {
      fs.rmSync(legacyRunDirectory, { recursive: true, force: true });
    }
    fs.cpSync(publicRunDirectory, legacyRunDirectory, { recursive: true, force: true });
  } else if (fs.existsSync(legacyRunDirectory)) {
    fs.rmSync(legacyRunDirectory, { recursive: true, force: true });
  }
}

function syncSourceOfTruthData() {
  const canonicalFlow = fs.readFileSync(
    path.join(repoRoot, "docs", "architecture", "compute-engine-flow-reference.md"),
    "utf8",
  );
  write(path.join(legacyWebsiteDataRoot, "compute-engine-flow-reference.md"), canonicalFlow);
  write(path.join(publicDataRoot, "compute-engine-flow-reference.md"), canonicalFlow);

  const runsTodayMatrix = fs.readFileSync(
    path.join(repoRoot, "docs", "status", "runs-today-support-matrix.json"),
    "utf8",
  );
  write(path.join(dataRoot, "runs-today-support-matrix.json"), runsTodayMatrix);
  write(path.join(legacyWebsiteDataRoot, "runs-today-support-matrix.json"), runsTodayMatrix);
  write(path.join(publicDataRoot, "runs-today-support-matrix.json"), runsTodayMatrix);

  const useCaseYaml = fs.readFileSync(
    path.join(repoRoot, "docs", "use-cases", "use-case-index.yml"),
    "utf8",
  );
  const useCaseIndex = parseYaml(useCaseYaml);
  const useCaseJson = JSON.stringify(useCaseIndex, null, 2) + "\n";
  write(path.join(dataRoot, "use-case-index.json"), useCaseJson);
  write(path.join(legacyWebsiteDataRoot, "use-case-index.json"), useCaseJson);
  write(path.join(publicDataRoot, "use-case-index.json"), useCaseJson);

  const benchmarkEvidence = fs.readFileSync(
    path.join(publicBenchmarkRoot, "benchmark-results.json"),
    "utf8",
  );
  write(path.join(dataRoot, "benchmark-evidence.json"), benchmarkEvidence);
  write(path.join(legacyWebsiteDataRoot, "benchmark-evidence.json"), benchmarkEvidence);
  write(path.join(legacyWebsiteBenchmarkRoot, "benchmark-results.json"), benchmarkEvidence);
  write(path.join(publicDataRoot, "benchmark-evidence.json"), benchmarkEvidence);

  const benchmarkManifest = fs.readFileSync(
    path.join(publicBenchmarkRoot, "manifest.json"),
    "utf8",
  );
  write(path.join(dataRoot, "benchmark-manifest.json"), benchmarkManifest);
  write(path.join(legacyWebsiteBenchmarkRoot, "manifest.json"), benchmarkManifest);
  syncBenchmarkRowChunks();
}

function yamlStringList(values) {
  return (Array.isArray(values) ? values : []).map((value) => `  - ${JSON.stringify(String(value))}`).join("\n");
}

function frontmatter(fields) {
  return [
    "---",
    ...Object.entries(fields).flatMap(([key, value]) => {
      if (Array.isArray(value)) {
        return [`${key}:`, yamlStringList(value)];
      }
      return [`${key}: ${JSON.stringify(value)}`];
    }),
    "---",
    "",
  ].join("\n");
}

const REFERENCE_PROOFS = {
  "README.md": "Public technical-preview posture, Vortex-first positioning, and no-fallback boundaries.",
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

function referenceList(references) {
  const rows = references ?? [];
  if (!rows.length) return `<ul data-citation-block="reference-files"><li>Reference not yet attached.</li></ul>`;
  return `<ul data-citation-block="reference-files">
${rows.map((ref) => `<li><code>${ref}</code> - What this proves: ${referenceProof(ref)}</li>`).join("\n")}
</ul>`;
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
  .map((term) => `- \`website/field-guide/${term.slug}.html\` - ${term.title} (\`${term.category}\` / \`${term.status}\`)`)
  .join("\n") || "- No related field-guide terms yet."}
`;
}

function termPage(term) {
  return `${frontmatter({
    title: term.title,
    description: term.summary,
    sidebar: { label: term.title },
  })}

<span class="status-chip status-${String(term.status).replaceAll("_", "-")}">${term.status}</span>

${term.summary}

## Plain-English Meaning

${term.summary}

## Why It Matters

This concept helps users understand how ShardLoom separates source admission, Vortex preparation, execution route selection, output planning, and claim-gated evidence.

## How ShardLoom Uses It

- Route: \`${term.route}\`
- Category: ${term.category}

## Current Support

Status: \`${term.status}\`

## Evidence Fields

${(term.evidence_fields ?? []).map((field) => `- \`${field}\``).join("\n") || "- Not reported for this term."}

## What It Does Not Claim

This Field Guide entry does not expand runtime support, performance claims, production readiness, object-store/lakehouse support, Foundry production support, package publication, broad SQL/DataFrame support, Spark-displacement claims, or fallback execution.

## Try It / Related Use Cases

${(term.related_use_cases ?? []).map((id) => `- [${id}](/use-cases/${id})`).join("\n") || "- No related use case yet."}

## Reference Files

${referenceList(term.references)}
`;
}

function fieldGuideIndex(terms) {
  const categories = [...new Set(terms.map((term) => term.category))];
  return `${frontmatter({
    title: "Field Guide",
    description: "A concise Starlight-powered atlas for ShardLoom routes, evidence terms, and support boundaries.",
    sidebar: { label: "Field Guide" },
  })}

A compact atlas for ShardLoom vocabulary. This Starlight-powered Field Guide explains the route and evidence vocabulary behind the public website, including UniversalIngress, vortex_ingest, VortexPreparedState, No fallback, and claim_gate_status.

## Category Table Of Contents

${categories.map((category) => `- [${category}](#${slug(category)})`).join("\n")}

${categories
  .map((category) => {
    const rows = terms
      .filter((term) => term.category === category)
      .map((term) => `- [${term.title}](/field-guide/${term.slug}/) - ${term.summary}`)
      .join("\n");
    return `## ${category}\n\n${rows}`;
  })
  .join("\n\n")}

## Claim Boundary

The Field Guide explains vocabulary. It does not create a runtime, performance, production, SQL/DataFrame, object-store, lakehouse, Foundry, package-publication, Spark-displacement, or fallback-execution claim.
`;
}

syncSourceOfTruthData();

const fieldGuide = readJson("field-guide.json");
const useCaseIndex = readJson("use-case-index.json");
const statusRows = readJson("status-rows.json");

cleanGenerated(docsUseCaseGeneratedRoot);
for (const useCase of useCaseIndex.use_cases ?? []) {
  write(path.join(docsUseCaseGeneratedRoot, `${useCase.id}.md`), docsUseCasePage(useCase, fieldGuide));
}

cleanGenerated(path.join(docsRoot, "field-guide"));
cleanGenerated(useCaseRoot);
cleanGenerated(statusRoot);
const starlightDocsIndex = path.join(docsRoot, "docs.mdx");
if (fs.existsSync(starlightDocsIndex)) fs.rmSync(starlightDocsIndex);
write(path.join(docsRoot, "field-guide", "index.mdx"), fieldGuideIndex(fieldGuide));

for (const term of fieldGuide) {
  write(path.join(docsRoot, "field-guide", `${term.slug}.mdx`), termPage(term));
}

for (const useCase of useCaseIndex.use_cases ?? []) {
  write(path.join(useCaseRoot, `${useCase.id}.json`), JSON.stringify(useCase, null, 2) + "\n");
}

for (const row of statusRows) {
  write(path.join(statusRoot, `${slug(row.capability)}.json`), JSON.stringify(row, null, 2) + "\n");
}

console.log(`synced ${fieldGuide.length} field-guide terms, ${(useCaseIndex.use_cases ?? []).length} use cases, and ${statusRows.length} status rows`);
