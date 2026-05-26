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

function syncSourceOfTruthData() {
  const canonicalFlow = fs.readFileSync(
    path.join(repoRoot, "docs", "architecture", "compute-engine-flow-reference.md"),
    "utf8",
  );
  write(path.join(publicDataRoot, "compute-engine-flow-reference.md"), canonicalFlow);

  const runsTodayMatrix = fs.readFileSync(
    path.join(repoRoot, "docs", "status", "runs-today-support-matrix.json"),
    "utf8",
  );
  write(path.join(dataRoot, "runs-today-support-matrix.json"), runsTodayMatrix);
  write(path.join(publicDataRoot, "runs-today-support-matrix.json"), runsTodayMatrix);

  const useCaseYaml = fs.readFileSync(
    path.join(repoRoot, "docs", "use-cases", "use-case-index.yml"),
    "utf8",
  );
  const useCaseIndex = parseYaml(useCaseYaml);
  const useCaseJson = JSON.stringify(useCaseIndex, null, 2) + "\n";
  write(path.join(dataRoot, "use-case-index.json"), useCaseJson);
  write(path.join(publicDataRoot, "use-case-index.json"), useCaseJson);

  const benchmarkEvidence = fs.readFileSync(
    path.join(publicBenchmarkRoot, "benchmark-results.json"),
    "utf8",
  );
  write(path.join(dataRoot, "benchmark-evidence.json"), benchmarkEvidence);
  write(path.join(publicDataRoot, "benchmark-evidence.json"), benchmarkEvidence);

  const benchmarkManifest = fs.readFileSync(
    path.join(publicBenchmarkRoot, "manifest.json"),
    "utf8",
  );
  write(path.join(dataRoot, "benchmark-manifest.json"), benchmarkManifest);
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
