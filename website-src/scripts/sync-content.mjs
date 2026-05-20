import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const dataRoot = path.join(root, "src", "data");
const docsRoot = path.join(root, "src", "content", "docs");
const useCaseRoot = path.join(root, "src", "content", "use-cases");
const statusRoot = path.join(root, "src", "content", "status");

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

${(term.references ?? []).map((ref) => `- \`${ref}\``).join("\n") || "- Reference not yet attached."}
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

function docsIndex() {
  return `${frontmatter({
    title: "Docs",
    description: "ShardLoom documentation entry point with quick-start, architecture, benchmark, and release references.",
    sidebar: { label: "Docs" },
  })}

ShardLoom docs live in the repository and this Starlight surface keeps the public site aligned with the source-of-truth files.

## Start

- [First 10 minutes](https://github.com/depsilon/shardloom/blob/main/docs/getting-started/first-10-minutes.md)
- [Examples](https://github.com/depsilon/shardloom/blob/main/docs/getting-started/examples.md)
- [Certified local workload](https://github.com/depsilon/shardloom/blob/main/docs/getting-started/certified-local-workload.md)

## Architecture

- [Compute engine flow reference](https://github.com/depsilon/shardloom/blob/main/docs/architecture/compute-engine-flow-reference.md)
- [Universal input contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/universal-input-contract.md)
- [Phased execution plan](https://github.com/depsilon/shardloom/blob/main/docs/architecture/phased-execution-plan.md)

## Evidence

- [Local taxonomy benchmark](https://github.com/depsilon/shardloom/blob/main/docs/benchmarks/local-taxonomy-benchmark.md)
- [Baseline comparison boundary](https://github.com/depsilon/shardloom/blob/main/docs/benchmarks/baseline-comparison-boundary.md)
- [Foundry proof of use](https://github.com/depsilon/shardloom/blob/main/docs/foundry/proof-of-use-certification.md)

## Claim Boundary

These pages are documentation and evidence interpretation surfaces. They do not claim production readiness, package publication, Spark displacement, or external-engine fallback.
`;
}

const fieldGuide = readJson("field-guide.json");
const useCaseIndex = readJson("use-case-index.json");
const statusRows = readJson("status-rows.json");

cleanGenerated(path.join(docsRoot, "field-guide"));
cleanGenerated(useCaseRoot);
cleanGenerated(statusRoot);
write(path.join(docsRoot, "field-guide", "index.mdx"), fieldGuideIndex(fieldGuide));
write(path.join(docsRoot, "docs.mdx"), docsIndex());

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
