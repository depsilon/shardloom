import fs from "node:fs";
import crypto from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parse as parseYaml } from "yaml";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = path.resolve(root, "..");
const dataRoot = path.join(root, "src", "data");
const docsRoot = path.join(root, "src", "content", "docs");
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

function canonicalizeDeployableBenchmarkPaths(directory) {
  if (!fs.existsSync(directory)) return;
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const child = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      canonicalizeDeployableBenchmarkPaths(child);
      continue;
    }
    if (entry.name !== "benchmark-row-admission-manifest.json") continue;
    const payload = JSON.parse(fs.readFileSync(child, "utf8"));
    let changed = false;
    if (Array.isArray(payload.chunks)) {
      payload.chunks = payload.chunks.map((chunk) => {
        if (!chunk || typeof chunk.path !== "string") return chunk;
        const nextPath = chunk.path.replace(
          /^website-public\/assets\/benchmarks\/latest\//,
          "website/assets/benchmarks/latest/",
        );
        if (nextPath !== chunk.path) changed = true;
        return { ...chunk, path: nextPath };
      });
    }
    if (changed) {
      fs.writeFileSync(child, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
    }
  }
}

function publicationProofSourceDigest(chunks) {
  const digest = crypto.createHash("sha256");
  for (const chunk of [...(Array.isArray(chunks) ? chunks : [])].sort((left, right) =>
    String(left?.path ?? "").localeCompare(String(right?.path ?? "")),
  )) {
    digest.update(String(chunk?.path ?? ""));
    digest.update("\0");
    digest.update(String(chunk?.row_count ?? ""));
    digest.update("\0");
    digest.update(String(chunk?.sha256 ?? ""));
    digest.update("\0");
    digest.update(String(chunk?.uncompressed_sha256 ?? ""));
    digest.update("\0");
  }
  return `sha256:${digest.digest("hex")}`;
}

function syncPublicationProofSidecarDigest(benchmarkRoot) {
  const benchmarkResultsPath = path.join(benchmarkRoot, "benchmark-results.json");
  const sidecarPath = path.join(benchmarkRoot, "publication-proof-sidecar.json");
  if (!fs.existsSync(benchmarkResultsPath) || !fs.existsSync(sidecarPath)) return;

  const benchmarkResults = JSON.parse(fs.readFileSync(benchmarkResultsPath, "utf8"));
  const chunks = benchmarkResults.published_benchmark_row_chunks;
  if (!Array.isArray(chunks)) return;

  const sidecar = JSON.parse(fs.readFileSync(sidecarPath, "utf8"));
  const expectedDigest = publicationProofSourceDigest(chunks);
  const expectedCount = chunks.length;
  if (
    sidecar.source_row_chunks_digest !== expectedDigest ||
    sidecar.source_row_chunk_count !== expectedCount
  ) {
    sidecar.source_row_chunks_digest = expectedDigest;
    sidecar.source_row_chunk_count = expectedCount;
    fs.writeFileSync(sidecarPath, `${JSON.stringify(sidecar, null, 2)}\n`, "utf8");
  }
}

function syncBenchmarkRowChunks() {
  fs.mkdirSync(legacyWebsiteBenchmarkRoot, { recursive: true });
  for (const entry of fs.readdirSync(legacyWebsiteBenchmarkRoot, { withFileTypes: true })) {
    if (entry.isFile() && /^published-benchmark-rows-\d+\.json(?:\.gz)?$/.test(entry.name)) {
      fs.rmSync(path.join(legacyWebsiteBenchmarkRoot, entry.name), { force: true });
    }
  }
  for (const entry of fs.readdirSync(publicBenchmarkRoot, { withFileTypes: true })) {
    if (entry.isFile() && /^published-benchmark-rows-\d+\.json(?:\.gz)?$/.test(entry.name)) {
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
    canonicalizeDeployableBenchmarkPaths(legacyRunDirectory);
  } else if (fs.existsSync(legacyRunDirectory)) {
    fs.rmSync(legacyRunDirectory, { recursive: true, force: true });
  }
  syncPublicationProofSidecarDigest(publicBenchmarkRoot);
  syncPublicationProofSidecarDigest(legacyWebsiteBenchmarkRoot);
}

function syncSourceOfTruthData() {
  const canonicalFlow = fs.readFileSync(
    path.join(repoRoot, "docs", "architecture", "compute-engine-flow-reference.md"),
    "utf8",
  );
  write(path.join(legacyWebsiteDataRoot, "compute-engine-flow-reference.md"), canonicalFlow);
  write(path.join(publicDataRoot, "compute-engine-flow-reference.md"), canonicalFlow);

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
  .map((term) => `- [${term.title}](https://shardloom.io/field-guide/${term.slug}) (\`${term.category}\` / \`${term.status}\`)`)
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

${(term.related_use_cases ?? [])
  .map((id) => `- [${id}](https://github.com/depsilon/shardloom/blob/main/docs/use-cases/generated/${id}.md)`)
  .join("\n") || "- No related use case yet."}

## Reference Files

${referenceList(term.references)}
`;
}

function docsPage({ title, description, order, body }) {
  return `${frontmatter({
    title,
    description,
    sidebar: { label: title, order },
  })}

${body}
`;
}

const durableDocsPages = [
  {
    slug: "start-local-proof",
    content: docsPage({
      title: "Start local proof",
      description: "Run ShardLoom from a source checkout and inspect no-fallback evidence.",
      order: 1,
      body: `ShardLoom is pre-release. Start from a source checkout, not a package-publication claim.

Canonical install and support pages:

- \`docs/getting-started/source-checkout-install.md\`
- \`docs/getting-started/package-user-install.md\`
- \`docs/getting-started/v1-supported-unsupported.md\`
- \`docs/getting-started/troubleshooting-support.md\`

## First Commands

\`\`\`powershell
python scripts\\release_dry_run_proof.py --rows 64 --iterations 1
python scripts\\check_production_usability_gate.py
python examples\\local-python-smoke\\run.py --repo-root .
\`\`\`

## Success Evidence

- \`fallback_attempted=false\`
- \`external_engine_invoked=false\`
- a visible \`claim_gate_status\`
- local output evidence or a deterministic blocker

## Boundary

This proves local technical-preview posture only. It does not prove package publication, production readiness, broad SQL/DataFrame parity, object-store runtime, or performance superiority.`,
    }),
  },
  {
    slug: "python-surface",
    content: docsPage({
      title: "Python surface",
      description: "Current Python ETL scenario shape for the primary ShardLoom route.",
      order: 2,
      body: `The Python surface is the current user-facing way to describe local ETL scenarios. It is a front door into ShardLoom route admission, not permission to use pandas, Polars, DuckDB, Spark, or DataFusion as fallback execution.

## Scenario Shape

Markers for the copyable v1 guide examples: \`stable_v1_example_local_csv\`,
\`stable_v1_example_blocker_inspection\`, and \`unsupported_example_broad_sql\`.

\`\`\`python
from shardloom import context
import shardloom as sl

ctx = context(repo_root="/path/to/shardloom", profile_order=("release", "debug"))

fact = ctx.read_csv("data/fact.csv", schema={
    "id": "int64",
    "group_key": "int64",
    "dim_key": "int64",
    "value": "int64",
    "metric": "float64",
    "flag": "boolean",
    "category": "utf8",
    "event_date": "utf8",
    "nullable_metric_00": "float64",
    "raw_event_time": "utf8",
    "dirty_numeric": "utf8",
})
dim = ctx.read_csv("data/dim.csv", schema={
    "dim_key": "int64",
    "dim_label": "utf8",
    "weight": "float64",
})
events = ctx.read_json("data/events.jsonl", schema={
    "id": "int64",
    "nested_payload": "utf8",
})

fact.filter(sl.col("flag") == True).select("id", "group_key", "value").limit(1000).collect()
fact.filter(sl.col("metric") >= 0).group_by("group_key").agg(
    rows="count(*)",
    total_metric="sum(metric)",
).limit(100).collect()
fact.join(dim, on="dim_key", how="inner").select("f.id", "d.dim_label", "f.metric").limit(100).collect()
fact.select("id", "group_key", "metric").nlargest(10, "metric").collect()
events.filter(sl.col("nested_payload").contains("target")).select("id", "nested_payload").limit(100).collect()
\`\`\`

## Boundary

The primary route must emit ShardLoom evidence. Unsupported casts, effects, or data paths fail closed unless the current runtime admits them.`,
    }),
  },
  {
    slug: "benchmark-methodology",
    content: docsPage({
      title: "Benchmark methodology",
      description: "How to read hot runtime, publication proof, claim gates, and baseline rows.",
      order: 3,
      body: `The benchmark page renders a promoted artifact. It is evidence, not a leaderboard.

## Timing Surfaces

- \`hot_runtime\`: the default ShardLoom route grid for runtime timing.
- \`full_replay_proof\`: machine replay proof when present.
- \`publication_proof\`: result-sink, replay, and human evidence rendering when included by the row formula.
- \`external_baseline\`: comparison context only, never fallback execution.

## Claim Rules

Do not compare rows without naming the timing surface, evidence tier, and claim gate. If \`performance_claim_allowed=false\`, the page may show timing evidence but must not claim superiority.`,
    }),
  },
  {
    slug: "limitations",
    content: docsPage({
      title: "Limitations",
      description: "Current public claim boundaries and unsupported behavior.",
      order: 4,
      body: `ShardLoom is not public production infrastructure yet.

## Not Claimed

- package publication readiness
- production support
- broad SQL/DataFrame parity
- Spark displacement
- object-store or lakehouse production runtime
- Foundry production runtime
- performance superiority

## Failure Behavior

Unsupported work must produce a deterministic blocker or report-only posture. It must not execute through Spark, DataFusion, DuckDB, Polars, pandas, Velox, Trino, a database, a warehouse, or another fallback engine.`,
    }),
  },
];

function fieldGuideIndex(terms) {
  const categories = [...new Set(terms.map((term) => term.category))];
  return `${frontmatter({
    title: "Field Guide",
    description: "A concise Starlight-powered atlas for ShardLoom routes, evidence terms, and support boundaries.",
    sidebar: { label: "Field Guide" },
  })}

A compact Starlight docs shell for ShardLoom's current public surface. Start with local proof,
Python route shape, benchmark methodology, and limitations, then use the vocabulary atlas for
exact route and evidence terms.

## Category Table Of Contents

- [Start local proof](/field-guide/start-local-proof/)
- [Python surface](/field-guide/python-surface/)
- [Benchmark methodology](/field-guide/benchmark-methodology/)
- [Limitations](/field-guide/limitations/)
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

const fieldGuideRoot = path.join(docsRoot, "field-guide");
prepareGenerated(fieldGuideRoot);
const starlightDocsIndex = path.join(docsRoot, "docs.mdx");
if (fs.existsSync(starlightDocsIndex)) fs.rmSync(starlightDocsIndex);
const expectedFieldGuideFiles = new Set(["index.mdx"]);
write(path.join(fieldGuideRoot, "index.mdx"), fieldGuideIndex(fieldGuide));

for (const term of fieldGuide) {
  const fileName = `${term.slug}.mdx`;
  expectedFieldGuideFiles.add(fileName);
  write(path.join(fieldGuideRoot, fileName), termPage(term));
}

for (const page of durableDocsPages) {
  const fileName = `${page.slug}.mdx`;
  expectedFieldGuideFiles.add(fileName);
  write(path.join(fieldGuideRoot, fileName), page.content);
}
pruneGenerated(fieldGuideRoot, expectedFieldGuideFiles);

console.log(`synced ${fieldGuide.length} field-guide terms and ${(useCaseIndex.use_cases ?? []).length} repository use-case records`);
