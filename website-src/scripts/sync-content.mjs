import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parse as parseYaml } from "yaml";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = path.resolve(root, "..");
const dataRoot = path.join(root, "src", "data");
const docsRoot = path.join(root, "src", "content", "docs");
const docsUseCaseGeneratedRoot = path.join(repoRoot, "docs", "use-cases", "generated");
const legacyWebsiteDataRoot = path.join(repoRoot, "website", "assets", "data");
const publicDataRoot = path.join(repoRoot, "website-public", "assets", "data");
// Source preparation versions can be ahead of the proof-backed public release.
const publication = JSON.parse(fs.readFileSync(path.join(repoRoot, "docs/release/package-channel-readiness-matrix.json"), "utf8"));
const publishedTag = publication.selected_v0_1_0_release_tag;
if (!publishedTag || publication.selected_v0_1_0_publication_status !== `published_and_verified_${publishedTag}`) {
  throw new Error("website installation guidance requires a verified selected package release");
}
const packageVersion = publishedTag.slice(1);

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
  { slug: "execution-model", content: docsPage({
  "title": "Execution model",
  "description": "How ShardLoom avoids data work and preserves its no-fallback contract.",
  "order": 3,
  "body": "ShardLoom computes over Vortex-native, encoded columnar data. It is a local technical preview\nwith Python, SQL, DataFrame-style, and CLI entry points into the same admitted execution families.\n\n## Avoid work first\n\nThe engine tries to answer from metadata, prune irrelevant segments, and compute against encoded\nvalues before decoding or materializing rows. A dictionary or repeated value can let an operator\nwork on fewer values while preserving exact results.\n\nZero-decode means an operation preserves encoded values. Zero-copy means a boundary avoids copying\nbuffers. They describe different things, and neither applies to every operation. Execution evidence\nrecords where reading, decoding, and materialization occur.\n\n## No fallback\n\nUnsupported plans return explicit diagnostics. ShardLoom does not delegate them to Spark,\nDataFusion, DuckDB, Polars, or another query engine.\n\nApproved upstream Vortex array, compute, scan, source, and sink APIs can supply native operations\ninside ShardLoom's feature and policy boundaries. Vortex integrations with external query engines\nare not execution providers.\n\n## Inputs and outputs\n\nCompatibility inputs such as CSV or Parquet enter through explicit adapters. Vortex is the native\nmiddle and the highest-fidelity persistence target. Compatibility outputs translate results and\nreport metadata loss; choosing an output format does not choose another execution engine.\n\nSee [execution routes](/field-guide/execution-routes) for preparation and reuse, and\n[runtime and I/O](/field-guide/runtime-and-io) for supported formats and limits.\n\n## Execution evidence\n\nAdmitted execution reports `fallback_attempted=false` and `external_engine_invoked=false`.\nCertificates describe the admitted source, work, output, and materialization boundaries. Where\nincluded, sink digests and replay checks support result verification.\n\n`claim_gate_status` describes the scope of the evidence. A local smoke test, a capability report,\nand a reproducible benchmark answer different questions. A certificate alone does not establish\nproduction readiness or performance superiority.\n\nThe [compute flow](/compute-engine-flow) shows the route in detail. Read\n[benchmark methodology](/field-guide/benchmark-methodology) before comparing timings."
}) },
  { slug: "execution-routes", content: docsPage({
  "title": "Execution routes",
  "description": "Source admission, prepare-once reuse, and native Vortex routes.",
  "order": 4,
  "body": "Local files, prepared artifacts, and native Vortex files enter the same engine at different\npoints. The distinction matters for reuse and for what a timing measurement includes.\n\n## Source admission\n\n`UniversalIngress` identifies the input family and checks whether an adapter is available.\n`SourceState` records the source identity, schema, and adapter evidence. Unsupported formats or\noperations return a diagnostic before execution; a recognized file extension is not a promise of\nfull format support.\n\n## Prepare once\n\nFor an admitted compatibility input, `vortex_ingest` creates a `VortexPreparedState`.\nThe `prepared_vortex` route executes against that artifact.\n\nPreparation can be reused when the source, schema, and artifact identities match. Generation\nchecks span reuse and execution, and detected changes fail explicitly. Each query has fresh\nexecution state: prepared state is not a query-result cache.\n\n## Native Vortex\n\nThe `native_vortex` route starts with data already stored as Vortex. Preparation preserves its\nexisting layout. Operations still depend on supported types, encodings, and physical layouts;\nnative input does not eliminate computation or result delivery.\n\n## Generated rows\n\nA generated-source route creates deterministic rows without reading an input dataset. Its evidence\nmust distinguish row generation from source parsing and from query execution.\n\n## Cold and warm runs\n\nA certified cold route can include source reading, parsing, staging, Vortex construction,\nwrite/reopen checks, query execution, and output evidence. A prepared warm run begins after the\nprepared artifact exists. Compare only runs with the same measured boundary.\n\nThe [benchmark methodology](/field-guide/benchmark-methodology) explains timing surfaces.\nThe [Python guide](/field-guide/python-surface) shows the normal local query path.\nFor the full route vocabulary, see the\n[repository contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/universal-ingress-route-taxonomy.md)."
}) },

  {
    slug: "start-local-proof",
    content: docsPage({
      title: "Install and run",
      description: "Install the published technical preview and inspect native execution evidence.",
      order: 1,
      body: `ShardLoom ${packageVersion} is a published technical preview. Install from PyPI or Homebrew,
then follow the [local query walkthrough](/start). GitHub pre-release and TestPyPI artifacts are
also available; see [package installation](https://github.com/depsilon/shardloom/blob/main/docs/getting-started/package-user-install.md)
for supported platforms and channel verification.

## Install

\`\`\`sh
python -m pip install shardloom
# Or install the CLI with Homebrew
brew install depsilon/tap/shardloom
\`\`\`

## Develop from source

For development, follow [source checkout installation](https://github.com/depsilon/shardloom/blob/main/docs/getting-started/source-checkout-install.md).

## Verify execution

- \`fallback_attempted=false\`
- \`external_engine_invoked=false\`
- a visible \`claim_gate_status\`
- local output evidence or a deterministic blocker

## Boundary

Package publication and source execution have separate evidence. Check the
[supported surface](https://github.com/depsilon/shardloom/blob/main/docs/getting-started/v1-supported-unsupported.md)
and [troubleshooting guide](https://github.com/depsilon/shardloom/blob/main/docs/getting-started/troubleshooting-support.md).
Neither establishes production readiness, broad SQL/DataFrame parity, object-store runtime, or
performance superiority.`,
    }),
  },
  {
    slug: "python-surface",
    content: docsPage({
      title: "Python",
      description: "Current Python ETL scenario shape for the primary ShardLoom route.",
      order: 2,
      body: `Use Python to read local data, build queries, and inspect or export results. Admitted queries
run in ShardLoom. Unsupported work returns a diagnostic without fallback execution.

## Run a local query

Create the small CSV in the [getting-started walkthrough](/start) before running this example.
The public \`run()\` report exposes a shared result envelope across admitted routes.

\`\`\`python
import shardloom as sl

ctx = sl.context()
result = (
    ctx.read("data/orders.csv")
       .filter(sl.col("status") == "paid")
       .limit(10)
       .run()
)

print(result.envelope.field_int("output_row_count"))
print(result.envelope.human_text)
print(result.fallback_attempted, result.external_engine_invoked)
\`\`\`

\`ctx.read(path)\` infers local adapters for \`.csv\`, \`.json\`, \`.jsonl\`, \`.ndjson\`, \`.parquet\`,
\`.arrow\`, \`.ipc\`, \`.feather\`, \`.avro\`, \`.orc\`, and \`.vortex\`. CSV, flat JSON/JSONL/NDJSON,
generated rows, and scoped local Vortex inputs are the default public examples. Parquet, Arrow
IPC/Feather, Avro, and ORC are scoped local-format surfaces when the matching feature-gated build is
present; otherwise ShardLoom returns deterministic adapter blockers without fallback execution.

## Reuse And Export

Python contexts can retain a local worker to avoid per-call CLI startup. Automatic preparation
reuses an unchanged source only when its schema and artifact identities match. Each query still
has fresh execution state; a prepared artifact is not a cache of query results.

Admitted flat results can use \`write_vortex\`, \`write_parquet\`, \`write_arrow_ipc\`,
\`write_avro\`, \`write_orc\`, \`write_csv\`, \`write_json\`, and \`write_jsonl\` through the
shared native result and sink contracts. Feature gates, supported types, and result limits apply.
See [runtime and I/O](/field-guide/runtime-and-io) and the
[user-surface index](https://github.com/depsilon/shardloom/blob/main/docs/reference/shardloom-user-surface-index.md).

## Next steps

See [the examples](https://github.com/depsilon/shardloom/blob/main/docs/getting-started/examples.md)
for preparation, bounded collection, writes, and blocker inspection. The [limitations](/field-guide/limitations)
page describes current coverage gaps.`,
    }),
  },
  {
    slug: "runtime-and-io",
    content: docsPage({
      title: "Runtime and I/O",
      description: "Shipped native execution, preparation reuse, format support, and result limits.",
      order: 3,
      body: `Python, SQL, DataFrame-style calls, and the CLI lower admitted work into the same
ShardLoom-native and Vortex-native execution families. Compatibility formats are adapters and
writers around that middle; they do not select a different query engine.

## Native Execution

Supported routes use exact metadata, segment pruning, encoded reductions, weighted dictionary
aggregation, exact DISTINCT, and late payload gathering. Sparse dictionary selections decode only
referenced values. Coverage depends on the operation, type, and physical layout.

Admitted text grouping retains input-backed dictionary strings, uses compact exact-count state,
and shares partial construction between serial and bounded workers. Transformed text grouping
can overlap bounded dictionary preparation with native input progress and ordered consumption.
See the [implementation and measured tradeoffs](https://github.com/depsilon/shardloom/blob/main/docs/architecture/dictionary-preparation-screen-2026-09-30.md).

Filters, projections, aggregates, sort/Top-K, and selected provider-backed join workflows are
available. General joins, set operations, analytic windows, and subqueries still have native
coverage gaps. See the [front-door contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/v1-front-door-runtime-scope.md)
and [remaining family inventory](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-runtime-completion-2026-09-20.md#finite-availability-inventory).

## Local Formats

| Format | Input | Output |
| --- | --- | --- |
| Vortex | Native files; admitted local manifests and partitions | Highest-fidelity native persistence |
| Parquet | Feature-gated local reader | Feature-gated typed writer |
| Arrow IPC / Feather | Feature-gated local reader | Feature-gated Arrow IPC writer |
| Avro | Feature-gated local reader | Feature-gated writer; type restrictions apply |
| ORC | Feature-gated local reader | Feature-gated writer; nested and unrepresentable unsigned values remain unsupported |
| CSV | Local inference or declared schema; quoted multiline records | Text output with explicit materialization |
| JSON | Local JSON input with declared ingestion semantics | One top-level JSON array |
| JSONL / NDJSON | Local line-delimited JSON | JSONL text output |

Method availability does not imply every operator can feed every sink. Consult the
[input surface](https://github.com/depsilon/shardloom/blob/main/docs/reference/shardloom-user-surface-index.md)
and [output contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/v1-local-output-sink-scope.md)
for enabled features, schema support, and write policy. Compatibility outputs expose metadata
loss; text formats do not preserve Vortex layouts or static types.

## Preparation And Result Handoffs

Automatic compatibility-input preparation can reuse a local Vortex artifact when the source,
declared schema, and artifact identities match. Generation checks span reuse and execution;
detected changes fail explicitly. This is local preparation reuse, not a global query-result cache.

Native Vortex preparation preserves the existing layout. The shared all-I/O physical-layout
optimization policy remains follow-up work. Reading Vortex does not make parsing, computation,
encoding, or result delivery instantaneous.

Supported owned results retain Vortex arrays, validity, and memory credits. Complete flat aggregate
and sorted results reach the shared writers without rerunning the query or reparsing serialized
JSON for binary export. General computed results still use bounded scalar-to-native construction:
**65,536 rows, 128 fields, and 8 MiB**, subject to type and memory admission. This is not unbounded
streaming or every-operator/every-sink parity. See the
[I/O integration evidence](https://github.com/depsilon/shardloom/blob/main/docs/architecture/public-io-route-repair-2026-09-27.md).

## Resources And Recovery

Prepared sessions retain source handles and supported lowering while calls create fresh execution
state. Resident serving can bound concurrent calls, CPU grants, and positional I/O, with an
explicit reserved metadata lane. This does not establish production-scale fairness or an RSS ceiling.
Native file operations drain admitted I/O and reader ownership before completion. Metadata-only
aggregates avoid payload and worker admission, including when no spare payload credit is available.

COUNT/DISTINCT and selected numeric sort spill have specific admission, recovery, cancellation,
and cleanup contracts. Broad compound-key spill and spill-backed export remain incomplete. See
[COUNT/DISTINCT contracts](https://github.com/depsilon/shardloom/blob/main/docs/reference/resident-native-results.md),
[numeric sort spill](https://github.com/depsilon/shardloom/blob/main/docs/reference/native-query-spill.md),
and [serving evidence](https://github.com/depsilon/shardloom/blob/main/docs/architecture/concurrent-native-serving-2026-09-20.md).

Every admitted execution preserves \`fallback_attempted=false\` and
\`external_engine_invoked=false\`. Unsupported work returns deterministic diagnostics.`,
    }),
  },
  {
    slug: "benchmark-methodology",
    content: docsPage({
      title: "Benchmark methodology",
      description: "How to read hot runtime, publication proof, claim gates, and baseline rows.",
      order: 3,
      body: `The benchmark page is a ClickBench handoff, not a ShardLoom-hosted leaderboard.
Local benchmark and UAT artifacts remain repository evidence for engineering validation, route
coverage, and claim gating.

## Timing Surfaces

- \`hot_runtime\`: the default ShardLoom route grid for runtime timing.
- \`full_replay_proof\`: machine replay proof when present.
- \`publication_proof\`: result-sink, replay, and human evidence rendering when included by the row formula.
- \`external_baseline\`: comparison context only, never fallback execution.

## External baselines

External engines are comparison context only. Their execution never satisfies a ShardLoom
execution contract or its no-fallback evidence. The [benchmarks page](/benchmarks) links to ClickBench.

## Claim Rules

Do not compare rows without naming the timing surface, evidence tier, and claim gate. If
\`performance_claim_allowed=false\`, local artifacts may support engineering decisions but must not
claim superiority.`,
    }),
  },
  {
    slug: "limitations",
    content: docsPage({
      title: "Limitations",
      description: "Current public claim boundaries and unsupported behavior.",
      order: 4,
      body: `ShardLoom ${packageVersion} is available as a local technical preview through GitHub
pre-release, PyPI, TestPyPI, and Homebrew. See the
[publication record](https://github.com/depsilon/shardloom/blob/main/docs/release/v1-local-source-package-release.md)
and [current support matrix](https://github.com/depsilon/shardloom/blob/main/docs/release/public-status-matrix.md).
Package availability is separate from production readiness.

## Runtime Limits

- General native joins, set operations, windows, and subqueries have remaining coverage gaps.
- General computed-result exports are bounded to 65,536 rows, 128 fields, and 8 MiB; format and type restrictions apply.
- Existing Vortex inputs preserve their layout during preparation; all-I/O physical-layout optimization remains unfinished.
- Spill, memory reservations, and concurrent serving are scoped contracts, not universal spill support, an RSS ceiling, or production fairness proof.
- Remote output, table/catalog writes, and lakehouse transactions are outside the local sink contract.

See [runtime and I/O](/field-guide/runtime-and-io) for the shipped boundaries and linked evidence.

## External systems

Remote output, table transactions, and production object-store or Foundry runtime are outside
the local contract. Local fixtures, metadata inspection, and explicitly admitted local table-file
reads do not establish live provider support. Network, credential, extension, UDF, API, and model
effects require explicit admission; discovery does not execute them.

## Not claimed

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

function fieldGuideIndex() {
  return docsPage({
    title: "Field Guide",
    description: "Install ShardLoom, run local queries, and understand Vortex-native execution.",
    order: 0,
    body: "A practical guide to local, Vortex-native compute. Start with a query, then follow the\nparts of the engine that matter to your workload.\n\n## Get started\n\n- [Install and run](/field-guide/start-local-proof) — install the technical preview and verify a local query.\n- [Python](/field-guide/python-surface) — read, filter, inspect results, and choose an output.\n\n## Understand the engine\n\n- [Execution model](/field-guide/execution-model) — encoded work, native Vortex, and the no-fallback contract.\n- [Execution routes](/field-guide/execution-routes) — source admission, preparation, and reuse.\n- [Runtime and I/O](/field-guide/runtime-and-io) — operators, formats, result limits, and resource boundaries.\n\n## Read the evidence\n\n- [Benchmark methodology](/field-guide/benchmark-methodology) — what a timing includes and how to compare it.\n- [Limitations](/field-guide/limitations) — current coverage gaps and failure behavior.\n\nFor the architecture diagram, open the [compute flow](/compute-engine-flow).\nThe [repository](https://github.com/depsilon/shardloom) holds detailed API references and implementation evidence.",
  });
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
write(path.join(fieldGuideRoot, "index.mdx"), fieldGuideIndex());

// Retired vocabulary URLs resolve to useful sections, without adding pages to search.
const redirectsPath = path.join(repoRoot, "website-public", "_redirects");
const redirectMarker = "# Consolidated Field Guide links";
const authoredRedirects = fs.readFileSync(redirectsPath, "utf8").split(redirectMarker)[0].trimEnd();
const retiredRedirects = fieldGuide.flatMap((term) => {
  if (!term.redirect?.startsWith("/field-guide/")) throw new Error(`missing redirect for ${term.slug}`);
  return ["", "/"].map((suffix) => `/field-guide/${term.slug}${suffix} ${term.redirect} 301`);
});
write(redirectsPath, `${authoredRedirects}\n\n${redirectMarker}\n${retiredRedirects.join("\n")}\n`);

for (const page of durableDocsPages) {
  const fileName = `${page.slug}.mdx`;
  expectedFieldGuideFiles.add(fileName);
  write(path.join(fieldGuideRoot, fileName), page.content);
}
pruneGenerated(fieldGuideRoot, expectedFieldGuideFiles);

console.log(`synced ${durableDocsPages.length + 1} guide pages, ${fieldGuide.length} vocabulary redirects, and ${(useCaseIndex.use_cases ?? []).length} repository use-case records`);
