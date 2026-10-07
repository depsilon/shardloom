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
  body: `ShardLoom is built toward general-purpose data processing: read data, transform or query it,
and deliver the result. Python, SQL, DataFrame-style calls, and the CLI express work for one
Vortex-native execution pipeline. The published local engine has
[specific coverage limits](/field-guide/limitations).

## One native pipeline

**Input adapter → Vortex-native data → ShardLoom execution → output adapter**

Choose the data source, query, and destination. Input adapters handle format differences;
the shared engine plans and executes the work. Data already in Vortex preserves its native
representation. Preparation and reuse are stages in the data lifecycle.

Metadata-first planning, pruning, encoded execution, resource control, and late materialization
belong inside this pipeline. The engine applies each mechanism where the operation and data
permit it. There is no separate fast-mode workflow to select.

## Avoid work first

The engine tries to answer from metadata, prune irrelevant segments, and compute against encoded
values before decoding or materializing rows. A dictionary or repeated value can let an operator
work on fewer values while preserving exact results.

Zero-decode means an operation preserves encoded values. Zero-copy means a boundary avoids copying
buffers. They describe different things, and neither applies to every operation. Execution evidence
records where reading, decoding, and materialization occur.

## Broader workloads, the same engine

The architecture supports extending formats, data types, operators, and connectors around the same
native middle. Completing that support means making whole read → transform → write workflows
compose and handling larger data with bounded streaming, shared resource accounting, and native
spill. These are active engineering requirements, with remaining work tracked in the
[breadth and scale plan](https://github.com/depsilon/shardloom/blob/main/docs/architecture/universal-workflow-completion-2026-10-01.md).

ClickBench is one regression and comparison workload. Its queries and schema do not define the
product's intended scope. Shared optimizations must also work with other schemas and compositions.

## No fallback

Unsupported plans return explicit diagnostics. ShardLoom does not delegate them to Spark,
DataFusion, DuckDB, Polars, or another query engine.

Approved upstream Vortex array, compute, scan, source, and sink APIs can supply native operations
inside ShardLoom's feature and policy boundaries. Vortex integrations with external query engines
are not execution providers.

## Inputs and outputs

Compatibility inputs such as CSV or Parquet enter through explicit adapters. Vortex is the native
middle and the highest-fidelity persistence target. Compatibility outputs translate results and
report metadata loss; choosing an output format does not choose another execution engine.

See [data lifecycle](/field-guide/execution-routes) for preparation and reuse, and
[runtime and I/O](/field-guide/runtime-and-io) for supported formats and limits.

## Execution evidence

Admitted execution reports \`fallback_attempted=false\` and \`external_engine_invoked=false\`.
Certificates describe the source, work, output, and materialization boundaries. Where
included, sink digests and replay checks support result verification.

\`claim_gate_status\` describes the scope of the evidence. A local smoke test, a capability report,
and a reproducible benchmark answer different questions. A certificate alone does not establish
production readiness or performance superiority.

The [compute flow](/field-guide/compute-flow) illustrates how the pipeline adapts to a query.
Read [benchmark methodology](/field-guide/benchmark-methodology) before comparing timings.`
}) },
  { slug: "execution-routes", content: docsPage({
  "title": "Data lifecycle",
  "description": "How input preparation, native execution, reuse, and output fit one pipeline.",
  "order": 4,
  body: `Every supported workflow uses the same native pipeline. This page explains the lifecycle
behind a read or query: admit the source, prepare its representation where needed, execute,
and deliver the result. These stages do not require choosing an execution route.

## Source admission

The input adapter identifies the format and validates the source, schema, and requested work.
Unsupported formats or operations return a diagnostic before execution. A recognized file
extension alone does not establish full format support.

## Prepare once

An admitted local compatibility input is prepared into Vortex. The engine then executes over
that native representation. Preparation can be reused when the
source, schema, and artifact identities match. Generation checks span reuse and execution; detected changes fail explicitly.
Each query has fresh execution state. Preparation reuse does not cache query answers.

## Native Vortex

Data already stored as Vortex enters with its existing layout preserved. It uses the same native
operators and output contracts. Available optimizations still depend on the operation, types,
encodings, and physical layout.

## Generated rows

Generated and in-memory inputs also feed the native middle. Their supported schemas and
operations are bounded today. A generated source accounts for row construction instead of file
parsing; it does not introduce a separate compute engine.

## Deliver the result

Supported results remain native through the operator and sink boundaries. Vortex preserves the
most information. Compatibility writers translate at the output boundary and report metadata loss.
The [runtime and I/O guide](/field-guide/runtime-and-io) lists current composition and output limits.

## Cold and warm runs

A first-use measurement can include source reading, parsing, Vortex construction, persistence,
query execution, and output verification. A repeated query may reuse preparation and retained
handles. These measure different amounts of work within the same pipeline.

Keep preparation, execution, and result delivery visible when comparing timings. See
[benchmark methodology](/field-guide/benchmark-methodology) for the measurement contracts.

## Reading diagnostic labels

Reports retain names such as \`SourceState\`, \`VortexPreparedState\`, \`prepared_vortex\`, and
\`native_vortex\` to identify source state and evidence boundaries. They describe what happened
inside the pipeline; they are not a menu of faster or slower execution modes.
The [repository contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/universal-ingress-route-taxonomy.md)
defines the full diagnostic vocabulary. The [Python guide](/field-guide/python-surface) shows usage.`
}) },

  {
    slug: "compute-flow",
    content: docsPage({
      title: "Compute flow",
      description: "Follow a query from local input to native output, and see where ShardLoom avoids work.",
      order: 2,
      body: `import ComputeFlow from '../../../components/ComputeFlow.astro';

One pipeline handles every supported workflow: input → Vortex → native execution → output.
The examples below show how the work changes with the query. The engine selects applicable
optimizations; the controls here only switch illustrations.

<ComputeFlow />

## One native middle

SQL, Python, and the DataFrame-style surface enter ShardLoom planning. Admitted local
compatibility inputs prepare into Vortex; native Vortex inputs start there. Reuse validates
the source and artifact generation. It does not reuse a previous query answer.

Vortex remains the highest-fidelity persistence target. Parquet, Arrow IPC, Avro, ORC, CSV,
and JSON writers are explicit output boundaries, with their own type and size limits.
See [data lifecycle](/field-guide/execution-routes) and [runtime and I/O](/field-guide/runtime-and-io).

## Where the differentiators apply

- **Metadata and pruning:** avoid payload reads only when exact evidence supports the answer or exclusion.
- **Encoded execution:** dictionary identities, repeated values, and native typed accessors reduce expansion on eligible operations.
- **Capillary work units:** divide admitted work into bounded pieces, including local preparation tasks.
- **PulseWeave:** applies resource-aware control within supported preparation and execution work. Reports distinguish applied control from readiness-only evidence.
- **Late materialization:** defer group-value resolution or winning-row payload gathering until the result needs it.

The query and representation determine which mechanisms apply. Coverage is still being completed
within the same pipeline; an execution-mode choice is not required to obtain these benefits.
The [PulseWeave contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/pulseweave-runtime-control.md),
[native result contracts](https://github.com/depsilon/shardloom/blob/main/docs/reference/resident-native-results.md),
and [dictionary execution evidence](https://github.com/depsilon/shardloom/blob/main/docs/architecture/dictionary-preparation-screen-2026-09-30.md)
describe their implemented scope.

## Evidence travels with the result

The result reports source identity, execution and materialization boundaries, and output evidence.
Admitted work preserves \`fallback_attempted=false\` and \`external_engine_invoked=false\`.
An unsupported operation returns an explicit diagnostic.

Use [support and limitations](/field-guide/limitations) to check a workload, and
[benchmarks](/field-guide/benchmark-methodology) to understand measured evidence.`,
    }),
  },
  {
    slug: "start-local-proof",
    content: docsPage({
      title: "Install and run",
      description: "Install the published local engine and inspect native execution evidence.",
      order: 1,
      body: `ShardLoom ${packageVersion} is a published local engine. Install from PyPI or Homebrew,
then follow the [local query walkthrough](/start). GitHub pre-release and TestPyPI artifacts are
also available; see [package installation](https://github.com/depsilon/shardloom/blob/main/docs/getting-started/package-user-install.md)
for supported platforms and channel verification.

**Operational hardening is in progress.** Preview support refers to the remaining workload,
resource and failure acceptance. Package availability is verified. Read the
[local-engine exit criteria](/field-guide/limitations#release-and-readiness) before relying on a
production support promise.

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

Check the
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
      description: "Read, transform, and write data through ShardLoom's native pipeline from Python.",
      order: 2,
      body: `Use Python to read local data, build queries, and inspect or export results. Admitted queries
run in ShardLoom. Unsupported work returns a diagnostic without fallback execution.

## Run a local query

Create the small CSV in the [getting-started walkthrough](/start) before running this example.
The public \`run()\` report exposes the result and native execution evidence.

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

## Consume results in batches

Current source builds add \`iter_batches()\` for admitted results and
\`from_batches()\` for explicitly typed resident input. These additions merged
in PR #1526 after complete local and hosted checks; published v0.4.0 packages
predate them. Source builds now also admit opt-in \`streaming=True\` for one
finite source used once through pure scan/filter/project operations, as below.
Its [corrected local acceptance](https://github.com/depsilon/shardloom/blob/main/docs/benchmarks/native-fsst-admission-2026-10-07.md)
and independent packet inspection pass. Final integration is tracked in
[PR #1530](https://github.com/depsilon/shardloom/pull/1530).

\`\`\`python
def orders():
    yield [{"order_id": 1, "amount": 12.5}]
    yield [{"order_id": 2, "amount": None}]

frame = sl.from_batches(
    orders, schema={"order_id": "int64", "amount": "float64"},
    streaming=True,
)
with frame.iter_batches(batch_rows=1024) as batches:
    for batch in batches:
        print(batch.result_rows)
    assert batches.report is not None
\`\`\`

Input admits nullable Int64, finite Float64, booleans and UTF8 strings with an
explicit schema. Each input batch contains at most 2,048 row mappings and an
8 MiB frame, with up to 128 fields and 4,096 batches per source. In the default
resident mode (\`streaming=False\`), total native input must fit the query memory
grant. A factory, as above, supplies fresh input for repeated calls; an iterable
can be consumed once.

Streaming retains at most one native input batch, so cumulative input may exceed
the query grant within those finite limits. It supports incremental results,
bounded small collection or one native Vortex destination. Joins, repeated
sources, sets, sorting, explicit limits/offsets, aggregates, windows and dynamic
schemas reject before producer consumption. The producer must reach its explicit
end event before results are final. Typed intake and output compaction are
charged copies; output and sink reservations remain separate. No input spill or
process-RSS ceiling is added. See the
[complete streaming contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-input-completion-2026-10-07.md)
for native intake, wire-frame, ownership and failure bounds.

Requesting the next result batch acknowledges the preceding one. Use the
context manager when stopping early, and treat delivered batches as provisional
until the final report is present after full exhaustion. Prepare compatibility
file inputs explicitly to Vortex before batch consumption. See
[resource boundaries](/field-guide/runtime-and-io#resources-and-recovery).

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

Current capabilities, reviewed October 7, 2026.

## Native Execution

Supported operations use exact metadata, segment pruning, encoded reductions, weighted dictionary
aggregation, exact DISTINCT, and late payload gathering. Sparse dictionary selections decode only
referenced values. Coverage depends on the operation, type, and physical layout.

Admitted text grouping retains input-backed dictionary strings, uses compact exact-count state,
and shares partial construction between serial and bounded workers. Transformed text grouping
can overlap bounded dictionary preparation with native input progress and ordered consumption.
See the [implementation and measured tradeoffs](https://github.com/depsilon/shardloom/blob/main/docs/architecture/dictionary-preparation-screen-2026-09-30.md).

Admitted filters, projections, COUNT/SUM/AVG/MIN/MAX, exact DISTINCT, sort/Top-K, and selected
provider-backed join workflows execute today. Current source builds compose eight admitted
flat-scalar unary families—DISTINCT, duplicate removal and masks, tail, sampling, scalar
rewrites, melt, and rolling—with native relational stages and all eight local writers.
Operations consume the preceding stage's rows and column names while retaining source
declarations and the operation's CPU/memory allocation. See the
[composition scope and complete-result evidence](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-unary-composition-2026-10-02.md).
Current source builds also carry bounded static list/struct payloads through admitted
relational stages and ordered/repeated explode. Vortex, JSON, JSONL, Arrow IPC, Parquet
and Avro accept representable nested output. CSV translates nested values to
quoted JSON text cells; it does not preserve their logical dtype. ORC denies
nested output before publication. See the
[nested composition contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-nested-composition-2026-10-02.md).
Scalar pivot and pivot-table stages also compose through the same native plan.
Their observed columns bind during execution, including independent domains for
correlated inner rows; inspection remains inert. All eight local writers accept
representable scalar results within the existing field and state budgets. See the
[dynamic pivot contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-dynamic-pivot-composition-2026-10-03.md).
Current source builds after published v0.4.0 also admit static List,
FixedSizeList and Struct pivot index, domain and selected-value roles. Nested
cells support first, first_unique, count, min and max; first_unique accepts
repeated equal complete values and rejects conflicts. Nested extrema skip NULL
parents and use the shared child-NULL ordering. Python pivot() and
pivot_table(aggfunc="first") retain their first_unique alias, while SQL's
explicit first selects the first row, including NULL. Nested SUM/MEAN,
nested-index margins and pivot-state spill remain unsupported; nested MIN/MAX
margins require a UTF8 index. Nested cells accept absent or NULL fill only.
Representable nested results use Vortex, Parquet,
Arrow IPC, Avro, JSON and JSONL; CSV translates nested values to quoted JSON
text, and ORC rejects nested output. Existing 128-field, collection and memory
limits apply. Complete local workflow/regression acceptance and all 39 hosted
checks passed before PR #1525 merged. Published v0.4.0 packages predate this source support.
See the [nested pivot state contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-nested-pivot-state-2026-10-06.md).
Binary, Decimal128 (precision 1–38, scale 0–precision), Date32 and timezone-free
microsecond timestamps can travel as payloads, including nested leaves. Their
flat equality, hashing and ordering are admitted through relational joins, sets,
groups, windows and subqueries, with COUNT/COUNT DISTINCT/MIN/MAX and scoped
comparisons and expressions; Decimal key precision and scale must match.
Nested keys follow the subsequent contract below. Current source
builds admit typed literals, explicit CAST/TRY_CAST, exact decimal
arithmetic/rounding and scoped binary/calendar functions through the shared
native expression binder. Decimal arithmetic output metadata binds before
execution; explicit decimal downscaling requires zero discarded digits. Key
compatibility still requires matching decimal precision/scale and preserves
distinct temporal types. Wider analytic-window semantics, broader adapters
and state spill remain separate. See the [typed expression contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-typed-expressions-2026-10-03.md).
Flat typed values retain exact logical types through duplicate selection and
masks, tail/sample, replacement/forward-fill, lossless melt, rolling COUNT and
scoped pivot first/first-unique/COUNT. Exact Python literals lower into the same
native declarations. Primitive predicate and numeric sampling-weight limits
remain; see the [typed unary contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-typed-unary-2026-10-03.md).
See the [typed key contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-typed-keys-2026-10-03.md).
Binary supports all eight writers; ORC rejects decimal and temporal payloads.
Text output uses explicit typed encodings and does not preserve native logical
types. See the [typed payload contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-typed-payloads-2026-10-03.md).
The [nested key and retained-state contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-nested-keys-state-2026-10-04.md)
extends static List/FixedSizeList/Struct equality, hashing and ordering through
the existing relational kernels, with COUNT/COUNT DISTINCT/MIN/MAX and scoped
comparisons, NULL tests and CASE/COALESCE/NULLIF selection. Exact recursive key
schemas must match except for nullability. Retained nested values support
DISTINCT/duplicate selection and masks, tail, sampling, parent forward fill,
lossless same-shape melt and rolling COUNT. These finite nested/typed units are
merged with complete local and hosted check evidence. General Variant/extensions,
nested arithmetic and wider operator state spill remain separate boundaries.
The subsequent nested pivot scope is described above.
Current source builds also admit computed aggregate arguments and exact decimal
SUM/AVG/MIN/MAX through existing aggregate, rolling and scalar pivot state.
Decimal totals remain exact; inexact averages and final overflow fail explicitly.
Native ARRAY/STRUCT constructors preserve admitted logical children. File,
typed-memory and source-free inputs reach the same Vortex-native engine, while
Python conversions consume its typed results only at the requested output boundary.
Analytic aggregates and FIRST_VALUE/LAST_VALUE/NTH_VALUE admit explicit ROWS,
GROUPS and RANGE frames and exclusions through the same native window state.
Bounded RANGE requires one compatible ordering key; named windows, variable
offsets, calendar-month intervals, IGNORE NULLS and general window-state spill
remain outside the [frame contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-analytic-frames-2026-10-05.md).
The [analytic-frame acceptance](https://github.com/depsilon/shardloom/blob/main/docs/benchmarks/native-analytic-frames-full43-2026-10-05.md)
records 22,658 public checks, including 2,213 frame checks, and all 129 Full43
executions. The [fresh release UAT](https://github.com/depsilon/shardloom/blob/main/docs/benchmarks/release-candidate-fresh-uat-2026-10-05.md)
adds complete input/output workflow comparisons. These suites overlap and do
not establish a comparative speedup.
General joins, set operations, analytic windows and broader subquery shapes still have native coverage
gaps. See the [front-door contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/v1-front-door-runtime-scope.md)
and [remaining family inventory](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-runtime-completion-2026-09-20.md#finite-availability-inventory).

## Scalar subqueries

Current source builds after published v0.4.0 admit scalar-value subqueries through
SQL \`(SELECT ...)\` and Python \`sl.scalar_subquery(...)\`. The v0.4.0 packages
predate this addition. Use them to put one query result into an expression:

\`\`\`sql
SELECT value, (SELECT outer.value + 10 AS adjusted) AS adjusted
FROM range(1, 4)
\`\`\`

This returns values 1, 2 and 3 with adjusted values 11, 12 and 13. The inner
query must bind one static output column. Zero rows return a typed NULL, one
row returns its value, and multiple rows raise a cardinality error even when
their values are equal. Correlation uses explicit \`outer.<column>\` references.
CASE/COALESCE execute only selected branches; every branch still requires syntax,
source, type and schema admission. Dynamic-pivot-dependent scalar schemas and
lateral relations remain unsupported. Existing dtype, writer and resource limits
apply. See the [Python example](https://github.com/depsilon/shardloom/blob/main/python/README.md),
[scalar-subquery contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-scalar-subqueries-2026-10-05.md)
and [acceptance evidence](https://github.com/depsilon/shardloom/blob/main/docs/benchmarks/native-scalar-subqueries-full43-2026-10-05.md).

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

Supported owned results retain Vortex arrays, validity, and memory credits. Current source builds
stream admitted flat aggregate, ordered, unary, and relational results through all eight local
writers without rerunning the query or reparsing serialized JSON for binary export. This includes
admitted native ordering spill. Small computed-result collection remains bounded to
**65,536 rows, 128 scalar fields, and 8 MiB**; complete writers use bounded native batches and
can exceed the collection row and byte limits. Type, resource, and write-policy admission still
apply. Nested and extension results have separate coverage limits. See the
[output contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/v1-local-output-sink-scope.md).

Flat aggregate collection and writes use the same native admission, carrying the complete
filter, group, measure, HAVING, order, and limit chain with declared schemas and resources.
Explicit SQL \`NULLS FIRST\`/\`NULLS LAST\` and Python \`sort(..., nulls="first")\` or
\`nulls="last"\` place nulls independently of ascending or descending values.
JSON collection reports the final materialization boundary and metadata loss; exceeding its
row or serialized-byte limit fails without returning a successful prefix.

## Resources And Recovery

Current source builds account for reviewed FSST/Zstd payload,
view and validity buffers through the shared native memory owner. Retained
clones and slices keep their allocation credits. It also adds
[Python batch input and results](/field-guide/python-surface#consume-results-in-batches).
These additions merged in PR #1526 after complete local and hosted checks;
published v0.4.0 predates them. Source implementation also admits the actual
one-shot Zstd decoder and by-reference prepared-dictionary workspaces before
allocation, releasing them after each decode. See the
[workspace acceptance](https://github.com/depsilon/shardloom/blob/main/docs/benchmarks/native-codec-workspaces-2026-10-07.md)
for its separate local and hosted evidence. The
[builder resource acceptance](https://github.com/depsilon/shardloom/blob/main/docs/benchmarks/native-builder-resources-2026-10-07.md)
also covers native primitive/Boolean/decimal Chunked output, nullable bitmaps and
numeric/string builder finalization buffers. Local source and complete workflow
checks pass; PR #1529 merged after all 39 hosted checks passed, with the accepted
runtime unchanged. Child decoder scratch, structural
metadata, compression contexts, dictionary training and other unreviewed
allocations remain outside this finite scope. These resource corrections make
no speedup claim, and a query grant still does not bound total process RSS.

Opt-in \`from_batches(..., streaming=True)\` now has separate
[local resource and correctness acceptance](https://github.com/depsilon/shardloom/blob/main/docs/benchmarks/native-fsst-admission-2026-10-07.md),
including complete 4.5-GiB UTF8 input under a 1-GiB native grant. It retains at
most one native input batch and admits only one finite source through pure
scan/filter/project operations. Output compaction prevents retained results from
pinning input; native sink metadata and retained output still consume credits.
Late failure prevents successful completion and incomplete file publication.
The accepted runtime also rejects malformed FSST row lengths before native
decoder allocation. Final integration is tracked in
[PR #1530](https://github.com/depsilon/shardloom/pull/1530); published v0.4.0 is unchanged.

Prepared sessions retain source handles and supported lowering while calls create fresh execution
state. Resident serving can bound concurrent calls, CPU grants, and positional I/O, with an
explicit reserved metadata lane. This does not establish production-scale fairness or an RSS ceiling.
Native file operations drain admitted I/O and reader ownership before completion. Metadata-only
aggregates avoid payload and worker admission, including when no spare payload credit is available.

Supply CPU and memory limits at each operation's start through Rust, the CLI, or Python.
The runtime selects concurrency within the supplied maximum and the CPU capacity available
to the process; an explicit one-CPU allocation stays one. Streaming ingestion shares this
grant across ready source, conversion, statistics, and writer tasks. Full bounded queues
yield their drivers, while memory admission can narrow unfinished work. Allocations can
vary well beyond P4/P6/P8. I/O and serial work may limit useful concurrency; these controls
do not guarantee full CPU utilization or a total-process RSS ceiling. See the
[allocation contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/adaptive-ingest-budget-2026-10-02.md).

Current source builds carry the same \`memory_gb\` and \`max_parallelism\` request through SQL and
DataFrame collection and local writers. Optional \`spill\` declares an existing local workspace,
\`quota_bytes\`, and \`buffer_bytes\`. Nullable multi-key relational ordering can flush typed
Vortex runs and merge them under the same query grant and disk quota, including composed inputs.
This includes flat typed keys in the [typed key contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-typed-keys-2026-10-03.md).
The buffer is a flush threshold; it is not a second memory grant or a whole-process RSS ceiling.
Verified cleanup must finish before an execution or writer reports success.

COUNT/DISTINCT and selected numeric sort retain their existing specialized strategies. Aggregate,
join, and window state do not gain spill from relational ordering permission. Other key types and
broader resource accounting remain separate work. See
[COUNT/DISTINCT contracts](https://github.com/depsilon/shardloom/blob/main/docs/reference/resident-native-results.md),
[native spill contracts](https://github.com/depsilon/shardloom/blob/main/docs/reference/native-query-spill.md),
and [serving evidence](https://github.com/depsilon/shardloom/blob/main/docs/architecture/concurrent-native-serving-2026-09-20.md).

Every admitted execution preserves \`fallback_attempted=false\` and
\`external_engine_invoked=false\`. Unsupported work returns deterministic diagnostics.`,
    }),
  },
  {
    slug: "benchmark-methodology",
    content: docsPage({
      title: "Benchmarks",
      description: "ClickBench comparisons, current engineering evidence, and how to read a measured result.",
      order: 3,
      body: `ClickBench is one workload used to test and compare ShardLoom; its schema and query set do
not define the engine's intended breadth. Use it for public cross-engine OLAP comparisons.
This guide explains ShardLoom's
engineering evidence and measurement boundaries; it does not present a public ranking.

**[Open ClickBench ↗](https://benchmark.clickhouse.com/)**

## Current engineering evidence

Reviewed October 1, 2026. Recent local work validates complete query results and measures
individual changes against a frozen ShardLoom control:

- [Native execution and ingestion comparisons](https://github.com/depsilon/shardloom/blob/main/docs/architecture/performance-quiet-intake-2026-09-30.md) — retained mechanisms, rejected experiments, and links to their acceptance evidence.
- [Dictionary preparation](https://github.com/depsilon/shardloom/blob/main/docs/architecture/dictionary-preparation-screen-2026-09-30.md) — source-backed strings, bounded preparation overlap, and measured tradeoffs.
- [Native runtime completion](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-runtime-completion-2026-09-20.md) — complete-value Full43 regression runs, public-call checks, and the remaining availability inventory.

These are workload- and machine-specific development results. A full 43-query regression run
does not by itself establish an accepted ClickBench submission, cross-engine superiority,
or production readiness. Earlier receipts remain historical evidence for their named builds.

## What a timing includes

| Measurement | Read it as |
| --- | --- |
| Cold preparation | Source read and parse, Vortex construction, persistence, and the checks included by that runner. Keep this separate from warm queries. |
| Query over existing Vortex data | Execution against a native or previously prepared artifact. State whether setup, process startup, output, and verification are timed. |
| Fresh-process complete query | A complete invocation and result, with startup and output costs. OS cache can still be warm. |
| Resident call | A retained local worker or session; state which source handles and lowering are reused. Every query still executes. |
| Replay/publication proof | Additional validation and evidence work, where the runner includes it. Do not silently fold it into or remove it from runtime. |

Older artifacts use \`hot_runtime\`, \`full_replay_proof\`, \`publication_proof\`, and
\`external_baseline\` labels. Use each artifact's exact formula and evidence tier rather than
assuming those labels describe every current runner.

## External baselines

External engines may supply benchmark baselines or independent correctness references.
Their execution never satisfies a ShardLoom execution contract or its no-fallback evidence.

## Comparing results

Compare the same query semantics, full results, dataset and layout, build, machine, resource
caps, cache/setup state, and output boundary. Report repetitions and the chosen statistic;
do not mix a sum of minima with a median or a separate ingest run.

If \`performance_claim_allowed=false\`, local artifacts support engineering decisions without
authorizing superiority claims. The [support and limitations](/field-guide/limitations) page
separates executable scope from public readiness claims.`,
    }),
  },
  {
    slug: "limitations",
    content: docsPage({
      title: "Support and limitations",
      description: "Current workflow coverage and the remaining work toward broader data processing.",
      order: 4,
      body: `ShardLoom executes local queries over native and prepared Vortex data today.
Coverage is specific to the operation, types, source layout, enabled features, and output contract.
The product direction is general-purpose data processing through one native pipeline. The gaps
below are completion work within that pipeline.

Current capabilities, reviewed **October 7, 2026**. See the
[public support matrix](https://github.com/depsilon/shardloom/blob/main/docs/release/public-status-matrix.md)
for the detailed evidence behind this scope.

## Runtime Limits

| Area | Available today | Remaining work or boundary |
| --- | --- | --- |
| Core analytics | Metadata counts, filtering, projection, COUNT/SUM/AVG/MIN/MAX, exact DISTINCT, and sort/Top-K, including explicit null ordering in flat aggregate collection and writes. | Function, type, layout, and composition coverage is finite. Parser recognition alone does not mean native execution. |
| Relational and DataFrame operations | Current source builds compose admitted relational and unary stages, including static nested payloads/explode, nested key/retained-state operations, exact decimal reductions and accepted analytic ROWS/GROUPS/RANGE frames. Scalar pivot columns bind during execution, including correlated inner scopes. Source builds after published v0.4.0 admit [scalar-value subqueries](/field-guide/runtime-and-io#scalar-subqueries) with local and hosted acceptance. They also admit static List/FixedSizeList/Struct pivot index, domain and selected-value roles with complete [workflow, resource and regression acceptance](https://github.com/depsilon/shardloom/blob/main/docs/benchmarks/native-nested-pivot-state-full43-2026-10-06.md) and hosted integration. | Operation/type coverage is finite. Named windows, variable frame offsets, dynamic-pivot-dependent scalar schemas, lateral relations, nested SUM/MEAN, non-NULL nested fill, nested-index margins, unsupported nested leaves and pivot-state spill remain outside the admitted contracts; nested MIN/MAX margins require a UTF8 index. Broader adapters and scalar pivot's 128-field, type and memory boundaries remain. |
| Repeated queries | Retained local workers, source handles, supported lowering, and validated preparation reuse. | Fresh execution state per call. No global result cache or automatic incremental refresh of arbitrary queries. |
| Results and writes | Native owned results and admitted local Vortex, Parquet, Arrow IPC, Avro, ORC, CSV, JSON, and JSONL writes. | Operator-to-sink, type, feature, and write-policy restrictions apply. See the specific handoff limit below. |
| Memory and recovery | Reservations, bounded serving admission, specialized COUNT/DISTINCT/numeric-sort spill, and nullable multi-key relational ordering spill in current source builds. | Spill remains operator-specific; aggregate/join/window state, broader reader/codec accounting, and whole-process RSS bounds remain separate work. |
| Physical layout | Native Vortex input preserves its existing layout; compatibility preparation builds a Vortex artifact. | A shared all-I/O layout optimization policy remains follow-up work. |

Current source builds add reviewed FSST/Zstd buffer accounting and
[Python batch input/incremental results](/field-guide/python-surface#consume-results-in-batches).
PR #1526 merged after complete local and hosted checks. Published v0.4.0
predates these additions.
The subsequent [Zstd workspace unit](/field-guide/runtime-and-io#resources-and-recovery)
also admits the actual decoder and by-reference prepared dictionary. Its
resource proof remains limited to the reviewed allocations.
The [builder resource unit](/field-guide/runtime-and-io#resources-and-recovery)
covers primitive/Boolean/decimal Chunked output, nullable bitmaps and
numeric/string finalization buffers, with local and hosted acceptance complete
in PR #1529. Child decoder scratch and structural metadata remain separate.
Default input remains resident under the query grant. Opt-in
[streaming input](/field-guide/python-surface#consume-results-in-batches) admits
one finite source used once through pure scan/filter/project, with one retained
native input batch and observed end-of-input required for success. Its local
acceptance and packet inspection pass; final integration is tracked in
[PR #1530](https://github.com/depsilon/shardloom/pull/1530).
Blocking/repeated-source plans and streamed compatibility writes remain denied.
Output backpressure does
not enable general operator spill, account for all codec scratch, or bound
consumer-retained Python objects and total process RSS.

The **65,536-row / 128-top-level-field / 8-MiB** bound applies to small computed-result collection.
Current source builds deliver complete admitted flat results through bounded native batches to
all eight local writers, including admitted ordering spill, above the collection row and byte
limits. Representable static nested results use Vortex, JSON, JSONL, Arrow IPC, Parquet
and Avro. CSV translates nested values to quoted JSON text without logical-type
preservation; ORC rejects nested output. Recursive schema/child-buffer admission,
format fidelity, resources and write policy still apply. Binary, exact Decimal128,
Date32 and timezone-free microsecond timestamps are admitted payloads, including
nested leaves. Flat key operations for these types follow the [typed key contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-typed-keys-2026-10-03.md).
The [nested key and retained-state contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-nested-keys-state-2026-10-04.md)
extends equality, hashing and ordering to static lists, fixed-size lists and
structs through existing joins, sets, groups, sort, windows and subqueries,
with COUNT/COUNT DISTINCT/MIN/MAX and scoped selected expressions. Exact
recursive schemas include field names/order, widths, decimal metadata and
temporal identity; recursive nullability is ignored for key compatibility.
The finite nested/typed units are merged after complete local and hosted checks.
Current source builds admit typed literals, explicit CAST/TRY_CAST, exact decimal
arithmetic/rounding and scoped binary/calendar functions through the shared
native expression binder. Decimal arithmetic output metadata binds before
execution; explicit decimal downscaling requires zero discarded digits. Key
compatibility still requires matching decimal precision/scale and preserves
distinct temporal types. Wider analytic-window semantics, broader adapters
and state spill remain separate. See the [typed expression contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-typed-expressions-2026-10-03.md).
Flat retained unary state admits the four typed domains for duplicate selection,
tail/sample, replacement/forward-fill, lossless melt, rolling COUNT and scoped
pivot policies. The nested contract also admits DISTINCT/duplicate selection
and masks, tail, sampling, parent forward fill, same-shape melt and rolling COUNT.
Forward fill replaces a NULL parent; child NULLs do not trigger filling.
Nested SUM/MEAN, non-NULL nested pivot fill, nested-index margins, nested
arithmetic/string operations, temporal arithmetic rewrites, wider typed
predicates and general state spill remain unsupported. Nested MIN/MAX margins
require a UTF8 index. Nested pivot spill remains unsupported.
See the [typed unary contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-typed-unary-2026-10-03.md).
Computed aggregate arguments and exact decimal aggregate/rolling/scalar-pivot
reductions now have complete local acceptance. Inexact decimal averages and final
overflow remain explicit errors. ARRAY/STRUCT constructors use admitted native
children. See the [revised engine acceptance](https://github.com/depsilon/shardloom/blob/main/docs/benchmarks/native-typed-reductions-full43-2026-10-05.md)
for the frozen scope, full public and ClickBench results, and remaining work.
Subsequent [analytic-frame acceptance](https://github.com/depsilon/shardloom/blob/main/docs/benchmarks/native-analytic-frames-full43-2026-10-05.md)
covers framed aggregates, FIRST_VALUE/LAST_VALUE/NTH_VALUE and explicit
exclusions. Bounded RANGE needs one compatible ordering key. Calendar-month
intervals, IGNORE NULLS and general window-state spill remain unsupported.
ORC rejects decimal/temporal output. General Variant/extension operations retain
separate coverage limits. See the
[output contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/v1-local-output-sink-scope.md).

Use [runtime and I/O](/field-guide/runtime-and-io), the
[front-door contract](https://github.com/depsilon/shardloom/blob/main/docs/architecture/v1-front-door-runtime-scope.md),
and the [native completion inventory](https://github.com/depsilon/shardloom/blob/main/docs/architecture/native-runtime-completion-2026-09-20.md#finite-availability-inventory)
for exact scope. A familiar method name is not an every-shape support promise.

## Extending breadth and volume

The [completion plan](https://github.com/depsilon/shardloom/blob/main/docs/architecture/universal-workflow-completion-2026-10-01.md)
starts with whole read → transform → write workflows. Priorities include native result streaming
through chained operators and writers, broader relational and type coverage, shared resource
accounting, and native spill for larger state. New adapters connect to the same engine.

Acceptance must cover varied schemas, skew, nulls, output sizes, and data larger than the configured
memory budget. ClickBench remains one regression suite alongside those workflow checks.
Adding a reader alone does not complete a workflow, and removing a size guard alone does not
establish safe scale.

## External systems

The local workflow contract covers admitted files, local Vortex manifests and partitions,
and scoped local table-file reads. Remote output, catalog/table transactions, and production
object-store or Foundry runtime are outside it. Local fixtures and metadata inspection are
separate from live provider execution.

Arbitrary Python data callbacks and general extension/UDF execution are not implied by the
DataFrame surface. Network, credential, API, and model effects require explicit admission;
discovery does not execute them.

## Release and readiness

**Published local engine; operational hardening in progress.** GitHub pre-release, PyPI,
TestPyPI and Homebrew package access is verified. The technical-preview designation describes
support maturity; missing publication proof is no longer a blocker.

The remaining local-engine exit criteria are concrete:

- Define the supported platforms, workload shapes, formats, types and resource/storage conditions.
- Account for reader/codec scratch, retained operator state, queued results and writers, with
  bounded retention or safe denial. A query memory grant is not a whole-process RSS ceiling.
- Accept complete workflows under memory pressure, cancellation, source changes and storage
  failures, including output integrity, owned cleanup and documented recovery behavior.
- Bind correctness, operating measurements, API compatibility and support/upgrade instructions
  to the approved release source and binary.

Ordering spill already has quota, corruption, cancellation and consumer-failure coverage.
General aggregate/join/window spill and broader reader/codec accounting remain open. A stable
local release need not wait for cloud integrations or every SQL feature; unsupported work must
have an explicit boundary. The repository owns the
[full acceptance checklist](https://github.com/depsilon/shardloom/blob/main/docs/release/production-certification-gate.md#local-engine-preview-exit-criteria).

Claims of production support, broad SQL/DataFrame parity and Spark displacement require their own
evidence. Local runtime and benchmark evidence do not establish production distributed/live-hybrid
service or performance superiority. Read the
[benchmark evidence](/field-guide/benchmark-methodology) within its measured scope.

## Failure Behavior

Unsupported work must produce a deterministic blocker or report-only posture. It must not execute through Spark, DataFusion, DuckDB, Polars, pandas, Velox, Trino, a database, a warehouse, or another fallback engine.`,
    }),
  },
];

function fieldGuideIndex() {
  return docsPage({
    title: "Field Guide",
    description: "Read, transform, and write data through one Vortex-native pipeline.",
    order: 0,
    body: `Read data, transform or query it, and deliver the result through one Vortex-native pipeline.
Input and output adapters connect formats to the same engine. Python, SQL, DataFrame-style calls,
and the CLI provide familiar ways to describe the work.

ShardLoom is being built for general-purpose data processing. Its published local engine supports
specific workflows today. Operational hardening is in progress; the guide makes the supported
coverage and [preview exit criteria](/field-guide/limitations#release-and-readiness) visible.

## Get started

- [Install and run](/field-guide/start-local-proof) — install the local engine and verify a query.
- [Python](/field-guide/python-surface) — read, filter, inspect results, and choose an output.

## Understand the engine

- [Execution model](/field-guide/execution-model) — one native pipeline and automatic work avoidance.
- [Data lifecycle](/field-guide/execution-routes) — source admission, preparation, reuse, and output.
- [Runtime and I/O](/field-guide/runtime-and-io) — operators, formats, result limits, and resource boundaries.

## Read the evidence

- [Benchmarks](/field-guide/benchmark-methodology) — ClickBench and broader workflow evidence.
- [Support and limitations](/field-guide/limitations) — what executes today and the breadth and scale still to complete.

For the architecture diagram, open the [compute flow](/field-guide/compute-flow).
The [repository](https://github.com/depsilon/shardloom) holds detailed API references and implementation evidence.`,
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
