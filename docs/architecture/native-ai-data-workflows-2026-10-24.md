<!-- SPDX-License-Identifier: Apache-2.0 -->

# AI data preparation, delivery and retrieval

Status: accepted October 24 maintainer addition at the back of the active queue,
after [everyday analytical completion](phased-execution-plan.md#everyday-analytical-completion)
and before the optional breadth backlog. This is implementation work within the
same SQL/DataFrame, storage, resource and execution model. It does not change
the preceding queue or establish a separate AI engine, released capability,
performance claim or completion of CG-1 through CG-23.

The product workflow is read → filter/join/prepare features → select necessary
vectors, text or media → deliver typed model batches or retrieve matches → keep
results as ordinary ShardLoom data. Model execution remains a separately
declared boundary. Compressed processing before that boundary does not imply
that arbitrary models can consume every Vortex encoding.

## Reuse and provider intake

The [rich-data package](native-rich-data-lifecycle-2026-10-24.md) owns vector and
tensor fidelity, opaque payload ownership, lower precision and exact scalar
vector expressions. This package connects those foundations to model consumers
and relational retrieval; it must not create parallel types or kernels. The
[connected programming model](native-connected-programming-model-2026-10-24.md)
owns plan composition, functions and explicit application batch functions;
[universal I/O](native-universal-io-performance-coverage-2026-10-24.md) owns source,
publication and transport boundaries. Warm source reuse supplies bounded,
generation-checked retained state, not cached query answers.

Current source has native Arrow batch delivery in
[`capsules.rs`](../../shardloom-python/src/capsules.rs) and Arrow/NumPy export in
[`_batches.py`](../../python/src/shardloom/_batches.py). These are integration
seams, not proof of model-ready shape, worker or ownership semantics.
[`native_payload_schema.rs`](../../shardloom-vortex/src/native_payload_schema.rs)
excludes Float16 from native scalar admission and admits selected temporal
extensions; [`native_arrow_conversion.rs`](../../shardloom-vortex/src/native_arrow_conversion.rs)
also excludes Float16 and maps selected temporal extensions. The
[`extension capability matrix`](../../shardloom-vortex/src/extension_type_capability.rs)
and [`status surfaces`](../../shardloom-cli/src/status_capabilities.rs) still
describe vector/tensor execution and TurboQuant blockers. Extend real shared
providers and retire blockers only with executable evidence.

Historical 0.85 source review: the supplied upstream review identifies
concrete Vector/FixedShapeTensor types, vector scalar functions and a brute-force
similarity-expression helper. Verify their exact crate/version, features,
registration, null/shape/precision behavior and callable path before choosing
reuse. Upstream development source, a published 0.74.0 TurboQuant package and
current Python documentation are distinct evidence; none certifies a ShardLoom
interface. The installed 0.85.0 preview edition lists tensor/vector type and
function IDs, but those edition declarations alone are not callable
implementation proof. In 0.90 the preview declaration is empty; that does not
establish the availability or absence of separate optional provider APIs. Check
the exact 0.90 crates, features, registration and callable paths when this
package starts. Its scan `StrictSortedBuffer` requires strictly increasing
indices, reinforcing the indexed adapter's reorder/duplicate-restoration contract.
A cosine-threshold full-scan helper is not a top-K operator
or ANN index. Keep upstream providers inside approved `shardloom-vortex`
boundaries with dependency/license review and native certificates.

The [controlled 0.90 refresh](native-vortex-provider-refresh-2026-10-24.md)
provides the current provider baseline and records specific activated paths.
Verify each type and function against its own admission evidence when this
package starts; the refresh does not establish vector/tensor or rich-type
admission. Retain this package's logical result identity,
arbitrary sample-order/duplicate restoration, epoch and ownership contracts;
upgrading the scanner does not implement those consumer semantics by itself.

## Ordered implementation and acceptance

1. **Model-ready typed batches.** Connect ordinary query results to declared
   feature columns, labels, shapes, element types and null policy. Preserve
   feature order, row identity and tensor metadata; specify casting, ragged
   values and invalid-shape failures. Consume native batches without a Python
   row reconstruction step. Keep conversions explicit for compressed, nullable
   or incompatible layouts. Use CPU execution first while representing device
   ownership and transfers. A writable consumer must receive exclusive ownership
   or an admitted copy; it must not mutate immutable shared ShardLoom owners.
   Test consumer mutation, slices, source/result drop and last-owner release.
   Do not make an unconditional zero-copy or GPU claim.
2. **Streaming, then indexed dataset adapters.** Start with a streaming dataset
   over the result-batch contract. Add optional PyTorch and Hugging Face adapters
   that consume the executed ShardLoom plan, including its joins and filters;
   reopening the original source through another library cannot replace it.
   Then add indexed batches with requested order and duplicate multiplicity:
   map logical result identities to source/retained-result regions, read sorted
   unique indices as required by the admitted provider, and scatter back to the
   requested sequence. Source row numbers are not post-join result identities.
   When an arbitrary plan needs an indexable retained result, create it explicitly
   under existing ownership, memory and spill contracts. Hold a stable generation
   for an epoch; define changed-source failure, restart, worker partitioning,
   seeds, sample count and final-batch behavior. Full shuffle and bounded-buffer
   shuffle are different contracts. Verify no worker-induced duplicates or losses,
   exact requested duplicates, reproducibility and cancellation/early close.
3. **Feature preparation with persisted fitted state.** Add stable feature
   assembly, imputation, scaling, category mapping and reproducible train/
   validation/test splits through shared expressions and reducers. Fit only the
   declared training population; persist parameter values, algorithm/version,
   schema, feature order, category identity and null/unknown-category behavior.
   Reapply that state after reopening, without refitting each batch or inference
   input. Physical per-chunk dictionary codes are not semantic model category IDs.
   Cover deterministic group/time splits and prevent leakage across memberships.
   Build point-in-time joins on the temporal package, testing both economic
   effective time and application availability time. Late-arriving information
   must not become a historical feature merely because its effective date is old.
4. **Exact retrieval in ordinary plans.** Compose admitted vector expressions
   with eligibility/authorization filters, ranking/top-K and late payload fetch.
   Define metric, dimensions, embedding-space compatibility, accumulation type,
   null/non-finite/zero-norm behavior and ties through the shared expression
   contract. Ordinary numeric vectors need no invented model identity. Exact
   search works without an index. Eligibility must define the searched population;
   global top-K followed by discarding ineligible rows is not equivalent.
   Verify complete scores and identities against an independent exact reference,
   including selective filters, duplicates, ties and fewer than K eligible rows.
5. **Lexical/hybrid retrieval and optional compact representations.** Add a
   distinct full-text path with declared tokenization, normalization, ranking
   and index-generation/refresh semantics, then an explicit hybrid ranking rule.
   Substring matching alone is not full-text retrieval. Bind indexes to source
   generations, authorization and publication/cleanup contracts. Investigate
   TurboQuant after exact vectors and batch delivery: preserve the original
   logical vector by default and make lossy codes an explicitly selected derived
   column or index representation with scheme/version metadata. Freeze finite
   storage, encoding/decoding, query-cost and quality gates before the experiment.
   Record recall/ranking quality against the exact eligible population, including
   any candidate rescoring. Rescoring cannot recover omitted candidates and does
   not make approximate retrieval globally exact. Retain a justified optional
   representation or record an evidenced rejection; a permanent report-only
   feature flag does not complete the investigation.

Alongside these groups, expose dataset/schema discovery, plan validation,
resource/permission-bound execution, bounded preview and retained/exportable
result handles through existing query and diagnostic APIs. The
[`agent contract pack`](agent-contract-pack.md) is report-only; reuse its
contracts instead of adding another overlapping report catalog. An optional
tool-protocol adapter must call the same operations. Large results stay behind
handles, with authorization, lifetime and cancellation enforced. Documents and
data are untrusted inputs, not authority to execute actions. Planning must not
silently invoke a model or a natural-language executor.

Batch inference follows usable delivery as an explicit extension of the queued
application-function boundary. Define model initialization, schemas, stable
record alignment, batch size, cancellation, errors and retry/idempotency behavior.
Remote models additionally require declared credentials, egress and cost limits.
ShardLoom owns the surrounding data computation; the supplied model owns
prediction. Model training frameworks, a feature-store service, automatic model
selection, OCR/media interpretation and GPU implementation are not prerequisites.
No model call, credential setup or external effect is authorized by this document.

## Evidence and completion

Use a mixed dataset with record IDs, labels, quality fields, text, vectors,
opaque media bytes and nested model-output records. Ordinary selection must
avoid unneeded vector/media work; selected values must survive retained results,
streaming/spill, writing and separate-process reopening. Coordinate flexible
nested output with the rich-data type contract. A Variant roadmap entry does not
establish an admitted provider or justify silently discarding unknown fields.

Extend existing conformance tooling with five connected workloads: typed batch
delivery from a joined/filtered query; repeated streaming and indexed epochs;
fit → persist → reopen → apply with group/time splits and availability-aware
features; exact filtered retrieval with late payloads; and lexical/hybrid search
with an independently assessed compact-representation experiment. Use real
optional consumer clients for handoff checks. Verify full values, identity,
shape, nulls and order, including empty inputs, repeated indices, source changes,
consumer mutation, worker failures, memory pressure, spill and partial publication.

Measure training-batch throughput, repeated-epoch cost, bytes read/decoded/copied,
peak memory, retrieval engine time and quality, and round-trip fidelity. Freeze
scale, consumer version, resource limits, reference semantics and finite
acceptance gates before comparison. PulseWeave/capillary work shaping and dynamic
admission apply through shared scan, state and delivery components where measured
work warrants them; metadata pruning and late payload access come first.
Full43 remains the isolated engine-time regression check and cannot establish
AI-workload value. Run focused provider/public-consumer tests followed by the
required formatting, workspace lint/tests and native-feature gates for changed
code. Capability and documentation updates follow proved behavior; move completed
checklists to the existing ledger with source-bound evidence. No external engine
fallback, package publication or superiority claim follows from queue intake.

## Primary references

- [Vortex scan I/O](https://docs.vortex.dev/api/python/io) documents sorted,
  non-null row indices and strictly increasing indices for its Arrow scan.
  The implementation must verify the selected pinned Rust/provider contract.
- [Vortex Hugging Face Datasets](https://docs.vortex.dev/api/python/datasets)
  documents streaming integration and scan selection; it is an integration
  reference, not permission to bypass the ShardLoom plan.
- [PyTorch data loading](https://docs.pytorch.org/docs/2.14/data.html) describes
  indexed/iterable datasets and multi-worker loading.
  [PyTorch DLPack](https://docs.pytorch.org/docs/2.14/dlpack.html) documents shared
  memory and mutation effects, which require deliberate ownership at handoff.
- [Polars `to_torch`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.to_torch.html)
  supports tensor/dataset handoff and labels the interface unstable. It provides
  a consumer-facing comparison target, not a ShardLoom dependency.
- [Published Vortex TurboQuant documentation](https://docs.rs/vortex-turboquant/latest/vortex_turboquant/)
  identifies version 0.74.0 and an MSE-focused lossy representation without QJL
  residual correction. Recheck exact version compatibility before integration.

RFCs 0011/0013/0014/0021/0023 and 0033–0035 govern extensions, ownership,
resource/effect policy and user/remote interfaces. Settle missing persistent
fitted-state, indexed-result and search-index contracts before their respective
implementation groups; avoid creating a parallel execution or resource system.
