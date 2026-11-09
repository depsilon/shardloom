# Resource-admitted wide nested schemas

Status: implementation, local runtime and installed-package acceptance complete,
November 9, 2026. This is a source change after 1.1.0. Hosted integration and
package publication are separate; item 19 and competitive gates remain open.

## Contract and provider decision

This work replaces the nested-field width of 1024, expanded logical-node count
of 4096 and 8 MiB conservative metadata estimate with admission through the
existing operation memory pool. The depth-24 boundary remains until a separate
unit covers provider serialization, value traversal and destruction together.
The JSON declaration/cell byte limits, physical IPC verification, buffer alias
controls and output-format representation limits are independent contracts.

Vortex-first classification: `use_vortex_native_provider`. The pinned
Apache-2.0 Vortex 0.93 `DType`, `StructFields`, array, file and native spill
providers remain the type and persistence model. There is no parallel type
system, new execution dependency or external residual evaluator. Existing
native feature gates and execution/I/O certificates continue to report
`fallback_attempted=false` and `external_engine_invoked=false`.

Schema inspection borrows the existing memory owner and, when operating in a
call, its cancellation token. It grows a `MemoryLease` before visiting each
node and before allocating a uniqueness table. An expanded shared subtree
therefore consumes the caller's grant even if its physical type objects share
storage. Arithmetic overflow, cancellation and admission failure propagate;
they do not become an unsupported-type routing decision. Estimation releases
its inspection credit; persistent metadata construction transfers a lease to
the existing source, plan, buffer or result owner. Temporary uniqueness uses
hashing of borrowed names rather than a quadratic duplicate scan.

The provider's lazy struct parser admits its own name/field/cache/lookup
containers before construction. Fallible field inspection exposes allocation
and parse errors, and first-touch parsing is serialized so concurrent callers
cannot construct duplicate temporary child schemas. The container owner is
not a whole-process memory guarantee: detached caller-owned names and DType
clones, arbitrary extension implementations and unrelated provider scratch
remain outside it. Native arrays/results must retain their own full schema
credit, including empty outputs and independently retained Arrow children.
Those lifetime boundaries require tests before acceptance.

Cold file consumers also use fallible inspection: legacy/nested statistics,
struct readers, field expressions and native binary-object detection propagate
lazy schema admission failures. This preserves lazy construction while avoiding
an allocation panic at a previously infallible field accessor. Existing
infallible provider compatibility APIs remain outside these admitted paths.

## Finite acceptance fixed before candidate execution

The selected fixture family uses nullable Int64 leaves with values determined
by row, branch and field ordinal, plus ordinary key columns and a small
metadata-bearing extension inside a separate ordinary container. Parent-null,
leaf-null and empty-list cases are distinct. Expected values are generated
independently from ordinal formulas, not by reading candidate output.

| Shape | Exact structure of the `payload` value | Replaced boundary |
| --- | --- | --- |
| Siblings | Struct with 1025 uniquely named Int64 fields | Nested struct width |
| Nodes | Struct of 512 lists, each containing a struct with 8 Int64 fields | 5121 logical nodes |
| Metadata | Struct of 10 structs, each containing 850 Int64 fields | 8511 nodes and more than 8 MiB conservative metadata |

Use a 512 MiB execution grant for the complete positive lifecycle. Small
semantic cases use eight input rows, a nonzero selected offset, duplicate
right matches and unmatched outer rows. Complete values and types must agree
through SQL and DataFrame filter/join, bounded delivery, native write,
separate-process reopen and explicit result conversion. Typed zero-row output
and context-drop/final-result-owner release are separate checks. Keep the
original accepted runtime's refusals as observations.

The spill case uses 257 input rows, the same sibling shape, a 1 MiB spill
buffer and a 64 MiB native spill quota. It must report a positive spill-run
count and verify complete values after merging and cleanup; configuration
alone is not activation evidence. Do not silently reduce a frozen shape or
replace a failed path with a count-only assertion. An implementation-driven
protocol correction must preserve the original observation and explain the
change before another candidate execution.

Resource regressions include insufficient grants, unchanged reservations after
failed growth, cancellation during wide traversal, partial construction
cleanup, exponential shared-subtree refusal within the grant, duplicate and
empty names, malformed/cyclic declarations, retained depth refusal and
corrupted encoded messages. Provider tests must exercise first-touch refusal,
retry after admission changes, concurrent initialization and final-owner
release. Existing format/transport negative controls remain in force.

Acceptance requires focused regressions, required default/native formatting,
strict lint and tests, applicable feature/MSRV gates, a fresh installed-wheel
proof and a source-bound portable evidence packet. No Full43, comparative
performance campaign, hosted deployment or package publication is part of
this unit. Item 19 and the broader deep-traversal obligation remain open.

## Local verification and remaining boundaries

All three frozen shapes preserve complete values and types through SQL/DataFrame
operations, bounded delivery, actual native spill, native write/fresh-process
reopen and explicit Arrow IPC/Parquet conversion. Empty and populated exported
Arrow children retain credit through the final owner. The original lifecycle
test methods, fixture bytes and 512 MiB positive grant remain unchanged.

The first public attempt exposed insufficient writer-footer schema credit;
the fix admits the full schema before staging. Subsequent cold-file probes
exposed lazy DType allocation panics at 256 and 512 KiB. Both observations and
the backtrace remain saved. Added tests cover 24 fresh-process reopen cases
across all three shapes and eight grants, requiring ordinary memory diagnostics,
complete values with sufficient admission and zero retained session credit after
close. A separate Rust regression covers legacy/nested/absent statistics,
refusal, retry and final owners with native binary-object support disabled.

| Check | Local observation |
| --- | --- |
| Default workspace | 3,322 tests passed; zero failures or ignores |
| Native workspace | 7,032 tests passed; zero failures; 37 existing opt-in ignores |
| Formatting and strict lint | Default and native checks passed |
| Provider tests and strict lint | All eight selected provider crates passed; Vortex array has 5,776 passing tests and one existing ignore |
| Feature and supported Rust checks | Seven feature checks and both Rust 1.96.0 checks passed |
| Selected public wide-schema tests | Eight passed; zero failures, errors or skips |
| Installed wheel | 405 passed; zero failures, errors or skips; isolated load and uninstall passed |
| Python core | 1,271 discovered cases; 864 reported runs; 60 optional/environment skip records; zero failures/errors |

The [evidence packet](../release/evidence/native-wide-schema-2026-11-09/README.md)
is the review entry point for exact commands, source identities, original
failures and portable verification. Its verifier requires 19 validation roles,
11 selected native regressions, three provider regressions and unchanged frozen
expectations. Two positive and 17 corruption controls exercise that verifier;
their actual outcomes are recorded separately from runtime checks.

Depth 24, serialized declaration/JSON byte bounds, physical IPC validation and
format representation limits remain. Credited schema/container ownership does
not bound whole-process RSS or account for arbitrary extension implementations.
This is local macOS arm64 proof, with no benchmark or performance claim. Vortex
remains native input/output and no external query-engine fallback is introduced.
