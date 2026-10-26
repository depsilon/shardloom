<!-- SPDX-License-Identifier: Apache-2.0 -->

# Connected programming acceptance — October 26, 2026

Status: local acceptance passes for active queue item 17. Hosted CI and review
remain separate from this record. This extends the source implementation beyond
the published 1.0.0 package; no new package or release has been published.

## Accepted behavior

SQL, readers, catalog lookups and DataFrame transformations return the same lazy
DataFrame. Named plans preserve immutable binding snapshots and their actual
source owners. Query-local relations, scoped ordinary CTEs and typed scalar
parameters bind into native declarations without executing an intermediate,
rewriting SQL text with values, or converting data through another engine.
Replacing or dropping a named plan affects future bindings; already-bound plans
retain their meaning. Cross-context bindings fail before input effects.

The connected acceptance reads CSV and Parquet, filters and registers a
plan, binds parameterized CTE SQL, joins another DataFrame, applies a reusable
window, observes aggregates during a Vortex write, and reopens the actual output.
Additional tests prove query-local bindings, exact decimal/date/null values,
schema inspection without producer execution, replacement/drop lifetimes,
resource rejection before producer effects, cancellation and retained buffers.
Wide CTE chains exceed the former 64-name limit, ordinary SQL exceeds 256 KiB,
and long pipelines exceed the former 24-level limit under resource admission.
These finite controls do not admit arbitrary unbounded plans.

Shared native scalar definitions drive name/alias, arity, type and null-rule
discovery as well as SQL binding. Every exposed scalar constructor is checked
against independent expected values, including SQL aliases. The generated
[function reference](../reference/native-scalar-functions.md) is checked against
the actual executable catalog. Python constructs expressions; native providers
evaluate them. Existing aggregate, nested-value and window providers remain in
their approved boundaries.

COUNT, SUM, MIN and MAX observations retain their specified plan position and
consume the action's rows without a second input pass. Late producer errors,
cancellation, early stream close and failed publication never report complete
metrics or a successful write. Native data completion and output publication
remain distinct evidence fields.

The optional local ADBC driver is loaded through Apache's independent driver
manager. DB-API uses that standard interface, and notebook SQL attaches to an
explicit existing context. Tests cover exact typed parameters and rebinding,
metadata inspection, catalog snapshots, incremental fetch, retained Arrow owners,
connection/context lifetime, cancellation, late errors and standard error codes.
The [provider review](../dependencies/adbc-local-driver-review.md) records the
FFI boundary, pinned client closure and Apache license/notice intake.

## Local verification

The [portable packet](evidence/native-connected-programming-2026-10-26/manifest.json)
contains complete command receipts, logs, source inventories, frozen artifact
identities, earlier failed attempts, installed-package proof and all Full43 result
payloads. Its [offline verifier](evidence/native-connected-programming-2026-10-26/verify.py)
reads content-addressed objects in memory; it never extracts files or executes
archived code. The independently supplied reference contains all 381 result
rows, and exact result schemas are preserved separately.

| Gate | Result |
| --- | --- |
| Required default workspace formatting and Clippy | Pass |
| Required default workspace tests | 3,261 pass |
| Native workspace tests | 6,128 pass, 35 existing ignored tests |
| Native workspace Clippy | Pass |
| Python discovery and documented first example | 997 tests, 8 explicit environment/optional skips; example passes |
| Clean installed native wheel | 179 integration tests, zero skips; build, install, load and uninstall pass |
| Packaging tests | 24 pass |
| Export/API/function references, parity, front-door, API/schema, CI-gate and documentation checks | Pass |
| License bundle | 439 registry and 8 vendored components; 831 records, 250 unique texts |

The native workspace test pass precedes the final narrow ADBC diagnostic-code
mapping fix. Native Clippy and the clean installed package proof run after that
fix; the new independent-client test proves invalid input maps to the standard
`INVALID_ARGUMENT` status. The earlier wheel attempt failed before installation
because the two new ADBC crates lacked bundled notices. The original failure is
retained, and the complete successful proof follows the verified notice repair.

Python discovery's eight skips cover a diagnostic-only observation build,
optional NumPy/pandas/fastavro cases, and tests requiring a separately selected
installed package. The installed proof requires all its selected native and
adapter cases to execute with zero skips. This is macOS arm64, Python 3.13 local
execution evidence; downloadable Linux client wheels do not constitute Linux
execution proof. Hosted platform checks must report their own outcome.

## Full43 observation

**Full43 engine time: 47.935588458 seconds**, 43 queries executed once each.
All 381 reference rows and all 43 exact schemas match. The previous accepted
observation was 47.889862794 engine seconds; the change is approximately +0.0955%,
within the prospective 5% regression screen. This is a regression observation,
not a causal speedup comparison or competitive claim.

The standard runner used the frozen executable, unchanged Vortex input, unchanged
independent reference, a 24-GiB admission setting and maximum parallelism 12.
The admission setting is not a claim about physical RAM. All other task work and
agents were idle during the query pass. Full hashing and recursive inventories
occurred before or after execution; the existing lightweight concurrency,
storage, deadline, validation and cleanup guards remained active. Cache state
was uncontrolled, with a full input hash before queries and no cache manipulation.
The packet retains complete diagnostic resource and supervision observations.

| Artifact | SHA-256 |
| --- | --- |
| CLI executable | `b309284cfa8c9ba746ab64782e5d2ab9ae5485145b636439a4ba7482cc1e1027` |
| Native Python library | `5b708fa91b12b5951a22bf2b5c35aa7e8f5a5622279ea0d461ca3e7fe74fa76f` |
| Locally tested wheel | `7b6b543d74583007b29306ead987cb99c97157045512a288d268e59c4f7f137f` |
| Vortex input | `95a393d153a8c2e1ffeb5084b2656ac61465c6ef9dad435d032365844f1ac057` |
| Independent reference | `fc5780da415dc33bd06ed0879db07fc040db5dd419e3c37ecde7cd40a315696b` |

From the evidence directory, verify without any original local paths:

```sh
python3 -B verify.py manifest.json packet.tar.xz
python3 -B -m unittest -v test_verify
python3 -B -O -m unittest -v test_verify
```

The corruption controls reject altered source/artifact identities, incomplete
tests or query coverage, changed values/schema/timing, fallback evidence,
rewritten failures, malformed JSON and unsafe or oversized archives.

## Boundaries and next item

Recursive CTE execution, explicit application batch functions and additional
analytical semantics remain queue item 20. Explicit `MATERIALIZED` CTEs fail
until their ownership contract is implemented; ordinary and `NOT MATERIALIZED`
CTEs use bound plan references. A completed `collect()` followed by
`context.from_result()` is the explicit existing materialization boundary.
Correlated repeated observation and distinct-state metrics remain unadmitted.
The local adapters do not claim transactions, remote partitions, bulk parameter
streams, SQL writes or persistent catalogs.

Queue item 18 is universal I/O. The dropped item 15 implementation is not a
dependency and must not be restored unchanged. RFC 0044, native Vortex input and
output, explicit compatibility conversion and no external execution fallback
remain governing contracts. This acceptance closes no CG gate and makes no
production, release or superiority claim.
