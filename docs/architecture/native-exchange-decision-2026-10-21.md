<!-- SPDX-License-Identifier: Apache-2.0 -->

# Finite native exchange decision — October 21, 2026

Status: **reject the frozen fragment-transport mechanism**. Both complete
cohorts pass correctness, native representation, downstream execution and
cleanup, but both fail the aggregate benefit gate against every Arrow control
and the wide-text controls against compressed Arrow. The private transport,
service, receiver bridge and experiment drivers are removed from the active
source. Their complete implementation remains at Git commit
`6e3419bc5f36d0ec41d381420d255c77ef533409`.

This closes the finite row-21 investigation. It does not close any of the four
broader `NATIVE-ENCODED-EXCHANGE` substeps or admit a public remote service.
The [conditional sequence](native-conditional-investigations-2026-10-19.md)
continues with the two authorized
[follow-ups](native-investigation-followups-2026-10-20.md), then the 37 required
substeps. RFC 0044 and CG-1 through CG-23 retain their separate evidence gates.

## Evaluated mechanism

The [prospective plan](native-exchange-experiment-plan-2026-10-20.md),
[protocol prerequisite](native-encoded-exchange-protocol.md) and
[immutable freeze](../benchmarks/evidence/native-exchange-control-2026-10-20/protocol-freeze.json)
remain unchanged historical inputs. The candidate sends ordered slices of the
pinned Vortex `MessageEncoder` parts without assembling a whole message at the
sender. It uses low-level Flight `DoExchange`, mandatory mutual TLS, per-request
authorization and a bounded native receiving owner. This avoids one explicit
assembly operation; it does not eliminate provider, Protobuf, TLS or kernel
copies, share process pointers, or make the whole-source receiver streaming.

All five roles use one optimized executable and the same frozen immutable
inputs, security, resource policy and native consumer. The controls are ordinary
assembled Vortex IPC and standard Arrow Flight with no compression, LZ4 or Zstd.
Arrow is an explicit transport representation, never an execution fallback.
Thirteen fixtures cover empty/tiny, nullable/all-NULL, constant, three dictionary
domains, run ends, bit packing, wide text, nested and sliced/shared inputs.
Each receiver executes three distinct native projections in order `0,1,2,1`
after the sender exits. Repeated execution must execute again and preserve
complete values, types, order and certificates.

Each cohort contains all 13 cases, five roles and five samples: 325 workflows.
The second cohort reverses case and role order as frozen. All 650 workflows use
executable SHA-256
`c1560dd4c10fe126be1d5268f22f162534aa92621f4e240af463ba1f29cb5f0b`.
The accepted build freeze binds 2,003 repository inputs, all 26 fixture payloads,
the protocol/control identities and 126 external runtime/helper inputs. Source
and runtime identities are checked before and after both cohorts.

## Results and decision

| Frozen requirement | Cohort 1 | Cohort 2 |
| --- | --- | --- |
| Complete values, types, ordering, encodings, native execution and lifecycle | Pass, all 325 workflows | Pass, all 325 workflows |
| All thirteen ordinary Vortex IPC workflow controls | Pass | Pass |
| Four nonbenefiting cases against uncompressed Arrow | Pass | Pass |
| Four nonbenefiting cases against Arrow LZ4 | Fail: wide text | Fail: wide text |
| Four nonbenefiting cases against Arrow Zstd | Fail: wide text | Fail: wide text |
| All thirteen ordinary Vortex IPC engine controls | Pass | Pass |
| Nine-target aggregate benefit and bootstrap bound against each Arrow role | Fail against all three roles | Fail against all three roles |
| At least two distinct winning encoded families | Pass: constants and dictionaries | Pass: constants and dictionaries |

For each Arrow role, both the aggregate benefit threshold and its bootstrap
bound fail in both cohorts. The constant and nested fixtures satisfy the
family-level win rule in both cohorts; they cannot override the aggregate or
control failures. The full 13-case by four-control comparison tables, every
paired sample, all engine clocks and the separately defined diagnostic clocks
remain in the raw evidence. No cohort is replaced, gate relaxed, favorable
subset promoted or unchanged mechanism rerun.

The experiment establishes a negative decision for this finite mechanism.
It does not establish that all native exchange is unhelpful, isolate the cause
of every cost difference, or supply a whole-engine/Full43 result. Host and
process observations are retained; ordinary desktop activity is not proof of
CPU isolation. Native credits and sampled RSS are distinct from unmeasured
provider/TLS/kernel allocations and copies.

## Correctness and failure evidence

Before scoring, registered release tests pass 65 complete producer/transport/
receiver/native-consumer workflows and two client workflows. The latter exercise
an ordinary Flight SQL client over the finite allowed SQL operation and explicit
native client selection. They are not arbitrary SQL or general client support.
The final protocol suite discovers and executes 41 unique tests, including real
mutual TLS, per-request rights, canonical framing, strict-native refusal,
bounded ownership, cancellation, deadlines, replay and response faults. Four
additional process workflows interrupt a partial transfer by killing either
peer, reject a stale epoch after restart, and complete a fresh transfer.
Killed processes have OS reclamation evidence, not invented zero-credit reports.

All successful owned processes drain; the scored cohorts retire all sixteen
private key files. Complete output and resource checks are replayed against the
frozen authority. The checker also refuses 24 specifically corrupted reports,
and all seven gate-arithmetic tests pass. Before timing, formatting, strict
feature/workspace Clippy and all 3,235 default workspace tests pass.
These checks overlap and must not be summed as unique test coverage.

The first optimized preflight exposed a private source-preparation difference:
Vortex 0.85 derives run-end sortedness statistics in debug construction, while
the optimized path did not. The experiment was corrected to derive and charge
those statistics for every transport role before encoding. A focused test
checks the absent-statistics case, denied credit, cancellation and exact frozen
message bytes. The complete final correctness checks ran before either scored
cohort. Frozen fixtures, protocol and thresholds did not change. All failed
setup, compilation, test and diagnostic attempts remain in the packet.

## Evidence, restoration and remaining scope

The [decision record](../benchmarks/evidence/native-exchange-2026-10-21/decision.json)
and [compact results](../benchmarks/evidence/native-exchange-2026-10-21/results.json.xz)
retain 232 original members, including 86 guarded receipts and 21 failed
attempts. The [replay](../benchmarks/evidence/native-exchange-2026-10-21/verify.py)
checks every member, all frozen source bytes against Git, the final protocol
inventory, complete workflow checks, negative report cases and both original
cost decisions without an engine executable. Four pure verification files are
preserved byte-for-byte from the scored source; no runtime is retained just to
recompute evidence. See the
[reproduction instructions](../benchmarks/evidence/native-exchange-2026-10-21/README.md).

The three modified production files are restored exactly to `02aa39f3`, and all
34 files added for the private experiment are removed from active runtime/test
locations. The accepted local IPC boundary, opt-in Arrow representation checks,
dependency intake and independent fixture recipes from that baseline remain.
The [restoration checks](../benchmarks/evidence/native-exchange-2026-10-21/postchecks.json)
and [cleanup receipt](../benchmarks/evidence/native-exchange-2026-10-21/cleanup.json)
record source equality, fresh validation and retirement of generated artifacts.
After removal, formatting and strict workspace/feature Clippy pass. The fresh
workspace suite passes all 3,235 tests; the feature-enabled native suite passes
3,069 tests with 31 existing manual/opt-in tests ignored. These suites overlap.
The restoration packet verifies all 1,969 remaining frozen source inputs against
the accepted prerequisite baseline and preserves sixteen public certificate sets.
Git holds the candidate source; no executable or generated-dataset rollback
backup is retained.

Public package, Full43 and hosted integration acceptance is not attempted for
this rejected candidate. Existing acceptance remains historical evidence.
A materially different native exchange mechanism needs a new prospective
protocol. General protocol/ownership/client obligations, broad remote-service
support and all competitive claims remain open.
