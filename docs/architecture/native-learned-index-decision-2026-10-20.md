<!-- SPDX-License-Identifier: Apache-2.0 -->

# Finite weighted native search — October 20, 2026

Status: the bounded predictor is **not retained**. Both large targets improve
engine time in both frozen cohorts, but two controls exceed the 5% engine
regression limit in cohort A. Cohort B passes. Both cohorts were required, so
the experiment has a complete negative retention decision without establishing
a repeated regression. Experimental runtime and observation code is removed.

This completes the finite row-15 investigation under `PERF-10.learned_indexes`,
not the broader owner. The [conditional sequence](native-conditional-investigations-2026-10-19.md)
continues with `PERF-03.regions`; eligible pre-writer idle attribution still
requires a separate revisit before required breadth. RFC 0044, all 58 roadmap
substeps and CG-1 through CG-23 retain their independent gates.

## Mechanism and prospective controls

The [intake](native-learned-index-intake-2026-10-20.md) measures actual search
work in the resident weighted replacement sampler. Its
[candidate protocol](native-learned-index-2026-10-20.md) is preserved unchanged.
One private model borrows the existing credited candidate population, calibrates
the specified monotonic Float64 predictor, and uses exact lower-bound correction
inside its certified interval. Every execution rebuilds the model. Counts,
prediction error and available credit govern admission; fixture names do not.
Discarded payloads do not perform row searches and do not calibrate a model.

The model uses 48 bytes of credited metadata, below the frozen 128-byte ceiling,
and polls cancellation every 256 calibration entries. Failed calibration,
insufficient free metadata credit and ineligible populations retain ordinary
exact native search. No second CDF, sorted copy, persistent index, dependency,
external engine, source cache or new public option is introduced.

Pinned Vortex 0.85.0 provides exact sorted search, but the sampler's cumulative
weights live inside its existing credited candidate records. The protocol's
`implement_shardloom_kernel` decision therefore uses a borrowed private helper;
Vortex-native source/result construction and normal certificates remain intact.
This is original implementation informed by the protocol's cited papers.

Both roles use the same public preparation and complete JSONL collection
harness: eight inputs, exact input schema/order, seed 7, one projected source ID,
replacement, one CPU and a 64-MiB grant. Each fresh prepared workflow makes two
calls. Each cohort has one unscored warmup and seven scored samples per role and
cell. Cohort B reverses cell and paired-role order. Observation counters are
disabled symmetrically during measurement. Complete preparation, serialization,
release and teardown controls remain in the raw evidence alongside engine time.
The captured host is ARM64 macOS 27.0 with ten logical CPUs and 16 GiB of physical
RAM, distinct from the one-CPU/64-MiB operation grant. Builds use Rust 1.99.0,
the optimized release profile and `release-user-surfaces`. Every workflow
constructs fresh native source state; no cold-cache or CPU-isolation claim is made.

## Results and decision

The table reports the median sum of two engine execution clocks per workflow,
in milliseconds; each median contains seven scored workflows. The percentage
is candidate minus baseline divided by baseline. These are finite comparative
observations, not a Full43 pass or a whole-engine performance claim.

| Cell | A baseline → candidate (ms) | A change | B baseline → candidate (ms) | B change |
| --- | ---: | ---: | ---: | ---: |
| Uniform target | 48.204627 → 44.994626 | −6.66% | 48.229465 → 44.683502 | −7.35% |
| Periodic target | 47.923547 → 45.634910 | −4.78% | 47.351285 → 44.754042 | −5.49% |
| Shifted | 46.180173 → 47.868212 | +3.66% | 45.897331 → 46.987957 | +2.38% |
| Hot tail | 42.964836 → 44.487377 | +3.54% | 42.806211 → 44.565293 | +4.11% |
| Few draws | 38.779417 → 41.032749 | **+5.81%** | 39.207542 → 40.833251 | +4.15% |
| Tiny | 0.127541 → 0.134083 | **+5.13%** | 0.127957 → 0.126292 | −1.30% |
| Unweighted | 12.975498 → 13.282286 | +2.36% | 12.596334 → 12.685998 | +0.71% |
| Rounded ties | 0.110749 → 0.110458 | −0.26% | 0.112874 → 0.109749 | −2.77% |

Both targets exceed the required 2% improvement in both cohorts. The few-draw
control also fails its separate complete-workflow bound in A. All other
complete-workflow controls and every memory gate pass. These control failures
prevent retention even though the predictor is bypassed in the failing cells.
The experiment does not isolate their cause. Host-load and owned-process
observations are preserved; ordinary desktop activity is not CPU isolation.

All 16 preflight workflows and 256 comparison workflows pass exact source,
ordered values, logical types, certificate and ownership checks: 544 calls,
13,370,568 source-row checks and 22,291,964 output-row checks. These counts
include repeated verification of the sixteen shared content-addressed witnesses.
Of the comparison workflows, 224 are scored and 32 are warmups. Every teardown
leaves zero reserved credits and no denied reservations; paired peaks are equal.
No fallback or external engine is invoked.

Preflight confirms a model on every uniform and periodic call, with radii 1 and
5. Their correction searches make 196,607 and 327,672 comparisons respectively,
against 1,114,112 ordinary comparisons per baseline call. Shifted and hot-tail
calibration rejects after ten visits. Few-draw, tiny, unweighted and rounded-tie
cells do not calibrate. Eight candidate tests cover exact/neighbor/gap boundaries,
admitted rounded duplicates, tiny/large totals, refusal, credit ownership,
cancellation and unwind; three common observer tests also pass.

The targets cannot compensate for failed controls. No cohort is replaced,
threshold relaxed, favorable distribution promoted or unchanged failed candidate
rerun. A materially different mechanism would need a new prospective protocol.

## Evidence and reproduction

The [intake packet](../benchmarks/evidence/learned-intake-packet-825.json.xz)
preserves its sixteen calls and fourteen preliminary guards, including six
failed setup/verification attempts and their corrections. Its protocol explains
the exact schema-spelling and compiler-fixture corrections. Those queries were
not rerun to repair the verifier.

The [comparison packet](../benchmarks/evidence/learned-cost-packet-826.json.xz)
and [manifest](../benchmarks/evidence/learned-cost-packet-826.manifest.json)
preserve all 272 workflows, complete witnesses, both frozen source versions,
seventeen successful guards, fresh optimized compiler records, exact executable
identities and original invocation order. Every local compiled dependency has
an individual pre-build hash: 1,546 for baseline and 1,547 for candidate.
The shared protocol, harness, observer, reference and guard bytes match.
The comparison packet SHA-256 is
`e9bbacab272fd8f8ccfc4c810b9825dc7a2c1e6ac936ba3ca9df8a3c4a7fa41c`.

Before any comparison query, both copied test executable basenames were corrected
so the unchanged native-work guard recognizes them. Qualification records prove
unchanged compiled bytes and inode; no guard was weakened and no timing was
repeated. All comparison setup and query guards pass.

The [standalone replay](../benchmarks/evidence/package_learned_cost_826.py)
reconstructs all results, checks source closure against Git plus archived changes,
recomputes the original medians and returns `retention_gate_passed=false` without
an engine executable. Run it with `--verify` pointing to the packet,
`--scratch-root` pointing to local unsynced storage, and `--repository` pointing
to a checkout containing commit `8721506c487b873cc0598ead0b78b4e22339ff99`.
The packet contains the executed comparison driver for reproduction in a fresh
workspace; historical output paths must not be reused.

## Restoration boundary

Three modified source files are restored byte-for-byte to the pre-experiment
commit, and the predictor, observer and comparison-harness modules are removed.
The intake and comparison sources remain in their compact evidence. No
experimental selector remains in the normal test inventory.

Fresh formatting, strict workspace/native Clippy, all 3,235 workspace tests and
all 3,041 native tests pass. The native suite retains its 30 existing opt-in/manual
ignores; the two suites overlap. The optimized native test executable is freshly
compiled from the restored source. All 3,024 frozen Rust/Cargo/JSON/Python/script
inputs remain unchanged across the five checks. The
[restoration packet](../benchmarks/evidence/learned-closeout-827.json.xz),
[manifest](../benchmarks/evidence/learned-closeout-827.manifest.json) and
[replay helper](../benchmarks/evidence/package_learned_closeout_827.py) retain
the commands, full logs, source checks, public-runtime bridge and scoped cleanup.
After complete replay and closed-handle/identity checks, the 288 temporary result
files and two comparison executables are retired. Replay succeeds again after
retirement; current public runtime artifacts are unchanged. No executable backup
or generated-dataset rollback archive is retained.

Normal public CLI/Python runtime artifacts from the preceding accepted baseline
remain unchanged, with all 2,222 of their frozen source files matching the restored
tree. This attempt adds no public capability or release claim.
Candidate public-package, Public39, Full43 and modular integration acceptance
is not attempted because its prerequisite retention gate failed. Previous
baseline acceptance remains historical evidence and is not relabeled as fresh.
