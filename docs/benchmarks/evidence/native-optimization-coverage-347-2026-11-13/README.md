# Compact 347 Saved-Result Replay (r2)

This r2 bundle preserves the complete saved-result evidence used by the existing offline replay for
the frozen 347-case Parquet cohort. Its `evidence.tar.gz`, `manifest.json`, and consumed evidence
path map are byte-for-byte identical to r1. The archive contains 1,741 consumed evidence members
and seven separately listed provenance members (4,144,945 compressed bytes). Archive names are
generated safe names; the manifest maps each original literal path to its member, byte length, and
SHA-256.

The replay verifies 347 complete comparisons and 12,565 rows. Baseline and current engine clocks are
11.978234339 and 12.065545164 seconds. The replay uses zero query executions and invokes no
comparison engine.

## Verify

Run from this directory:

```sh
PYTHONDONTWRITEBYTECODE=1 python3.13 -B verify.py \
  --repo-root /Users/dylan/.codex/worktrees/native-vector-representations/shardloom-local-repo \
  --result-out verification-original-r2.json
```

`verify.py` checks every archive member name, count, length, and hash without extracting files. It
maps each original requested path to its archive member and invokes the existing
`scripts/parquet_demand_completion_replay.py::replay` implementation. The runtime source revision
remains pinned to `9f4580c723f876f087d3e75fb56153b4b099ef20`. Verification accepts a checkout at a
different commit only when every one of the 29 manifest-listed replay source files matches its
recorded byte length and SHA-256. This proves those listed files match; it makes no whole-repository
equivalence claim. Each standalone result records the verifier script's own byte length and SHA-256.

The bundle replays saved results when the repository verifier sources are available. It does not
include runtime binaries, Parquet inputs, or source data. `runtime-binding.json` and frozen input
identities record paths, sizes, and hashes only; runtime and data payloads are not copied.

`verification.json` records the initial r1 recording replay and passing r2 checks against both the
original bundle directory and a relocated copy. The temporary relocated copy was removed.
`verification-original-r2.json` and `verification-relocated-r2.json` preserve each standalone check.
