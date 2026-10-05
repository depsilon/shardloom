# Benchmark Publishing Runbook

ShardLoom’s public benchmark page is a handoff to [ClickBench](https://benchmark.clickhouse.com/).
The repository does not publish a comparative dashboard or regenerate benchmark mirrors. The
retired dashboard bundle and mirror files must remain absent. Static absence checks document that
boundary; they do not authorize a benchmark run, publication, or performance claim.

## Check the public-surface boundary

From the repository root, run the claim gate against its default canonical manifest path:

```sh
python scripts/check_benchmark_publication_claim_gate.py
```

The checker reports whether the canonical public bundle and all retired mirrors are absent. It
does not execute workloads, build the website, publish files, or prove runtime behavior or
performance. Its report is a static public-surface absence check; performance, production,
superiority, parity, and publication claims remain false.

The doctor and front-door commands are equivalent entry points to that same check:

```sh
python scripts/check_benchmark_publish_doctor.py
python scripts/check_front_door_benchmark_publication.py
```

Each accepts `--repo-root`, `--manifest`, and `--output`. Supplying a custom manifest path does
not create publication authorization; only the canonical site path can pass the public-surface
check, and the retired mirrors must still be absent. The checkers write their report to standard
output unless `--output` is supplied.

## Inspect a local benchmark report

The artifact-completeness checker can inspect a local `public_native_benchmark.v1` report:

```sh
python scripts/check_benchmark_artifact_completeness.py \
  --manifest path/to/public-native-benchmark.json
```

This validates report structure and completeness only. It does not run benchmarks, admit a report
to the public site, or permit a promotion step; no promotion tool is part of the current workflow.
The report is emitted to standard output unless `--output` is supplied.

For local workload execution, follow
[`benchmarks/traditional_analytics/README.md`](../../benchmarks/traditional_analytics/README.md).
Local results remain local evidence and do not change the public ClickBench handoff.

## Publication and claim boundary

None of the commands above publishes files or rebuilds the website. Do not restore the retired
dashboard bundle, mirror-generation process, benchmark profiles, or promotion workflow. A passing
absence check establishes only that the repository’s static publication surface follows the
current boundary. A complete local report establishes only that it passed the completeness
checker. Neither result is runtime correctness, reproducible performance evidence, or permission
to make comparative or production claims.
