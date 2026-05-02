# Skill: Benchmarking

## Purpose
Ensure performance claims are reproducible, comparable, and tied to real workloads.

## When to use
Use when making or reviewing any performance claim.

## Rules
- No performance claims without a reproducible benchmark command/setup.
- Separate correctness verification from performance measurement.
- Report dataset shape, scale, hardware/runtime context, and variance.
- Benchmark representative encoded workloads, not only best-case microbenchmarks.
- Treat regressions as release blockers unless explicitly accepted.

## Validation checklist
- [ ] Benchmark command and environment are documented.
- [ ] Input data/profile is described sufficiently to reproduce.
- [ ] Baseline vs. change comparison is clearly reported.
- [ ] Claims match measured results and confidence caveats.
