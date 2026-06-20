<!-- SPDX-License-Identifier: Apache-2.0 -->

## Summary

<!-- Describe the change and why it is needed. -->

## Contribution Route

- [ ] I have the right to submit this contribution.
- [ ] I understand outside contributions require maintainer approval of the
      contribution-rights path before acceptance.
- [ ] I understand the current required signoff/CLA/DCO state: the draft
      `CLA.md` is a future route, DCO remains inactive, and No external CLA
      Assistant is active.

## Contribution And Provenance

- [ ] This contribution does not include copied implementation code from GPL,
      AGPL, SSPL, BUSL, proprietary, source-available, or unknown-license
      sources.
- [ ] AI/Codex-assisted content, if any, was reviewed for originality,
      correctness, license compatibility, and appropriate tests.

## No-Fallback And Dependency Check

- [ ] This change does not add Spark, DataFusion, DuckDB, Polars, Velox, or
      another external query engine as a ShardLoom runtime fallback dependency.
- [ ] New or changed dependencies, if any, have license, provenance,
      architecture, and no-fallback review.

## Security, Release, And RFC Impact

- [ ] Security impact was reviewed, or this change has no security impact.
- [ ] Package/release/public-claim impact was reviewed, or this change has no
      package/release/public-claim impact.
- [ ] RFC or architecture documentation impact was reviewed, or this change has
      no RFC/architecture impact.

## Claim Boundary

- [ ] This PR does not claim production readiness, public package availability,
      performance superiority, Spark replacement, broad SQL/DataFrame support,
      object-store/lakehouse support, or Foundry/platform support without
      attached evidence.

## Tests Run

<!-- List exact commands, or explain why a check was not applicable. -->

- [ ] Focused checks, for example `python3 scripts/run_focused_checks.py --profile <profile> ...`
- [ ] `cargo fmt --all -- --check`
- [ ] `cargo test --workspace --all-targets`
- [ ] `python scripts/check_contribution_governance.py`

## Reviewer State

<!-- Maintainer/reviewer records approval, requested changes, blocked state, or escalation. -->

## Notes

<!-- Include dependency, license, provenance, benchmark, or no-fallback impacts. -->
