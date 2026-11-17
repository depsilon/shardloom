# ShardLoom local parquet-variant patch

This preserves the existing Apache-2.0 `parquet-variant` 59.3.2 package from its
checksum-verified registry archive. No dependency version changes. The archive
omits legal texts and source-revision metadata. `LICENSE` and `NOTICE` retain
the same-release Parquet workspace texts already used by ShardLoom's dependency
license bundle. No source commit is inferred for this published crate.
`upstream-provenance.json` records the original archive, every original member,
modified/added files and the legal-text sources. Local publication is disabled.

## Ordered full validation

The RFC 0044 Variant validation amendment replaces recursive object/list child
validation with borrowed parent continuations. Full metadata and object-name
checks still precede the first child. Objects retain suffix access; lists retain
adjacent-offset bounds. Each child completes before the next offset is read.
Primitive decoding, errors, public constructors, lazy accessors and validated
no-ops keep their existing behavior. Copying an inline borrowed metadata header
allows the parent to retain its validation result without an owned recursive
tree. No unsafe operation or alternate parser is introduced.

Frozen original methods remain test-only references. Shallow complete-value,
error and inherited-panic comparisons cover mutated encodings and competing
malformations. Small-stack tests cover 8,192-level object/list/mixed values,
deep invalid leaves, late sibling failure and borrowed wrapper cleanup.

The upstream integration tests use the complete `variant/` fixture directory
from Apache Parquet testing commit `09f3cdbde45302f0f0c689c950e465e98a9df960`, the
submodule revision already attributed to the 59.3.2 Arrow release by the existing
Parquet provider intake. Every fixture is checked against its Git blob identity.
The separate Apache-2.0 fixture license is preserved and bundled. The empty
`test/parquet-testing/data` directory marker supports the upstream environment
variable convention; tests read its sibling `variant` directory.

Standalone checks use the committed provider lock and the recipe in
`scripts/ci_provider_tests.py`. Native depth guards remain unchanged until
complete memory admission and deep-workflow acceptance. This provider patch
does not certify other recursive Variant operations or grant public deep input.
