<!-- SPDX-License-Identifier: Apache-2.0 -->

# First 10 Minutes Smoke Snapshot

This snapshot records the expected shape of the local dry-run transcript. Local
paths, elapsed times, and exact build output vary by machine.

```text
schema_version: shardloom.release_dry_run_proof.v1
proof_status: passed
publication_attempted: false
tag_created: false
secrets_required: false
external_runtime_dependencies_added: false
fallback_engine_dependency_added: false
public_package_release_claim_allowed: false
local_python_user_surface_quickstart_performed: true
local_python_result_and_evidence_printed: true
local_python_unsupported_path_evidence_printed: true
generated_output_proof_distinct_from_no_dataset_smoke: true
generated_source_user_rows_runtime_performed: true
generated_source_range_runtime_performed: true
prepared_native_benchmark_smoke_performed: false
benchmark_smoke_required_for_package_release: false
benchmark_smoke_status: skipped_not_required_for_package_release
provenance_dry_run_performed: true
sbom_checksum_manifest_generated: true
clean_conda_env_install_status: passed | skipped_tool_missing
clean_conda_env_install_required: false
steps:
  - build_cli_binary -> 0
  - build_python_artifacts -> 0
  - create_clean_venv -> 0
  - install_local_wheel_clean_venv -> 0
  - wheel_import_and_client_smoke -> 0
      fallback_attempted=False
      capabilities_command=capabilities
  - create_clean_conda_env -> 0, when mamba/conda/micromamba is available
  - install_local_wheel_clean_conda -> 0, when clean Conda proof runs
  - conda_wheel_import_and_client_smoke -> 0, when clean Conda proof runs
  - cli_status_json -> 0
  - cli_capabilities_json -> 0
  - example_local_python_smoke -> 0
      fallback attempted: False
  - generated_source_user_rows_local_output_runtime -> 0
      generated_source_kind=user_rows
      generated_source_row_count=2
      output_io_performed=true
      generated_source_certificate_status=present
      output_native_io_certificate_status=certified_local_file_sink
      fallback_attempted=False
      external_engine_invoked=False
      claim_gate_status=fixture_smoke_only
  - generated_source_range_local_output_runtime -> 0
      generated_source_kind=range
      generated_source_row_count=8
      output_io_performed=true
      generated_source_certificate_status=present
      output_native_io_certificate_status=certified_local_file_sink
      fallback_attempted=False
      external_engine_invoked=False
      claim_gate_status=fixture_smoke_only
  - release_provenance_dry_run -> 0
```

Generate the live transcript with:

```powershell
python scripts\release_dry_run_proof.py --rows 64 --iterations 1
```

The generated transcript is intentionally written under `target/` and is not
committed. It is local proof evidence, not a public release artifact.
