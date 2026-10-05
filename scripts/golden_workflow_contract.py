# SPDX-License-Identifier: Apache-2.0
"""Shared identities for the executable native golden workflows."""

LOCAL_FILE_WORKFLOW_ID = "local_csv_jsonl_to_vortex_ingest_prepared_query_jsonl_csv_output"
SOURCE_FREE_WORKFLOW_ID = "source_free_sql_values_to_local_vortex_output_replay_fidelity"
NATIVE_PRIMITIVE_WORKFLOW_ID = "prepared_native_vortex_count_filter_project_execution_certificates"
GOLDEN_WORKFLOW_IDS = frozenset((
    LOCAL_FILE_WORKFLOW_ID, SOURCE_FREE_WORKFLOW_ID, NATIVE_PRIMITIVE_WORKFLOW_ID,
))
