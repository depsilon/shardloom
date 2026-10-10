def raw_family($p):
  ($p|length) >= 5 and
  (["public","direct","full43","batches","formats","streaming","growth","input_pressure","typed"]|index($p[0])) != null and
  (["raw_envelopes","raw_logs"]|index($p[1])) != null and $p[3] == "envelope";

reduce inputs as $event (
  {schema_version:null,status:null,source_identity:null,binary_sha256:null,source_commit:null,
   counts:{public_cases:0,public_rows:0,direct_cases:0,direct_rows:0,
     streaming_cases:0,growth_cases:0,typed_cases:0,batch_checks:0,format_checks:0,
     full43_runs:0,source_checks:0,source_fresh_checks:0,source_reused_checks:0,source_assets:0,
     typed_value_files:0,typed_schemas:0,typed_artifacts:0,typed_float_files:0,typed_float_values:0,
     typed_protocol_traces:0,typed_output_files:0,typed_denials:0,typed_timestamp_certificates:0,
     typed_stream_certificates:0,streaming_value_files:0,streaming_protocol_traces:0,
     streaming_pressure_files:0,streaming_pressure_rows:0,saved_pressure_files:0,saved_pressure_rows:0,
     growth_value_files:0,growth_artifacts:0,growth_range_values:0,
     pressure_controls:0,pressure_closed:0,pressure_ended:0,pressure_denied:0,pressure_output_rows:0,
     unified_routes:0,nested_unified_routes:0},
   envelopes:{public:0,direct:0,full43:0,batches:0,formats:0,streaming:0,growth:0,input_pressure:0,typed:0},
   fallback_headers:{public:0,direct:0,full43:0,batches:0,formats:0,streaming:0,growth:0,input_pressure:0,typed:0},
   claim_boundaries:{},stream_terminals:[],full43_complete:null,full43_values:null,
   fresh_public:null,historical_union:null,public_families:[],pressure_grants:{},
   invalid_checks:0,invalid_envelopes:0,fallback_violations:0,invalid_routes:0,
   resource_report_status:null,resource_declarations:0,resource_origins:0,resource_no_rss_claims:0,
   resource_declaration_failures:0,
   last_schema:null,last_pressure_file:null,last_pressure_row:null,
   pair_path:null,pair_key:null,pair_value:null,pair_key_set:false,pair_value_set:false};
  if ($event|length) != 2 then . else
    $event[0] as $p | $event[1] as $v |
    if $p == ["schema_version"] then .schema_version = $v
    elif $p == ["status"] then .status = $v
    elif $p == ["resource_report_conformance","status"] then .resource_report_status = $v
    elif $p == ["source_snapshot","source_identity_sha256"] then .source_identity = $v
    elif $p == ["build","binary_sha256"] then .binary_sha256 = $v
    elif $p == ["build","source_commit"] then .source_commit = $v
    elif ($p|length) == 5 and $p[1:3] == ["summary","cases"] then
      if ($p[0] == "public" or $p[0] == "direct") and $p[4] == "name" then .counts[($p[0] + "_cases")] += 1
      elif ($p[0] == "public" or $p[0] == "direct") and $p[4] == "complete_rows_verified" then .counts[($p[0] + "_rows")] += $v
      elif $p[0] == "streaming" and $p[4] == "status" then .counts.streaming_cases += 1 | if $v != "passed" then .invalid_checks += 1 else . end
      elif ($p[0] == "public" or $p[0] == "direct") and $p[4] == "passed" and $v != true then .invalid_checks += 1
      else . end
    elif ($p|length) == 4 and ($p[0] == "typed" or $p[0] == "growth") and $p[1] == "cases" and $p[3] == "status" then
      .counts[($p[0] + "_cases")] += 1 | if $v != "passed" then .invalid_checks += 1 else . end
    elif ($p|length) == 5 and ($p[0] == "batches" or $p[0] == "formats") and $p[1:3] == ["summary","checks"] and $p[4] == "passed" then
      .counts[(if $p[0] == "batches" then "batch_checks" else "format_checks" end)] += 1 | if $v != true then .invalid_checks += 1 else . end
    elif ($p|length) == 5 and $p[0:2] == ["source_checks","checks"] and $p[3:5] == ["receipt_value","status"] then
      .counts.source_checks += 1 | .counts.source_fresh_checks += 1 |
      if $v != "passed" then .invalid_checks += 1 else . end
    elif ($p|length) == 3 and $p[0] == "frozen_source_assets" and $p[2] == "original_sha256" then .counts.source_assets += 1
    elif $p == ["full43","summary","complete"] then .full43_complete = $v
    elif $p == ["full43","summary","full_result_validation"] then .full43_values = $v
    elif ($p|length) == 5 and $p[0:3] == ["full43","summary","records"] and $p[4] == "passed" then
      .counts.full43_runs += 1 | if $v != true then .invalid_checks += 1 else . end
    elif ($p|length) == 4 and ($p[0] == "typed" or $p[0] == "growth") and $p[1] == "complete_values" and $p[3] == "sha256" then .counts[($p[0] + "_value_files")] += 1
    elif ($p|length) == 4 and $p[0:2] == ["typed","artifacts"] and $p[3] == "sha256" then .counts.typed_artifacts += 1
    elif ($p|length) == 4 and $p[0:2] == ["typed","stored_output_files"] and $p[3] == "sha256" then .counts.typed_output_files += 1
    elif ($p|length) == 5 and $p[0:2] == ["typed","protocol_traces"] and $p[3:] == ["trace","sha256"] then .counts.typed_protocol_traces += 1
    elif ($p|length) == 4 and $p[0:2] == ["typed","float_bit_proofs"] and $p[3] == "complete_bitwise_values" then .counts.typed_float_files += 1 | .counts.typed_float_values += $v
    elif $p == ["typed","denial_count"] then .counts.typed_denials = $v
    elif $p == ["typed","timestamp_prepare_certificates"] then .counts.typed_timestamp_certificates = $v
    elif $p == ["typed","stream_completion_certificates"] then .counts.typed_stream_certificates = $v
    elif ($p|length) == 4 and $p[0:2] == ["streaming","exact_small_values"] and $p[3] == "sha256" then .counts.streaming_value_files += 1
    elif ($p|length) == 4 and $p[0:2] == ["streaming","protocol_traces"] and $p[3] == "sha256" then .counts.streaming_protocol_traces += 1
    elif ($p|length) == 4 and $p[0:2] == ["streaming","pressure_complete_value_proofs"] and $p[3] == "complete_rows_reopened" then .counts.streaming_pressure_files += 1 | .counts.streaming_pressure_rows += $v
    elif ($p|length) == 4 and $p[0:2] == ["growth","wide_artifacts"] and $p[3] == "sha256" then .counts.growth_artifacts += 1
    elif $p == ["growth","range","every_value_verified"] then .counts.growth_range_values = $v
    elif ($p|length) == 6 and $p[0:2] == ["input_pressure","controls"] and $p[3:5] == ["summary","value"] then
      if $p[5] == "status" then .counts.pressure_controls += 1 | if $v != "passed" then .invalid_checks += 1 else . end
      elif $p[5] == "producer_closed" and $v == true then .counts.pressure_closed += 1
      elif $p[5] == "producer_ended" and $v == true then .counts.pressure_ended += 1
      elif $p[5] == "expected_memory_denial" and $v == true then .counts.pressure_denied += 1
      elif $p[5] == "complete_output_rows_verified" then .counts.pressure_output_rows += $v
      else . end
    elif ($p|length) == 6 and $p[0:2] == ["input_pressure","controls"] and $p[3:] == ["protocol","value","memory_grant_bytes"] then .pressure_grants[$p[2]] = $v
    elif $p == ["public_family_execution","fresh_execution"] then .fresh_public = $v
    elif $p == ["public_family_execution","historical_union_repair_used"] then .historical_union = $v
    elif ($p|length) == 3 and $p[0:2] == ["public_family_execution","families"] then .public_families += [$v]
    elif ($p|length) == 2 and $p[0] == "claim_boundaries" then .claim_boundaries[$p[1]] = $v
    elif ($p|length) == 3 and $p[0:2] == ["claim_boundaries","single_use_stream_terminals"] then .stream_terminals += [$v]
    else . end |
    if ($p|length) >= 4 and $p[0:2] == ["typed","schema_proofs"] and .last_schema != $p[2] then .counts.typed_schemas += 1 | .last_schema = $p[2] else . end |
    if ($p|length) >= 4 and $p[0:2] == ["streaming","complete_pressure_rows"] then
      (if .last_pressure_file != $p[2] then .counts.saved_pressure_files += 1 | .last_pressure_file = $p[2] else . end) |
      if .last_pressure_row != $p[0:4] then .counts.saved_pressure_rows += 1 | .last_pressure_row = $p[0:4] else . end
    else . end |
    if raw_family($p) then
      if $p[4:] == ["status"] then
        .envelopes[$p[0]] += 1 | if (["success","error","unsupported"]|index($v)) == null then .invalid_envelopes += 1 else . end
      elif $p[4:] == ["fallback","attempted"] then
        .fallback_headers[$p[0]] += 1 | if $v != false then .fallback_violations += 1 else . end
      elif ($p|length) >= 7 and ($p|index("fields")) != null and ($p[-1] == "key" or $p[-1] == "value") then
        (if .pair_path != $p[:-1] then .pair_path = $p[:-1] | .pair_key_set = false | .pair_value_set = false else . end) |
        (if $p[-1] == "key" then .pair_key = $v | .pair_key_set = true else .pair_value = $v | .pair_value_set = true end) |
        if .pair_key_set and .pair_value_set then
          .pair_key as $key | .pair_value as $value |
          (if $key == "execution_resource_configuration_status" then
             (if $p[4:-2] == ["fields"] then .resource_declarations += 1 else . end) |
             if $value != "explicit_validated" then .resource_declaration_failures += 1 else . end
           elif $key == "execution_resource_memory_origin" or $key == "execution_resource_parallelism_origin" then
             (if $p[4:-2] == ["fields"] then .resource_origins += 1 else . end) |
             if (["execution_call","context","session","environment","platform"]|index($value)) == null then .resource_declaration_failures += 1 else . end
           elif $key == "execution_resource_whole_process_memory_limit_enforced" then
             (if $p[4:-2] == ["fields"] then .resource_no_rss_claims += 1 else . end) |
             if $value != "false" then .resource_declaration_failures += 1 else . end
           else . end) |
          (if ($key|type) == "string" and ($key|test("(fallback_attempted|external_engine_invoked|external_query_engine_invoked|fallback_execution_allowed)$")) and $value != false and $value != "false" then .fallback_violations += 1 else . end) |
          if $p[0] == "full43" and $key == "public_workflow_native_vortex_plan_route_family" then
            if $p[4:-2] == ["fields"] then
              if $value == "native_vortex_unified_plan" then .counts.unified_routes += 1 else .invalid_routes += 1 end
            elif $p[4:-2] == ["result","fields"] then
              if $value == "native_vortex_unified_plan" then .counts.nested_unified_routes += 1 else .invalid_routes += 1 end
            else . end
          else . end
        else . end
      else . end
    else . end
  end
) | del(.last_schema,.last_pressure_file,.last_pressure_row,.pair_path,.pair_key,.pair_value,.pair_key_set,.pair_value_set)
