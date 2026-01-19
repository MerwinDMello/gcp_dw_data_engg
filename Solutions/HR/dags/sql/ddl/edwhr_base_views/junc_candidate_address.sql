CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.junc_candidate_address AS SELECT
    junc_candidate_address.candidate_sid,
    junc_candidate_address.valid_from_date,
    junc_candidate_address.addr_sid,
    junc_candidate_address.addr_type_code,
    junc_candidate_address.valid_to_date,
    junc_candidate_address.source_system_code,
    junc_candidate_address.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.junc_candidate_address
;
