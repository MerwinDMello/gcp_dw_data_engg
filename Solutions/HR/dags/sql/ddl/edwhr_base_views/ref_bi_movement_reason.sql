CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_bi_movement_reason AS SELECT
    ref_bi_movement_reason.movement_reason_id,
    ref_bi_movement_reason.external_candidate_sw,
    ref_bi_movement_reason.lob_change_sw,
    ref_bi_movement_reason.division_change_sw,
    ref_bi_movement_reason.location_change_sw,
    ref_bi_movement_reason.department_change_sw,
    ref_bi_movement_reason.position_change_sw,
    ref_bi_movement_reason.status_change_sw,
    ref_bi_movement_reason.action_desc,
    ref_bi_movement_reason.reason_desc,
    ref_bi_movement_reason.source_system_code,
    ref_bi_movement_reason.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_bi_movement_reason
;
