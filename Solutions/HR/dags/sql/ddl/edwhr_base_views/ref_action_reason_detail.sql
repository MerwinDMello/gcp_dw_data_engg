CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_action_reason_detail AS SELECT
    ref_action_reason_detail.action_reason_text,
    ref_action_reason_detail.mapped_action_reason_text,
    ref_action_reason_detail.action_reason_group_text,
    ref_action_reason_detail.action_reason_sub_group_text,
    ref_action_reason_detail.action_reason_desc,
    ref_action_reason_detail.governance_group_desc,
    ref_action_reason_detail.action_reason_type_desc,
    ref_action_reason_detail.source_system_code,
    ref_action_reason_detail.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_action_reason_detail
;
