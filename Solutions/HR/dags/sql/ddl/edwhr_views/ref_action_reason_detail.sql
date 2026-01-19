/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_action_reason_detail AS SELECT
      a.action_reason_text,
      a.mapped_action_reason_text,
      a.action_reason_group_text,
      a.action_reason_sub_group_text,
      a.action_reason_desc,
      a.governance_group_desc,
      a.action_reason_type_desc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_action_reason_detail AS a
  ;

