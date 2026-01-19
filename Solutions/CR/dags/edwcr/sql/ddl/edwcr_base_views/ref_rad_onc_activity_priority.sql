CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_activity_priority
   OPTIONS(description='Contains information for radiation oncology activity priority')
  AS SELECT
      ref_rad_onc_activity_priority.activity_priority_sk,
      ref_rad_onc_activity_priority.activity_priority_desc,
      ref_rad_onc_activity_priority.source_system_code,
      ref_rad_onc_activity_priority.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_rad_onc_activity_priority
  ;
