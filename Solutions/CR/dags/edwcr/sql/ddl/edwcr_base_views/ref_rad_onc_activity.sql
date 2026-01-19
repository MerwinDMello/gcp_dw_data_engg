CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_activity
   OPTIONS(description='Contains information for radiation oncology activity')
  AS SELECT
      ref_rad_onc_activity.activity_sk,
      ref_rad_onc_activity.activity_object_status_id,
      ref_rad_onc_activity.site_sk,
      ref_rad_onc_activity.source_activity_id,
      ref_rad_onc_activity.activity_category_code_text,
      ref_rad_onc_activity.activity_category_desc,
      ref_rad_onc_activity.activity_code_text,
      ref_rad_onc_activity.activity_desc,
      ref_rad_onc_activity.source_last_modified_date_time,
      ref_rad_onc_activity.log_id,
      ref_rad_onc_activity.run_id,
      ref_rad_onc_activity.effective_start_date_time,
      ref_rad_onc_activity.effective_end_date_time,
      ref_rad_onc_activity.source_system_code,
      ref_rad_onc_activity.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_rad_onc_activity
  ;
