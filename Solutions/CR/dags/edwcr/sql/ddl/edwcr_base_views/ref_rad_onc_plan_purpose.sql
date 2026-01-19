CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_plan_purpose
   OPTIONS(description='Contains information for radiation oncology plan purpose')
  AS SELECT
      ref_rad_onc_plan_purpose.plan_purpose_sk,
      ref_rad_onc_plan_purpose.plan_purpose_name,
      ref_rad_onc_plan_purpose.source_system_code,
      ref_rad_onc_plan_purpose.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_rad_onc_plan_purpose
  ;
