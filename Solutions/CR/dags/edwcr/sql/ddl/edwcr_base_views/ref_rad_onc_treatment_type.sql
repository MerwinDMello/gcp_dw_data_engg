CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_treatment_type
   OPTIONS(description='Contains information for radiation oncology treatment type')
  AS SELECT
      ref_rad_onc_treatment_type.treatment_type_sk,
      ref_rad_onc_treatment_type.treatment_category_desc,
      ref_rad_onc_treatment_type.treatment_type_desc,
      ref_rad_onc_treatment_type.source_system_code,
      ref_rad_onc_treatment_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_rad_onc_treatment_type
  ;
