CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_toxicity_assessment_type
   OPTIONS(description='Contains information for radiation oncology assessment type')
  AS SELECT
      ref_rad_onc_toxicity_assessment_type.toxicity_assessment_type_sk,
      ref_rad_onc_toxicity_assessment_type.toxicity_assessment_type_desc,
      ref_rad_onc_toxicity_assessment_type.source_system_code,
      ref_rad_onc_toxicity_assessment_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_rad_onc_toxicity_assessment_type
  ;
