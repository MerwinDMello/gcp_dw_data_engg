CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_cr_treatment_type
   OPTIONS(description='This table contains different codes and descriptions for treatments')
  AS SELECT
      ref_cr_treatment_type.treatment_type_id,
      ref_cr_treatment_type.treatment_type_code,
      ref_cr_treatment_type.treatment_type_desc,
      ref_cr_treatment_type.treatment_group_id,
      ref_cr_treatment_type.source_system_code,
      ref_cr_treatment_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_cr_treatment_type
  ;
