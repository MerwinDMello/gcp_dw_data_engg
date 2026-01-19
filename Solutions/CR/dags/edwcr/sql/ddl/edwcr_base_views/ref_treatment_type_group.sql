CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_treatment_type_group
   OPTIONS(description='This table contains different codes and descriptions for treatment type groups')
  AS SELECT
      ref_treatment_type_group.treatment_type_group_id,
      ref_treatment_type_group.treatment_type_group_code,
      ref_treatment_type_group.treatment_type_group_desc,
      ref_treatment_type_group.nav_treatment_type_group_desc,
      ref_treatment_type_group.source_system_code,
      ref_treatment_type_group.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_treatment_type_group
  ;
