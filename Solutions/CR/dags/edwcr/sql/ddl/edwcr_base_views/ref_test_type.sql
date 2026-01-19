CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_test_type
   OPTIONS(description='Contains the distinct test types for the patients')
  AS SELECT
      ref_test_type.test_type_id,
      ref_test_type.test_type_desc,
      ref_test_type.test_sub_type_desc,
      ref_test_type.source_system_code,
      ref_test_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_test_type
  ;
