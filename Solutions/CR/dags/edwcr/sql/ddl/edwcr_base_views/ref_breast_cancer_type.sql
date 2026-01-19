CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_breast_cancer_type
   OPTIONS(description='Contains the Breast Cancer type codes')
  AS SELECT
      ref_breast_cancer_type.breast_cancer_type_id,
      ref_breast_cancer_type.breast_cancer_type_desc,
      ref_breast_cancer_type.source_system_code,
      ref_breast_cancer_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_breast_cancer_type
  ;
