CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor_pathology_result
   OPTIONS(description='This table contains pathology results of patient tumor')
  AS SELECT
      cr_patient_tumor_pathology_result.tumor_id,
      cr_patient_tumor_pathology_result.cr_patient_id,
      cr_patient_tumor_pathology_result.nodes_examined_id,
      cr_patient_tumor_pathology_result.positive_node_id,
      cr_patient_tumor_pathology_result.source_system_code,
      cr_patient_tumor_pathology_result.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cr_patient_tumor_pathology_result
  ;
