CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_procedure_pathology_result
   OPTIONS(description='Contains the pathology results from a patients surgery or biopsy.')
  AS SELECT
      cn_patient_procedure_pathology_result.cn_patient_proc_pathology_result_sid,
      cn_patient_procedure_pathology_result.cn_patient_procedure_sid,
      cn_patient_procedure_pathology_result.margin_result_id,
      cn_patient_procedure_pathology_result.nav_result_id,
      cn_patient_procedure_pathology_result.oncotype_diagnosis_result_id,
      cn_patient_procedure_pathology_result.navigation_procedure_type_code,
      cn_patient_procedure_pathology_result.pathology_result_date,
      cn_patient_procedure_pathology_result.pathology_result_name,
      cn_patient_procedure_pathology_result.pathology_grade_available_ind,
      cn_patient_procedure_pathology_result.pathology_grade_num,
      cn_patient_procedure_pathology_result.pathology_tumor_size_available_ind,
      cn_patient_procedure_pathology_result.tumor_size_num_text,
      cn_patient_procedure_pathology_result.margin_result_detail_text,
      cn_patient_procedure_pathology_result.sentinel_node_result_code,
      cn_patient_procedure_pathology_result.estrogen_receptor_sw,
      cn_patient_procedure_pathology_result.estrogen_receptor_strength_code,
      cn_patient_procedure_pathology_result.estrogen_receptor_pct_text,
      cn_patient_procedure_pathology_result.progesterone_receptor_sw,
      cn_patient_procedure_pathology_result.progesterone_receptor_strength_code,
      cn_patient_procedure_pathology_result.progesterone_receptor_pct_text,
      cn_patient_procedure_pathology_result.oncotype_diagnosis_score_num,
      cn_patient_procedure_pathology_result.oncotype_diagnosis_risk_text,
      cn_patient_procedure_pathology_result.comment_text,
      cn_patient_procedure_pathology_result.hashbite_ssk,
      cn_patient_procedure_pathology_result.source_system_code,
      cn_patient_procedure_pathology_result.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_procedure_pathology_result
  ;
