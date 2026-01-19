CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_procedure_pathology_result_stg (
cn_patient_procedure_sid INT64 NOT NULL
, margin_result_id STRING
, nav_result_id STRING
, oncotype_diagnosis_result_id STRING
, navigation_procedure_type_code STRING
, pathology_result_date DATE
, pathology_result_name STRING
, pathology_grade_available_ind STRING
, pathology_grade_num INT64
, pathology_tumor_size_av_ind STRING
, tumor_size_num_text STRING
, margin_result_detail_text STRING
, sentinel_node_result_code STRING
, estrogen_receptor_sw INT64
, estrogen_receptor_st_cd STRING
, estrogen_receptor_pct_text STRING
, progesterone_receptor_sw INT64
, progesterone_receptor_st_cd STRING
, progesterone_receptor_pct_text STRING
, oncotype_diagnosis_score_num STRING
, oncotype_diagnosis_risk_text STRING
, comment_text STRING
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
