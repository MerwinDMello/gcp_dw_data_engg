CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.cn_patient_procedure_pathology_result
(
  cn_patient_proc_pathology_result_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  cn_patient_procedure_sid INT64 NOT NULL OPTIONS(description='A unique identifier for each record.'),
  margin_result_id INT64 OPTIONS(description='A unique identifier for each margin result.'),
  nav_result_id INT64 OPTIONS(description='A unique identifier for each result.'),
  oncotype_diagnosis_result_id INT64 OPTIONS(description='A unique identifier for the oncotype result.'),
  navigation_procedure_type_code STRING OPTIONS(description='The different procedure type - surgical or biopsy.'),
  pathology_result_date DATE OPTIONS(description='The date pathology result was given.'),
  pathology_result_name STRING OPTIONS(description='The name of the pathology result.'),
  pathology_grade_available_ind STRING OPTIONS(description='Indicates the pathology grade is available.'),
  pathology_grade_num INT64 OPTIONS(description='The grade given on the pathology report.'),
  pathology_tumor_size_available_ind STRING OPTIONS(description='Indicates if the tumor size is available from the pathology report.'),
  tumor_size_num_text STRING OPTIONS(description='The size of the tumor.'),
  margin_result_detail_text STRING OPTIONS(description='Specific to surgical margins, document as either Positive or Negative'),
  sentinel_node_result_code STRING OPTIONS(description='Indicates if the cancer has spread beyond a primary tumor into the lymphatic system.'),
  estrogen_receptor_sw INT64 OPTIONS(description='Classifies tumor estrogen receptors as either positive or negative'),
  estrogen_receptor_strength_code STRING OPTIONS(description='Classifies tumor estrogen receptors as either positive or negative'),
  estrogen_receptor_pct_text STRING OPTIONS(description='The percentage of estrogen from the receptor.'),
  progesterone_receptor_sw INT64 OPTIONS(description='Classifies tumor proestrogen receptors as either positive or negative'),
  progesterone_receptor_strength_code STRING OPTIONS(description='Classifies tumor proestrogen receptors as either positive or negative'),
  progesterone_receptor_pct_text STRING OPTIONS(description='The percentage of proestrogen from the receptor.'),
  oncotype_diagnosis_score_num INT64 OPTIONS(description='The score from the oncotype.'),
  oncotype_diagnosis_risk_text STRING OPTIONS(description='The risk identified based on the oncotype result.'),
  comment_text STRING OPTIONS(description='A free text field that adds insight into the module of where the comments was entered.'),
  hashbite_ssk STRING OPTIONS(description='A unique guid that ties the record to the source system.'),
  source_system_code STRING NOT NULL OPTIONS(description='A one character code indicating the specific source system from which the data originated.'),
  dw_last_update_date_time DATETIME NOT NULL OPTIONS(description='Timestamp of update or load of this record to the Enterprise Data Warehouse.')
)
CLUSTER BY cn_patient_procedure_sid
OPTIONS(
  description='Contains the pathology results from a patients surgery or biopsy.'
);
