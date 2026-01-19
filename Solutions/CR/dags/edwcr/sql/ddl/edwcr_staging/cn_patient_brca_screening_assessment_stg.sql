CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_brca_screening_assessment_stg (
brca_screening_assessment_sid INT64 NOT NULL
, nav_patient_id NUMERIC(18,0)
, tumor_type_id INT64
, navigator_id INT64
, coid STRING NOT NULL
, company_code STRING NOT NULL
, early_onset_breast_cancer_ind STRING
, ovarian_cancer_history_ind STRING
, two_primary_breast_cancer_ind STRING
, male_breast_cancer_ind STRING
, triple_negative_ind STRING
, ashkenazi_jewish_ind STRING
, two_plus_relative_cancer_ind STRING
, meeting_assmnt_critieria_ind STRING
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
