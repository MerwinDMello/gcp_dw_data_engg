CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cancer_patient_outcome_timeliness_stg (
cancer_patient_tumor_driver_sk NUMERIC(18,0) NOT NULL
, cancer_patient_driver_sk NUMERIC(18,0) NOT NULL
, cancer_tumor_driver_sk INT64 NOT NULL
, coid STRING NOT NULL
, company_code STRING NOT NULL
, length_to_chemo_day_num INT64
, length_to_hormone_day_num INT64
, length_to_immuno_day_num INT64
, length_to_surgery_day_num INT64
, length_to_radiation_day_num INT64
, length_to_transplant_day_num INT64
, length_to_first_treatment_day_num INT64
, first_surgery_chemo_elapsed_day_num INT64
, first_chemo_surgery_elapsed_day_num INT64
, radiation_elapsed_day_num INT64
, length_to_surgery_last_contact_day_num INT64
, length_to_diagnosis_last_contact_day_num INT64
, length_to_diagnosis_last_contact_mth_num INT64
, last_contact_date DATE
, admission_date DATE
, rln10_concordant_ind STRING
, rln12_concordant_ind STRING
, act_concordant_ind STRING
, bcs_concordant_ind STRING
, bcsrt_concordant_ind STRING
, cbrrt_concordant_ind STRING
, cerct_concrodant_ind STRING
, cerrt_concrodant_ind STRING
, endctrt_concordant_ind STRING
, endlrc_concordant_ind STRING
, g15rlnc_concordant_ind STRING
, lct_concordant_ind STRING
, ht_concordant_ind STRING
, lnosurg_concordant_ind STRING
, mac_concordant_ind STRING
, mastrt_concordant_ind STRING
, nb_concordant_ind STRING
, ovsal_concordant_ind STRING
, recrtct_concordance_ind STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
