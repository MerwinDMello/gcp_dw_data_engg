CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cr_patient_diagnosis_detail
   OPTIONS(description='This table contains diagnosis details of patient')
  AS SELECT
      cr_patient_diagnosis_detail.tumor_id,
      cr_patient_diagnosis_detail.cr_patient_id,
      cr_patient_diagnosis_detail.tumor_site_id,
      cr_patient_diagnosis_detail.diagnosis_name_id,
      cr_patient_diagnosis_detail.diagnosis_date,
      cr_patient_diagnosis_detail.diagnose_age_num,
      cr_patient_diagnosis_detail.first_diagnose_year_num,
      cr_patient_diagnosis_detail.source_system_code,
      cr_patient_diagnosis_detail.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cr_patient_diagnosis_detail
  ;
