CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_diagnosis
   OPTIONS(description='Contains free text fields related to the diagnosis. Sourced from the patient daignosis table on SQL Server.')
  AS SELECT
      cn_patient_diagnosis.cn_patient_diagnosis_sid,
      cn_patient_diagnosis.nav_patient_id,
      cn_patient_diagnosis.tumor_type_id,
      cn_patient_diagnosis.diagnosis_result_id,
      cn_patient_diagnosis.nav_diagnosis_id,
      cn_patient_diagnosis.navigator_id,
      cn_patient_diagnosis.diagnosis_detail_id,
      cn_patient_diagnosis.coid,
      cn_patient_diagnosis.company_code,
      cn_patient_diagnosis.general_diagnosis_name,
      cn_patient_diagnosis.diagnosis_date,
      cn_patient_diagnosis.diagnosis_side_id,
      cn_patient_diagnosis.hashbite_ssk,
      cn_patient_diagnosis.source_system_code,
      cn_patient_diagnosis.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_diagnosis
  ;
