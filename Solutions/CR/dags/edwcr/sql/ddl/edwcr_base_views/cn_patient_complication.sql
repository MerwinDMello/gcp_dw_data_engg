CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_complication
   OPTIONS(description='Contains the details associated with any complications a patient had.')
  AS SELECT
      cn_patient_complication.cn_patient_complication_sid,
      cn_patient_complication.core_record_type_id,
      cn_patient_complication.therapy_type_id,
      cn_patient_complication.outcome_result_id,
      cn_patient_complication.nav_patient_id,
      cn_patient_complication.tumor_type_id,
      cn_patient_complication.diagnosis_result_id,
      cn_patient_complication.nav_diagnosis_id,
      cn_patient_complication.navigator_id,
      cn_patient_complication.coid,
      cn_patient_complication.company_code,
      cn_patient_complication.complication_date,
      cn_patient_complication.treatment_stopped_ind,
      cn_patient_complication.complication_text,
      cn_patient_complication.specific_complication_text,
      cn_patient_complication.comment_text,
      cn_patient_complication.hashbite_ssk,
      cn_patient_complication.source_system_code,
      cn_patient_complication.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_complication
  ;
