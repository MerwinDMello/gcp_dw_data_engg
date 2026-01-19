CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor
   OPTIONS(description='Contains details around the discovery of a tumor and patient.')
  AS SELECT
      cn_patient_tumor.cn_patient_tumor_sid,
      cn_patient_tumor.referral_source_facility_id,
      cn_patient_tumor.nav_status_id,
      cn_patient_tumor.treatment_end_physician_id,
      cn_patient_tumor.treatment_end_facility_id,
      cn_patient_tumor.treatment_end_reason_text,
      cn_patient_tumor.nav_patient_id,
      cn_patient_tumor.tumor_type_id,
      cn_patient_tumor.navigator_id,
      cn_patient_tumor.coid,
      cn_patient_tumor.company_code,
      cn_patient_tumor.electronic_folder_id_text,
      cn_patient_tumor.identification_period_text,
      cn_patient_tumor.referral_date,
      cn_patient_tumor.referring_physician_id,
      cn_patient_tumor.nav_end_reason_text,
      cn_patient_tumor.hashbite_ssk,
      cn_patient_tumor.source_system_code,
      cn_patient_tumor.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_tumor
  ;
