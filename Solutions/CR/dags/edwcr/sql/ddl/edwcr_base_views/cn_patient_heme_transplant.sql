CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_heme_transplant
   OPTIONS(description='Contains treatment regimen details for Hematology patient')
  AS SELECT
      cn_patient_heme_transplant.cn_patient_heme_sid,
      cn_patient_heme_transplant.transplant_type_id,
      cn_patient_heme_transplant.cellular_therapy_status_id,
      cn_patient_heme_transplant.nav_patient_id,
      cn_patient_heme_transplant.tumor_type_id,
      cn_patient_heme_transplant.diagnosis_result_id,
      cn_patient_heme_transplant.nav_diagnosis_id,
      cn_patient_heme_transplant.navigator_id,
      cn_patient_heme_transplant.med_spcl_physician_id,
      cn_patient_heme_transplant.coid,
      cn_patient_heme_transplant.company_code,
      cn_patient_heme_transplant.cellular_therapy_status_date,
      cn_patient_heme_transplant.cellular_therapy_comment_text,
      cn_patient_heme_transplant.transfer_date,
      cn_patient_heme_transplant.transplant_date,
      cn_patient_heme_transplant.transplant_comment_text,
      cn_patient_heme_transplant.hashbite_ssk,
      cn_patient_heme_transplant.source_system_code,
      cn_patient_heme_transplant.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_heme_transplant
  ;
