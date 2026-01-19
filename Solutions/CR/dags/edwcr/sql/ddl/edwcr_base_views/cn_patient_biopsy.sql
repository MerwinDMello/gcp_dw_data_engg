CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_biopsy
   OPTIONS(description='Contains the details behind a patients biopsy.')
  AS SELECT
      cn_patient_biopsy.cn_patient_biopsy_sid,
      cn_patient_biopsy.core_record_type_id,
      cn_patient_biopsy.med_spcl_physician_id,
      cn_patient_biopsy.referring_physician_id,
      cn_patient_biopsy.biopsy_type_id,
      cn_patient_biopsy.biopsy_result_id,
      cn_patient_biopsy.biopsy_facility_id,
      cn_patient_biopsy.biopsy_site_location_id,
      cn_patient_biopsy.biopsy_physician_specialty_id,
      cn_patient_biopsy.nav_patient_id,
      cn_patient_biopsy.tumor_type_id,
      cn_patient_biopsy.diagnosis_result_id,
      cn_patient_biopsy.nav_diagnosis_id,
      cn_patient_biopsy.navigator_id,
      cn_patient_biopsy.coid,
      cn_patient_biopsy.company_code,
      cn_patient_biopsy.biopsy_date,
      cn_patient_biopsy.biopsy_clip_sw,
      cn_patient_biopsy.biopsy_needle_sw,
      cn_patient_biopsy.general_biopsy_type_text,
      cn_patient_biopsy.comment_text,
      cn_patient_biopsy.hashbite_ssk,
      cn_patient_biopsy.source_system_code,
      cn_patient_biopsy.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_biopsy
  ;
