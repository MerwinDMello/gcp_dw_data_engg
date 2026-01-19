CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.fact_rad_onc_course_diagnosis
   OPTIONS(description='Contains fact information of Radiation Oncology for a course diagnosis')
  AS SELECT
      fact_rad_onc_course_diagnosis.fact_course_diagnosis_sk,
      fact_rad_onc_course_diagnosis.fact_patient_diagnosis_sk,
      fact_rad_onc_course_diagnosis.patient_course_sk,
      fact_rad_onc_course_diagnosis.diagnosis_code_sk,
      fact_rad_onc_course_diagnosis.site_sk,
      fact_rad_onc_course_diagnosis.source_fact_course_diagnosis_id,
      fact_rad_onc_course_diagnosis.primary_course_ind,
      fact_rad_onc_course_diagnosis.log_id,
      fact_rad_onc_course_diagnosis.run_id,
      fact_rad_onc_course_diagnosis.source_system_code,
      fact_rad_onc_course_diagnosis.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.fact_rad_onc_course_diagnosis
  ;
