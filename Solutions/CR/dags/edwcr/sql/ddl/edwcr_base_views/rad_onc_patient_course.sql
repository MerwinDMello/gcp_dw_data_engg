CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient_course
   OPTIONS(description='Contains information of patient courses in Radiation Oncology')
  AS SELECT
      rad_onc_patient_course.patient_course_sk,
      rad_onc_patient_course.patient_sk,
      rad_onc_patient_course.clinical_status_id,
      rad_onc_patient_course.treatment_intent_type_id,
      rad_onc_patient_course.site_sk,
      rad_onc_patient_course.source_patient_course_id,
      rad_onc_patient_course.course_id_text,
      rad_onc_patient_course.course_start_date_time,
      rad_onc_patient_course.course_session_planned_num,
      rad_onc_patient_course.course_session_delivered_num,
      rad_onc_patient_course.course_session_remaining_num,
      rad_onc_patient_course.dose_delivered_amt,
      rad_onc_patient_course.course_duration_num,
      rad_onc_patient_course.comment_text,
      rad_onc_patient_course.log_id,
      rad_onc_patient_course.run_id,
      rad_onc_patient_course.source_system_code,
      rad_onc_patient_course.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.rad_onc_patient_course
  ;
