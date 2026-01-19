CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient_plan
   OPTIONS(description='Contains information of plans linked to course involved in Radiation Oncology')
  AS SELECT
      rad_onc_patient_plan.patient_plan_sk,
      rad_onc_patient_plan.plan_purpose_sk,
      rad_onc_patient_plan.site_sk,
      rad_onc_patient_plan.source_patient_plan_id,
      rad_onc_patient_plan.patient_course_sk,
      rad_onc_patient_plan.plan_status_id,
      rad_onc_patient_plan.plan_unique_id_text,
      rad_onc_patient_plan.plan_creation_date_time,
      rad_onc_patient_plan.treatment_start_date_time,
      rad_onc_patient_plan.treatment_end_date_time,
      rad_onc_patient_plan.history_user_name,
      rad_onc_patient_plan.history_date_time,
      rad_onc_patient_plan.log_id,
      rad_onc_patient_plan.run_id,
      rad_onc_patient_plan.source_system_code,
      rad_onc_patient_plan.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.rad_onc_patient_plan
  ;
