create or replace view `{{ params.param_hr_base_views_dataset_name }}.ref_patient_care_position`
AS SELECT
    ref_patient_care_position.job_code,
    ref_patient_care_position.job_title_text,
    ref_patient_care_position.job_code_desc,
    ref_patient_care_position.source_system_code,
    ref_patient_care_position.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.ref_patient_care_position;