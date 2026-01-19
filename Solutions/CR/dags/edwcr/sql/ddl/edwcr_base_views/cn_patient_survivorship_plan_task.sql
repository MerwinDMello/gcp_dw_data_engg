CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_survivorship_plan_task
   OPTIONS(description='Contains all the details behind the tasks associated with a patients surviorship plan.')
  AS SELECT
      cn_patient_survivorship_plan_task.nav_survivorship_plan_task_sid,
      cn_patient_survivorship_plan_task.task_status_id,
      cn_patient_survivorship_plan_task.contact_method_id,
      cn_patient_survivorship_plan_task.nav_patient_id,
      cn_patient_survivorship_plan_task.navigator_id,
      cn_patient_survivorship_plan_task.coid,
      cn_patient_survivorship_plan_task.company_code,
      cn_patient_survivorship_plan_task.task_desc_text,
      cn_patient_survivorship_plan_task.task_resolution_date,
      cn_patient_survivorship_plan_task.task_closed_date,
      cn_patient_survivorship_plan_task.contact_result_text,
      cn_patient_survivorship_plan_task.contact_date,
      cn_patient_survivorship_plan_task.comment_text,
      cn_patient_survivorship_plan_task.hashbite_ssk,
      cn_patient_survivorship_plan_task.source_system_code,
      cn_patient_survivorship_plan_task.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_survivorship_plan_task
  ;
