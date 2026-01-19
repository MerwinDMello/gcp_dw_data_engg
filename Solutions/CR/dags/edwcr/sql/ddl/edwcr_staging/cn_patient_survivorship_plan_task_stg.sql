CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_survivorship_plan_task_stg (
nav_survivorship_plan_task_sid INT64 NOT NULL
, taskstate STRING
, taskmeasnofcontact STRING
, nav_patient_id NUMERIC(18,0)
, navigator_id INT64
, coid STRING NOT NULL
, company_code STRING NOT NULL
, task_desc_text STRING
, task_resolution_date DATE
, task_closed_date DATE
, contact_result_text STRING
, contact_date DATE
, comment_text STRING
, hashbite_ssk STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
