-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_surv_plan_task.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT stg.nav_survivorship_plan_task_sid,
          ref2.status_id,
          ref1.contact_method_id,
          stg.nav_patient_id,
          stg.navigator_id,
          stg.coid,
          stg.company_code,
          stg.task_desc_text,
          stg.task_resolution_date,
          stg.task_closed_date,
          stg.contact_result_text,
          stg.contact_date,
          stg.comment_text,
          stg.hashbite_ssk,
          stg.source_system_code,
          stg.dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_survivorship_plan_task_stg AS stg
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_contact_method AS ref1
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rtrim(stg.taskmeasnofcontact)) = upper(rtrim(ref1.contact_method_desc))
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_status AS ref2
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rtrim(stg.taskstate)) = upper(rtrim(ref2.status_desc))
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_survivorship_plan_task.hashbite_ssk) AS hashbite_ssk
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_survivorship_plan_task
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) ) AS a