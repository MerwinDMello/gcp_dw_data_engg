-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_address.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ', ') AS source_string
FROM
  (SELECT cn_patient_address_stg.*
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_address_stg
   WHERE cn_patient_address_stg.nav_patient_id NOT IN
       (SELECT cn_patient_address.nav_patient_id
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_address
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central'))
     AND cn_patient_address_stg.dw_last_update_date_time = current_date('US/Central') ) AS a