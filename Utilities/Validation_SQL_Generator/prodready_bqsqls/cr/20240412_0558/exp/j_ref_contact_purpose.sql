-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_ref_contact_purpose.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT contact_purpose_stg.contact_purpose_desc,
          contact_purpose_stg.source_system_code
   FROM {{ params.param_cr_stage_dataset_name }}.contact_purpose_stg
   WHERE upper(trim(contact_purpose_stg.contact_purpose_desc)) NOT IN
       (SELECT upper(trim(ref_contact_purpose.contact_purpose_desc))
        FROM {{ params.param_cr_core_dataset_name }}.ref_contact_purpose
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central'))
     AND contact_purpose_stg.dw_last_update_date_time = current_date('US/Central') ) AS a