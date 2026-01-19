-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cr_ref_test_type_core.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT trim(ref_test_type_stg.test_type_desc) AS test_type_desc,
          trim(ref_test_type_stg.test_sub_type_desc) AS test_sub_type_desc,
          ref_test_type_stg.source_system_code AS source_system_code
   FROM {{ params.param_cr_stage_dataset_name }}.ref_test_type_stg
   WHERE (upper(trim(ref_test_type_stg.test_type_desc)),
          upper(trim(ref_test_type_stg.test_sub_type_desc))) NOT IN
       (SELECT AS STRUCT upper(trim(ref_test_type.test_type_desc)),
                         upper(trim(ref_test_type.test_sub_type_desc))
        FROM {{ params.param_cr_core_dataset_name }}.ref_test_type
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central'))
     AND ref_test_type_stg.dw_last_update_date_time = current_date('US/Central') ) AS a