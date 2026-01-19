-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_ref_diagnosis_detail.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT trim(ref_diagnosis_detail_stg.diagnosis_detail_desc) AS diagnosis_detail_desc,
          trim(ref_diagnosis_detail_stg.diagnosis_indicator_text) AS diagnosis_indicator_text,
          ref_diagnosis_detail_stg.source_system_code AS source_system_code
   FROM {{ params.param_cr_stage_dataset_name }}.ref_diagnosis_detail_stg
   WHERE (upper(trim(ref_diagnosis_detail_stg.diagnosis_detail_desc)),
          upper(trim(ref_diagnosis_detail_stg.diagnosis_indicator_text))) NOT IN
       (SELECT AS STRUCT upper(trim(ref_diagnosis_detail.diagnosis_detail_desc)),
                         upper(trim(ref_diagnosis_detail.diagnosis_indicator_text))
        FROM {{ params.param_cr_core_dataset_name }}.ref_diagnosis_detail
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central'))
     AND ref_diagnosis_detail_stg.dw_last_update_date_time = current_date('US/Central') ) AS a