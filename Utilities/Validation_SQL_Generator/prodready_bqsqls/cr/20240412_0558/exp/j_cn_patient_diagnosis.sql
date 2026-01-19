-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_diagnosis.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT stg.cn_patient_diagnosis_sid,
          stg.nav_patient_id,
          stg.tumor_type_id,
          stg.diagnosis_result_id,
          stg.nav_diagnosis_id,
          stg.navigator_id,
          stg.coid,
          'H' AS company_code,
          stg.general_diagnosis_name,
          stg.diagnosis_date,
          rdd.diagnosis_detail_id,
          rs.side_id AS diagnosis_side_id,
          stg.hashbite_ssk,
          'N' AS source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_diagnosis_stg AS stg
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_side AS rs
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rtrim(stg.diagnosisside)) = upper(rtrim(rs.side_desc))
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_diagnosis_detail AS rdd
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rtrim(coalesce(trim(stg.diagnosismetastatic), 'X'))) = upper(rtrim(coalesce(trim(rdd.diagnosis_detail_desc), 'X')))
   AND upper(rtrim(coalesce(trim(stg.diagnosisindicator), 'XX'))) = upper(rtrim(coalesce(trim(rdd.diagnosis_indicator_text), 'XX')))
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_diagnosis.hashbite_ssk) AS hashbite_ssk
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_diagnosis
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) ) AS src