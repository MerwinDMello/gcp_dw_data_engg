-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_patient_complication_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT p.*
   FROM
     (SELECT 1 AS patient_complication_sk,
             trim(format('%11d', ca_server.server_sk)) AS server_sk,
             trim(format('%11d', a.patient_case_sk)) AS patient_case_sk,
             trim(format('%11d', stg.compuniqueid)) AS source_complication_unique_id,
             trim(format('%11d', stg.complicationid)) AS complication_id,
             trim(stg.complicationothsp) AS complication_other_text,
             trim(format('%11d', stg.sort)) AS source_sort_num,
             stg.createdate AS source_create_date_time,
             stg.lastupdate AS source_last_update_date_time,
             trim(stg.updateby) AS updated_by_3_4_id,
             trim('C') AS source_system_code,
             timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_complications_stg AS stg
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(trim(stg.full_server_nm)) = upper(trim(ca_server.server_name))
      LEFT OUTER JOIN
        (SELECT c.patient_case_sk,
                c.source_patient_case_num,
                s.server_name
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient_case AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(format('%11d', c.server_sk)) = trim(format('%11d', s.server_sk))) AS a ON upper(trim(stg.full_server_nm)) = upper(trim(a.server_name))
      AND trim(format('%11d', stg.casenumber)) = trim(format('%11d', a.source_patient_case_num))) AS p) AS q