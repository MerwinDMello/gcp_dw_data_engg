-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_referring_physician_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT p.*
   FROM
     (SELECT 1 AS referring_physician_sk,
             trim(format('%11d', a.contact_type_sk)) AS contact_type_sk,
             trim(format('%11d', b.contact_sk)) AS contact_sk,
             trim(format('%11d', p_0.patient_hosp_sk)) AS patient_hosp_sk,
             trim(format('%11d', s.server_sk)) AS server_sk,
             trim(format('%11d', stg.referringphysid)) AS source_referring_physician_id,
             stg.createdate AS source_create_date_time,
             stg.lastupdate AS source_last_update_date_time,
             trim(stg.updatedby) AS updated_by_3_4_id,
             trim('C') AS source_system_code,
             timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_referringphysicians_stg AS stg
      LEFT OUTER JOIN
        (SELECT c.contact_type_sk,
                c.source_contact_type_id,
                s_0.server_name
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact_type AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s_0
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(format('%11d', c.server_sk)) = trim(format('%11d', s_0.server_sk))) AS a ON trim(format('%11d', stg.contacttypeid)) = trim(format('%11d', a.source_contact_type_id))
      AND upper(trim(stg.full_server_nm)) = upper(trim(a.server_name))
      LEFT OUTER JOIN
        (SELECT c.contact_sk,
                c.source_contact_id,
                s_0.server_name
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_contact AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s_0
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(format('%11d', c.server_sk)) = trim(format('%11d', s_0.server_sk))) AS b ON trim(format('%11d', stg.contactid)) = trim(format('%11d', b.source_contact_id))
      AND upper(trim(stg.full_server_nm)) = upper(trim(b.server_name))
      LEFT OUTER JOIN
        (SELECT c.patient_hosp_sk,
                c.source_patient_hosp_id,
                s_0.server_name
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient_hosp AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s_0
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(format('%11d', c.server_sk)) = trim(format('%11d', s_0.server_sk))) AS p_0 ON trim(format('%11d', stg.hospitalizationid)) = trim(format('%11d', p_0.source_patient_hosp_id))
      AND upper(trim(stg.full_server_nm)) = upper(trim(p_0.server_name))
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(trim(stg.full_server_nm)) = upper(trim(s.server_name))) AS p) AS q