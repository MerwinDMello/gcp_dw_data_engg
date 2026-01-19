-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_post_operation_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT p.*
   FROM
     (SELECT 1 AS post_operation_sk,
             trim(format('%11d', ca_server.server_sk)) AS server_sk,
             trim(format('%11d', a.patient_case_sk)) AS patient_case_sk,
             trim(format('%11d', stg.postopid)) AS source_post_operation_id,
             trim(format('%11d', stg.intubate)) AS intubate_id,
             stg.intubatedt AS intubate_date_time,
             stg.extubatedt AS extubate_date_time,
             trim(format('%11d', stg.extubinor)) AS extubate_in_or_id,
             trim(format('%11d', stg.reintubate)) AS reintubate_id,
             stg.finextubdt AS final_extubate_date_time,
             trim(format('%11d', stg.reopaftropinadm)) AS reoperation_after_operation_id,
             stg.createdate AS source_create_date_time,
             stg.lastupdate AS source_last_update_date_time,
             trim(stg.updateby) AS updated_by_3_4_id,
             trim('C') AS source_system_code,
             stg.dw_last_update_date_time AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_postopdata_stg AS stg
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server ON upper(trim(stg.full_server_nm)) = upper(trim(ca_server.server_name))
      LEFT OUTER JOIN
        (SELECT c.patient_case_sk,
                c.source_patient_case_num,
                s.server_name
         FROM `hca-hin-dev-cur-clinical`.edwcdm.ca_patient_case AS c
         INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS s ON trim(format('%11d', c.server_sk)) = trim(format('%11d', s.server_sk))) AS a ON upper(trim(stg.full_server_nm)) = upper(trim(a.server_name))
      AND trim(format('%11d', stg.casenumber)) = trim(format('%11d', a.source_patient_case_num))) AS p) AS q