-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_contact_type_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT a.*
   FROM
     (SELECT NULL AS contact_type_sk,
             trim(format('%11d', ca_server.server_sk)) AS server_sk,
             trim(format('%11d', stg.source_contact_type_id)) AS source_contact_type_id,
             trim(stg.contact_type_name) AS contact_type_name,
             stg.source_create_date_time AS source_create_date_time,
             stg.source_last_update_date_time AS source_last_update_date_time,
             trim(stg.updated_by_3_4_id) AS updated_by_3_4_id,
             'C' AS source_system_code,
             timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_contact_type_stg AS stg
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server ON upper(stg.full_server_nm) = upper(ca_server.server_name)) AS a) AS b