-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_diagnosis_list_sts_detail_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    count(*)
  FROM
    (
      SELECT DISTINCT
          a.*
        FROM
          (
            SELECT DISTINCT
                ca.diagnosis_list_sk AS diagnosis_list_sk,
                wrk.server_sk AS server_sk,
                wrk.data_version_code AS data_version_code,
                wrk.sts_dup_sk AS sts_dup_sk,
                wrk.sts_harvest_code AS sts_harvest_code,
                wrk.sts_short_term_text AS sts_short_term_text,
                wrk.sts_base_term_num AS sts_base_term_num,
                wrk.source_system_code AS source_system_code,
                wrk.dw_last_update_date_time AS dw_last_update_date_time
              FROM
                (
                  SELECT DISTINCT
                      CAST(NULL as INT64) AS diagnosis_list_sk,
                      csrv.server_sk AS server_sk,
                      stg.id AS source_diagnosis_list_id,
                      '2.3/2.5' AS data_version_code,
                      rd.source_sts_dup_id AS sts_dup_sk,
                      stg.stsid250 AS sts_harvest_code,
                      stg.ststerm250 AS sts_short_term_text,
                      CAST(NULL as INT64) AS sts_base_term_num,
                      'C' AS source_system_code,
                      timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
                    FROM
                      `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_diagnosislist_stg AS stg
                      LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ref_ca_sts_dup AS rd ON stg.stsdup250 = rd.source_sts_dup_id
                      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS csrv ON upper(stg.full_server_nm) = upper(csrv.server_name)
                    WHERE stg.stsdup250 IS NOT NULL
                     OR stg.stsid250 IS NOT NULL
                     OR stg.ststerm250 IS NOT NULL
                  UNION DISTINCT
                  SELECT DISTINCT
                      CAST(NULL as INT64) AS diagnosis_list_sk,
                      csrv.server_sk AS server_sk,
                      stg.id AS source_diagnosis_list_id,
                      '3.0' AS data_version_code,
                      rd.source_sts_dup_id AS sts_dup_sk,
                      stg.stsid30 AS sts_harvest_code,
                      stg.ststerm30 AS sts_short_term_text,
                      stg.stsbaseterm AS sts_base_term_num,
                      'C' AS source_system_code,
                      timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
                    FROM
                      `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_diagnosislist_stg AS stg
                      LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ref_ca_sts_dup AS rd ON stg.stsdup30 = rd.source_sts_dup_id
                      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS csrv ON upper(stg.full_server_nm) = upper(csrv.server_name)
                    WHERE stg.stsdup30 IS NOT NULL
                     OR stg.stsid30 IS NOT NULL
                     OR stg.ststerm30 IS NOT NULL
                     AND stg.stsbaseterm IS NOT NULL
                  UNION DISTINCT
                  SELECT DISTINCT
                      CAST(NULL as INT64) AS diagnosis_list_sk,
                      csrv.server_sk AS server_sk,
                      stg.id AS source_diagnosis_list_id,
                      '3.22' AS data_version_code,
                      rd.source_sts_dup_id AS sts_dup_sk,
                      stg.stsid32 AS sts_harvest_code,
                      stg.ststerm32 AS sts_short_term_text,
                      stg.stsbaseterm32 AS sts_base_term_num,
                      'C' AS source_system_code,
                      timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
                    FROM
                      `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_diagnosislist_stg AS stg
                      LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ref_ca_sts_dup AS rd ON stg.stsdup32 = rd.source_sts_dup_id
                      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS csrv ON upper(stg.full_server_nm) = upper(csrv.server_name)
                    WHERE stg.stsdup32 IS NOT NULL
                     OR stg.stsid32 IS NOT NULL
                     OR stg.ststerm32 IS NOT NULL
                     AND stg.stsbaseterm32 IS NOT NULL
                  UNION DISTINCT
                  SELECT DISTINCT
                      CAST(NULL as INT64) AS diagnosis_list_sk,
                      csrv.server_sk AS server_sk,
                      stg.id AS source_diagnosis_list_id,
                      '3.3' AS data_version_code,
                      rd.source_sts_dup_id AS sts_dup_sk,
                      stg.stsid33 AS sts_harvest_code,
                      stg.ststerm33 AS sts_short_term_text,
                      stg.stsbaseterm33 AS sts_base_term_num,
                      'C' AS source_system_code,
                      timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
                    FROM
                      `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_diagnosislist_stg AS stg
                      LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ref_ca_sts_dup AS rd ON stg.stsdup33 = rd.source_sts_dup_id
                      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS csrv ON upper(stg.full_server_nm) = upper(csrv.server_name)
                    WHERE stg.stsdup33 IS NOT NULL
                     OR stg.stsid33 IS NOT NULL
                     OR stg.ststerm33 IS NOT NULL
                     AND stg.stsbaseterm33 IS NOT NULL
                ) AS wrk
                INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_diagnosis_list AS ca ON ca.source_diagnosis_list_id = wrk.source_diagnosis_list_id
                 AND ca.server_sk = wrk.server_sk
                LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_diagnosis_list_sts_detail AS ch ON ch.diagnosis_list_sk = ca.diagnosis_list_sk
              WHERE ch.diagnosis_list_sk IS NULL
          ) AS a
    ) AS b
;
