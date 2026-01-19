-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/exp/j_cdm_adhoc_ca_diagnosis_list_detail_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT count(*)
FROM
  (SELECT DISTINCT a.*
   FROM
     (SELECT DISTINCT ca.diagnosis_list_sk AS diagnosis_list_sk,
                      wrk.server_sk AS server_sk,
                      wrk.diagnosis_list_detail_measure_id AS diagnosis_list_detail_measure_id,
                      wrk.diagnosis_list_detail_measure_value_text AS diagnosis_list_detail_measure_value_text,
                      wrk.diagnosis_list_detail_measure_value_num AS diagnosis_list_detail_measure_value_num,
                      wrk.source_system_code AS source_system_code,
                      wrk.dw_last_update_date_time AS dw_last_update_date_time
      FROM
        (SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                         ser.server_sk AS server_sk,
                         stg.id AS source_diagnosis_list_id,
                         '226' AS diagnosis_list_detail_measure_id,
                         CAST(NULL AS INT64) AS diagnosis_list_detail_measure_value_text,
                         stg.kingdomcode AS diagnosis_list_detail_measure_value_num,
                         'C' AS source_system_code,
                         timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_diagnosislist_stg AS stg
         INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS ser ON upper(stg.full_server_nm) = upper(ser.server_name)
         WHERE stg.kingdomcode IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        ser.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '227' AS diagnosis_list_detail_measure_id,
                                        CAST(`hca-hin-dev-cur-clinical`.bqutil_fns.cw_td_normalize_number(stg.kingdom) AS INT64) AS diagnosis_list_detail_measure_value_text,
                                        CAST(NULL AS INT64) AS diagnosis_list_detail_measure_value_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_diagnosislist_stg AS stg
         INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS ser ON upper(stg.full_server_nm) = upper(ser.server_name)
         WHERE stg.kingdom IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        ser.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '228' AS diagnosis_list_detail_measure_id,
                                        CAST(`hca-hin-dev-cur-clinical`.bqutil_fns.cw_td_normalize_number(stg.phylum) AS INT64) AS diagnosis_list_detail_measure_value_text,
                                        CAST(NULL AS INT64) AS diagnosis_list_detail_measure_value_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_diagnosislist_stg AS stg
         INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS ser ON upper(stg.full_server_nm) = upper(ser.server_name)
         WHERE stg.phylum IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        ser.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '229' AS diagnosis_list_detail_measure_id,
                                        CAST(`hca-hin-dev-cur-clinical`.bqutil_fns.cw_td_normalize_number(stg.ipccc) AS INT64) AS diagnosis_list_detail_measure_value_text,
                                        CAST(NULL AS INT64) AS diagnosis_list_detail_measure_value_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_diagnosislist_stg AS stg
         INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS ser ON upper(stg.full_server_nm) = upper(ser.server_name)
         WHERE stg.ipccc IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        ser.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '230' AS diagnosis_list_detail_measure_id,
                                        CAST(NULL AS INT64) AS diagnosis_list_detail_measure_value_text,
                                        stg.funddx AS diagnosis_list_detail_measure_value_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_diagnosislist_stg AS stg
         INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS ser ON upper(stg.full_server_nm) = upper(ser.server_name)
         WHERE stg.funddx IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        ser.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '231' AS diagnosis_list_detail_measure_id,
                                        CAST(NULL AS INT64) AS diagnosis_list_detail_measure_value_text,
                                        stg.pc4funddx AS diagnosis_list_detail_measure_value_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_diagnosislist_stg AS stg
         INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS ser ON upper(stg.full_server_nm) = upper(ser.server_name)
         WHERE stg.pc4funddx IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        ser.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '232' AS diagnosis_list_detail_measure_id,
                                        CAST(NULL AS INT64) AS diagnosis_list_detail_measure_value_text,
                                        stg.funddx32 AS diagnosis_list_detail_measure_value_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_diagnosislist_stg AS stg
         INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS ser ON upper(stg.full_server_nm) = upper(ser.server_name)
         WHERE stg.funddx32 IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        ser.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '233' AS diagnosis_list_detail_measure_id,
                                        CAST(NULL AS INT64) AS diagnosis_list_detail_measure_value_text,
                                        stg.funddx33 AS diagnosis_list_detail_measure_value_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM `hca-hin-dev-cur-clinical`.edwcdm_staging.cardioaccess_diagnosislist_stg AS stg
         INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_server AS ser ON upper(stg.full_server_nm) = upper(ser.server_name)
         WHERE stg.funddx33 IS NOT NULL ) AS wrk
      INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_diagnosis_list AS ca ON ca.source_diagnosis_list_id = wrk.source_diagnosis_list_id
      AND ca.server_sk = wrk.server_sk
      LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm.ca_diagnosis_list_detail AS ch ON ch.diagnosis_list_sk = ca.diagnosis_list_sk
      WHERE ch.diagnosis_list_sk IS NULL ) AS a) AS b