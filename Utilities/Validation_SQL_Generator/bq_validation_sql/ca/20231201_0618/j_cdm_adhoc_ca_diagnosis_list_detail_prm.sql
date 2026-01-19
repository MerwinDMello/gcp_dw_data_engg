##########################
## Variable Declaration ##
##########################

BEGIN

DECLARE difference, control2_difference, control3_difference, src_rec_count, tgt_rec_count int64 DEFAULT 0;

DECLARE control2_src_rowcount, control2_tgt_rowcount, control3_src_rowcount, control3_tgt_rowcount NUMERIC DEFAULT 0;

DECLARE srctableid, tolerance_percent INT64;

DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;

DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

SET current_ts = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET srctableid = NULL;

SET sourcesysnm = ''; -- This needs to be added

SET srcdataset_id = (SELECT arr[offset(1)] FROM (SELECT SPLIT({{ params.param_ca_stage_dataset_name }} , '.') AS arr));

SET srctablename = concat(srcdataset_id, '.','' ); -- This needs to be added

SET tgtdataset_id = (SELECT arr[offset(1)] FROM (SELECT SPLIT({{ params.param_ca_core_dataset_name }} , '.') AS arr));

SET tgttablename =concat(tgtdataset_id, '.', @p_targettable_name);

SET audit_type ='RECORD_COUNT';

SET tableload_start_time = @p_tableload_start_time;

SET tableload_end_time = @p_tableload_end_time;

SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);

SET job_name = @p_job_name;

SET audit_time = current_ts;

SET tolerance_percent = 0;

SET
(src_rec_count) = 
(
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
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosislist_stg AS stg
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS ser
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(ser.server_name)
         WHERE stg.kingdomcode IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        ser.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '227' AS diagnosis_list_detail_measure_id,
                                        CAST({{ params.param_clinical_bqutil_fns_dataset_name }}.cw_td_normalize_number(stg.kingdom) AS INT64) AS diagnosis_list_detail_measure_value_text,
                                        CAST(NULL AS INT64) AS diagnosis_list_detail_measure_value_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosislist_stg AS stg
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS ser
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(ser.server_name)
         WHERE stg.kingdom IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        ser.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '228' AS diagnosis_list_detail_measure_id,
                                        CAST({{ params.param_clinical_bqutil_fns_dataset_name }}.cw_td_normalize_number(stg.phylum) AS INT64) AS diagnosis_list_detail_measure_value_text,
                                        CAST(NULL AS INT64) AS diagnosis_list_detail_measure_value_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosislist_stg AS stg
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS ser
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(ser.server_name)
         WHERE stg.phylum IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        ser.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '229' AS diagnosis_list_detail_measure_id,
                                        CAST({{ params.param_clinical_bqutil_fns_dataset_name }}.cw_td_normalize_number(stg.ipccc) AS INT64) AS diagnosis_list_detail_measure_value_text,
                                        CAST(NULL AS INT64) AS diagnosis_list_detail_measure_value_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosislist_stg AS stg
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS ser
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(ser.server_name)
         WHERE stg.ipccc IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        ser.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '230' AS diagnosis_list_detail_measure_id,
                                        CAST(NULL AS INT64) AS diagnosis_list_detail_measure_value_text,
                                        stg.funddx AS diagnosis_list_detail_measure_value_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosislist_stg AS stg
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS ser
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(ser.server_name)
         WHERE stg.funddx IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        ser.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '231' AS diagnosis_list_detail_measure_id,
                                        CAST(NULL AS INT64) AS diagnosis_list_detail_measure_value_text,
                                        stg.pc4funddx AS diagnosis_list_detail_measure_value_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosislist_stg AS stg
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS ser
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(ser.server_name)
         WHERE stg.pc4funddx IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        ser.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '232' AS diagnosis_list_detail_measure_id,
                                        CAST(NULL AS INT64) AS diagnosis_list_detail_measure_value_text,
                                        stg.funddx32 AS diagnosis_list_detail_measure_value_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosislist_stg AS stg
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS ser
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(ser.server_name)
         WHERE stg.funddx32 IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        ser.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '233' AS diagnosis_list_detail_measure_id,
                                        CAST(NULL AS INT64) AS diagnosis_list_detail_measure_value_text,
                                        stg.funddx33 AS diagnosis_list_detail_measure_value_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosislist_stg AS stg
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS ser
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(ser.server_name)
         WHERE stg.funddx33 IS NOT NULL ) AS wrk
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_diagnosis_list AS ca
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON ca.source_diagnosis_list_id = wrk.source_diagnosis_list_id
      AND ca.server_sk = wrk.server_sk
      LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_diagnosis_list_detail AS ch
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON ch.diagnosis_list_sk = ca.diagnosis_list_sk
      WHERE ch.diagnosis_list_sk IS NULL ) AS a) AS b
);

SET
(tgt_rec_count) =
(
SELECT coalesce(count(*), 0)
FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_diagnosis_list_detail
WHERE ca_diagnosis_list_detail.dw_last_update_date_time >=
    (SELECT max(etl_job_run.job_start_date_time) AS job_start_date_time
     FROM `hca-hin-dev-cur-clinical`.edwcdm_ac.etl_job_run
     WHERE upper(etl_job_run.job_name) = 'J_CDM_ADHOC_CA_DIAGNOSIS_LIST_DETAIL' )
);

SET
difference = CASE 
              WHEN src_rec_count <> 0 Then CAST(((ABS(tgt_rec_count - src_rec_count)/src_rec_count) * 100) AS INT64)
              WHEN src_rec_count =0 AND tgt_rec_count = 0 Then 0
              ELSE tgt_rec_count
              END;


SET
audit_status = CASE
    WHEN difference <= tolerance_percent THEN "PASS"
    ELSE "FAIL"
END;

##Insert statement
INSERT INTO
 {{ params.param_ca_audit_dataset_name }}.audit_control
VALUES
  (GENERATE_UUID(), cast(srctableid AS int64), sourcesysnm, srctablename, tgttablename, audit_type, 
  src_rec_count, tgt_rec_count, control2_src_rowcount, control2_tgt_rowcount, control3_src_rowcount, control3_tgt_rowcount,
  cast(tableload_start_time AS DATETIME), cast(tableload_end_time AS DATETIME),
  tableload_run_time, job_name, audit_time, audit_status );
END;
