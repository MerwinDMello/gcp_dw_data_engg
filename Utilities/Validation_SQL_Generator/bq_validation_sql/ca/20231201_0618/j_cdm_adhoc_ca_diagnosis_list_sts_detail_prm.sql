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
                      wrk.data_version_code AS data_version_code,
                      wrk.sts_dup_sk AS sts_dup_sk,
                      wrk.sts_harvest_code AS sts_harvest_code,
                      wrk.sts_short_term_text AS sts_short_term_text,
                      wrk.sts_base_term_num AS sts_base_term_num,
                      wrk.source_system_code AS source_system_code,
                      wrk.dw_last_update_date_time AS dw_last_update_date_time
      FROM
        (SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                         csrv.server_sk AS server_sk,
                         stg.id AS source_diagnosis_list_id,
                         '2.3/2.5' AS data_version_code,
                         rd.source_sts_dup_id AS sts_dup_sk,
                         stg.stsid250 AS sts_harvest_code,
                         stg.ststerm250 AS sts_short_term_text,
                         CAST(NULL AS INT64) AS sts_base_term_num,
                         'C' AS source_system_code,
                         timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosislist_stg AS stg
         LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ref_ca_sts_dup AS rd
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON stg.stsdup250 = rd.source_sts_dup_id
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS csrv
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(csrv.server_name)
         WHERE stg.stsdup250 IS NOT NULL
           OR stg.stsid250 IS NOT NULL
           OR stg.ststerm250 IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        csrv.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '3.0' AS data_version_code,
                                        rd.source_sts_dup_id AS sts_dup_sk,
                                        stg.stsid30 AS sts_harvest_code,
                                        stg.ststerm30 AS sts_short_term_text,
                                        stg.stsbaseterm AS sts_base_term_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosislist_stg AS stg
         LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ref_ca_sts_dup AS rd
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON stg.stsdup30 = rd.source_sts_dup_id
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS csrv
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(csrv.server_name)
         WHERE stg.stsdup30 IS NOT NULL
           OR stg.stsid30 IS NOT NULL
           OR stg.ststerm30 IS NOT NULL
           AND stg.stsbaseterm IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        csrv.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '3.22' AS data_version_code,
                                        rd.source_sts_dup_id AS sts_dup_sk,
                                        stg.stsid32 AS sts_harvest_code,
                                        stg.ststerm32 AS sts_short_term_text,
                                        stg.stsbaseterm32 AS sts_base_term_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosislist_stg AS stg
         LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ref_ca_sts_dup AS rd
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON stg.stsdup32 = rd.source_sts_dup_id
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS csrv
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(csrv.server_name)
         WHERE stg.stsdup32 IS NOT NULL
           OR stg.stsid32 IS NOT NULL
           OR stg.ststerm32 IS NOT NULL
           AND stg.stsbaseterm32 IS NOT NULL
         UNION DISTINCT SELECT DISTINCT CAST(NULL AS INT64) AS diagnosis_list_sk,
                                        csrv.server_sk AS server_sk,
                                        stg.id AS source_diagnosis_list_id,
                                        '3.3' AS data_version_code,
                                        rd.source_sts_dup_id AS sts_dup_sk,
                                        stg.stsid33 AS sts_harvest_code,
                                        stg.ststerm33 AS sts_short_term_text,
                                        stg.stsbaseterm33 AS sts_base_term_num,
                                        'C' AS source_system_code,
                                        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
         FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_diagnosislist_stg AS stg
         LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ref_ca_sts_dup AS rd
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON stg.stsdup33 = rd.source_sts_dup_id
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS csrv
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.full_server_nm) = upper(csrv.server_name)
         WHERE stg.stsdup33 IS NOT NULL
           OR stg.stsid33 IS NOT NULL
           OR stg.ststerm33 IS NOT NULL
           AND stg.stsbaseterm33 IS NOT NULL ) AS wrk
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_diagnosis_list AS ca
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON ca.source_diagnosis_list_id = wrk.source_diagnosis_list_id
      AND ca.server_sk = wrk.server_sk
      LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_diagnosis_list_sts_detail AS ch
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON ch.diagnosis_list_sk = ca.diagnosis_list_sk
      WHERE ch.diagnosis_list_sk IS NULL ) AS a) AS b
);

SET
(tgt_rec_count) =
(
SELECT coalesce(count(*), 0)
FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_diagnosis_list_sts_detail
WHERE ca_diagnosis_list_sts_detail.dw_last_update_date_time >=
    (SELECT max(etl_job_run.job_start_date_time) AS job_start_date_time
     FROM `hca-hin-dev-cur-clinical`.edwcdm_ac.etl_job_run
     WHERE upper(etl_job_run.job_name) = 'J_CDM_ADHOC_CA_DIAGNOSIS_LIST_STS_DETAIL' )
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
