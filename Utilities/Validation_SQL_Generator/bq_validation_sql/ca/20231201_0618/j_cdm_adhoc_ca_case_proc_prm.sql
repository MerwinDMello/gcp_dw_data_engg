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
  (SELECT p.*
   FROM
     (SELECT 1 AS case_proc_sk,
             trim(format('%11d', a.patient_case_sk)) AS patient_case_sk,
             trim(format('%11d', s.server_sk)) AS server_sk,
             trim(format('%11d', stg.procid)) AS proc_list_sk,
             trim(format('%11d', stg.procedureid)) AS procedureid,
             trim(format('%11d', stg.hospitalid)) AS hospitalid,
             trim(format('%11d', stg.casenumber)) AS casenumber,
             trim(stg.procedurename) AS procedurename,
             trim(stg.cptcode) AS cptcode,
             trim(stg.price) AS price,
             trim(format('%11d', stg.procid)) AS procid,
             trim(stg.modifier) AS modifier,
             trim(stg.proccateg) AS proccateg,
             trim(stg.procshrtlst) AS procshrtlst,
             trim(stg.icd_9code) AS icd_9code,
             trim(stg.icd_10code) AS icd_10code,
             trim(stg.code1) AS code1,
             trim(stg.code2) AS code2,
             trim(stg.code3) AS code3,
             trim(stg.code4) AS code4,
             trim(stg.code5) AS code5,
             trim(stg.code6) AS code6,
             trim(stg.code7) AS code7,
             trim(stg.code8) AS code8,
             trim(stg.code9) AS code9,
             trim(stg.code10) AS code10,
             trim(stg.code11) AS code11,
             trim(stg.code12) AS code12,
             trim(format('%11d', stg.primproc)) AS primproc,
             trim(format('%11d', stg.sort)) AS
      SORT,
             trim(stg.other) AS other,
             trim(stg.recur) AS recur,
             trim(stg.aristotlescore) AS aristotlescore,
             trim(format('%11d', stg.rachsscore)) AS rachsscore,
             stg.createdate AS createdate,
             stg.updatedate AS updatedate,
             stg.updateby AS updateby,
             trim(format('%11d', stg.psf)) AS psf,
             trim(stg.server_name) AS SERVER_NAME,
             trim(stg.full_server_nm) AS full_server_nm,
             stg.dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_procedures_stg AS stg
      LEFT OUTER JOIN
        (SELECT c.patient_case_sk,
                c.source_patient_case_num,
                s_0.server_name
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_patient_case AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s_0
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(format('%11d', c.server_sk)) = trim(format('%11d', s_0.server_sk))) AS a ON upper(trim(stg.full_server_nm)) = upper(trim(a.server_name))
      AND trim(format('%11d', stg.casenumber)) = trim(format('%11d', a.source_patient_case_num))
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(trim(stg.full_server_nm)) = upper(trim(s.server_name))
      LEFT OUTER JOIN
        (SELECT c.source_proc_list_id,
                p_0.server_name
         FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_proc_list AS c
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
         INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS p_0
         FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(format('%11d', c.server_sk)) = trim(format('%11d', p_0.server_sk))) AS b ON trim(format('%11d', stg.procid)) = trim(format('%11d', b.source_proc_list_id))
      AND upper(trim(stg.full_server_nm)) = upper(trim(b.server_name))) AS p) AS q
);

SET
(tgt_rec_count) =
(
SELECT count(*)
FROM
  (SELECT ca_case_proc.*
   FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_case_proc) AS a
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
