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
     (SELECT NULL AS proc_list_sk,
             trim(format('%11d', dc.proc_category_id)) AS proc_category_id,
             trim(format('%11d', s.server_sk)) AS server_sk,
             trim(format('%11d', stg.id)) AS id,
             trim(stg.wrkgrpcode) AS wrkgrpcode,
             trim(format('%11d', stg.kingdomcode)) AS kingdomcode,
             trim(stg.kingdom) AS kingdom,
             trim(stg.phylum) AS phylum,
             trim(stg.procedurename) AS procedurename,
             trim(stg.dbtype) AS dbtype,
             trim(format('%11d', stg.stsdup250)) AS stsdup250,
             trim(regexp_replace(format('%#5.1f', stg.stsdup30), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')) AS stsdup30,
             trim(stg.ststerm250) AS ststerm250,
             trim(stg.stsid250) AS stsid250,
             trim(stg.ststerm30) AS ststerm30,
             trim(stg.stsid30) AS stsid30,
             trim(stg.ipccc) AS ipccc,
             trim(stg.cptcode) AS cptcode,
             trim(stg.icd9code) AS icd9code,
             trim(stg.rachs1) AS rachs1,
             trim(stg.price) AS price,
             trim(format('%11d', stg.inactive)) AS inactive,
             trim(stg.pxmcategory) AS pxmcategory,
             trim(stg.pxscategory) AS pxscategory,
             trim(format('%11d', stg.pxvalve)) AS pxvalve,
             trim(format('%11d', stg.pxcabg)) AS pxcabg,
             trim(format('%11d', stg.pxmechsupp)) AS pxmechsupp,
             trim(format('%11d', stg.pxtx)) AS pxtx,
             trim(format('%11d', stg.pxhcsp)) AS pxhcsp,
             trim(format('%11d', stg.pxpacemaker)) AS pxpacemaker,
             trim(format('%11d', stg.pxvad)) AS pxvad,
             trim(format('%11d', stg.accproc)) AS accproc,
             trim(format('%11d', stg.pxnos)) AS pxnos,
             trim(format('%11d', stg.pxmodifier)) AS pxmodifier,
             trim(format('%11d', stg.aristlebasiclvl)) AS aristlebasiclvl,
             trim(regexp_replace(format('%#5.1f', stg.aristlebasicscore), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')) AS aristlebasicscore,
             trim(stg.ststerm30_sp) AS ststerm30_sp,
             trim(stg.stsid30_sp) AS stsid30_sp,
             trim(stg.abtscode) AS abtscode,
             trim(regexp_replace(format('%#5.1f', stg.statscore), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')) AS statscore,
             trim(format('%11d', stg.statcategory)) AS statcategory,
             trim(format('%11d', stg.stsrankorder30)) AS stsrankorder30,
             trim(regexp_replace(format('%#5.1f', stg.stsdup32), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')) AS stsdup32,
             trim(stg.ststerm32) AS ststerm32,
             trim(stg.stsid32) AS stsid32,
             trim(format('%11d', stg.stsbaseterm30)) AS stsbaseterm30,
             trim(stg.ststerm32_sp) AS ststerm32_sp,
             trim(stg.stsid32_sp) AS stsid32_sp,
             trim(format('%11d', stg.stsbaseterm32)) AS stsbaseterm32,
             trim(stg.ststerm33) AS ststerm33,
             trim(stg.stsid33) AS stsid33,
             trim(regexp_replace(format('%#5.1f', stg.stsdup33), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')) AS stsdup33,
             trim(stg.ststerm33_sp) AS ststerm33_sp,
             trim(stg.stsid33_sp) AS stsid33_sp,
             trim(format('%11d', stg.stsbaseterm33)) AS stsbaseterm33,
             trim(stg.server_name) AS SERVER_NAME,
             trim(stg.full_server_nm) AS full_server_nm,
             stg.dw_last_update_date_time AS dw_last_update_date_time
      FROM {{ params.param_clinical_cdm_stage_dataset_name }}.cardioaccess_procedurelist_stg AS stg
      LEFT OUTER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ref_ca_proc_category AS dc
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(trim(stg.pxmcategory)) = upper(trim(dc.proc_category_name))
      AND upper(trim(stg.pxscategory)) = upper(trim(dc.proc_sub_category_name))
      INNER JOIN {{ params.param_clinical_cdm_core_dataset_name }}.ca_server AS s
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(trim(stg.full_server_nm)) = upper(trim(s.server_name))) AS p) AS q
);

SET
(tgt_rec_count) =
(
SELECT count(*)
FROM
  (SELECT ca_proc_list.*
   FROM {{ params.param_clinical_cdm_core_dataset_name }}.ca_proc_list) AS a
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
