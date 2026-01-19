
##########################
## Variable Declaration ##
##########################

BEGIN
DECLARE
tolerance_percent,difference,srctableid,src_rec_count,tgt_rec_count int64;
declare
sourcesysnm,srctablename,tgttablename,audit_type,tableload_run_time,job_name,audit_status string;
declare
tableload_start_time,tableload_end_time,audit_time,current_ts datetime;
SET
current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
SET 
srctableid = Null;
SET
sourcesysnm = @p_source;
SET
srctablename = Null;
SET
tgttablename = 'edwhr.ref_submission_status';
SET
audit_type ='VALIDATION_COUNT';
SET
tableload_start_time = @p_tableload_start_time;
SET
tableload_end_time = @p_tableload_end_time;
SET
tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET
job_name = @p_job_name;
SET
audit_time = current_ts;
SET
tolerance_percent = 5;
SET
src_rec_count = 
(SELECT count(*)
FROM (
SELECT
    CAST(number as INT64) AS submission_status_id,
    available AS active_sw,
    applicationstate_number AS submission_state_id,
    trim(mnemonic) AS submission_status_code,
    trim(name) AS submission_status_name,
    trim(description) AS submission_status_desc,
    'T' AS source_system_code,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
    {{ params.param_hr_stage_dataset_name }}.taleo_cswstatus
GROUP BY 1, 2, 3, 4, 5, 6, 7

UNION ALL
SELECT
    0 as submission_status_id,
    CAST(ws.active AS STRING) AS active_sw,
    '0' AS submission_state_id,
    trim('1/' || ws.atsworkflow || '/' || ws.atsworkflowstep) AS submission_status_code,
    trim(ws.atsworkflowstep) AS submission_status_name,
    trim(ws.description) AS submission_status_desc,
    'B' AS source_system_code,
    DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
    {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_atsworkflowstep_stg AS ws
GROUP BY 1, 2, 3, 4, 5, 6, 7));

SET
tgt_rec_count =
(SELECT count(*)
FROM {{ params.param_hr_core_dataset_name }}.ref_submission_status
WHERE dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE);

SET
difference = CASE 
            WHEN src_rec_count <> 0 Then CAST(((ABS(tgt_rec_count - src_rec_count)/src_rec_count) * 100) AS INT64)
            WHEN src_rec_count =0 and tgt_rec_count = 0 Then 0
            ELSE tgt_rec_count
            END;

SET
audit_status = CASE
    WHEN difference <= tolerance_percent THEN "PASS"
    ELSE "FAIL"
END;

##Insert statement
INSERT INTO
{{ params.param_hr_audit_dataset_name }}.audit_control
VALUES
(GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type, src_rec_count, tgt_rec_count, 
cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
tableload_run_time,
job_name, audit_time, audit_status );
END; 
