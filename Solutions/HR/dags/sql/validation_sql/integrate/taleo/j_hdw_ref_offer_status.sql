
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
tgttablename = 'edwhr.ref_offer_status';
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
tolerance_percent = 0;
SET
src_rec_count = 
(SELECT count(*)
FROM (
SELECT DISTINCT
    CAST(stg.number AS INT64) AS offer_status_id,
    trim(description) AS offer_status_desc,
    'T' AS source_system_code,
    timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
    FROM
    {{ params.param_hr_stage_dataset_name }}.taleo_offerstatus AS stg
UNION ALL
SELECT
    CAST(CASE
        WHEN tgt_0.offer_status_id IS NOT NULL THEN tgt_0.offer_status_id
        ELSE (
        SELECT
            coalesce(max(ref_offer_status.offer_status_id), CAST(1000 as INT64))
            FROM
            {{ params.param_hr_core_dataset_name }}.ref_offer_status
            for system_time as of timestamp(tableload_start_time,'US/Central') 
            WHERE upper(ref_offer_status.source_system_code) = 'B'
        ) + CAST(row_number() OVER (ORDER BY tgt_0.offer_status_id, stgg.offer_status_desc) as BIGNUMERIC)
    END as INT64) AS offer_status_id,
    stgg.offer_status_desc,
    stgg.source_system_code,
    stgg.dw_last_update_date_time
FROM
(
    SELECT DISTINCT
        trim(offerstatus_state) AS offer_status_desc,
        'B' AS source_system_code,
        timestamp_trunc(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
    FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v3_jobapplication_stg
    WHERE coalesce(trim(offerstatus_state), '') <> ''
) AS stgg
LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.ref_offer_status AS tgt_0 
for system_time as of timestamp(tableload_start_time,'US/Central') 
ON trim(tgt_0.offer_status_desc) = stgg.offer_status_desc
    AND upper(tgt_0.source_system_code) = 'B'));

SET
tgt_rec_count =
(SELECT count(*)
FROM {{ params.param_hr_core_dataset_name }}.ref_offer_status
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
