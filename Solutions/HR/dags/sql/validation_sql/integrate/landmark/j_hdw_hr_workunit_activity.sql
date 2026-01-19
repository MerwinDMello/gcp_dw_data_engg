
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
tgttablename = concat('edwhr.',@p_table);
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
FROM
{{ params.param_hr_stage_dataset_name }}.hr_workunit_activity_wrk AS stg
LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.hr_workunit_activity AS tgt 
for system_time as of timestamp(tableload_start_time,'US/Central') 
ON stg.workunit_sid = tgt.workunit_sid
AND tgt.activity_seq_num = stg.activity_seq_num
AND coalesce(tgt.workunit_num, -999) = coalesce(stg.workunit_num, -999)
AND coalesce(tgt.start_date_time, DATE '1901-01-01') = coalesce(stg.start_date_time, DATE '1901-01-01')
AND coalesce(tgt.end_date_time, DATE '1901-01-01') = coalesce(stg.end_date_time, DATE '1901-01-01')
AND coalesce(trim(tgt.activity_name), 'X') = coalesce(trim(stg.activity_name), 'X')
AND coalesce(trim(tgt.activity_type_text), 'X') = coalesce(trim(stg.activity_type_text), 'X')
AND coalesce(trim(tgt.action_taken_text), 'X') = coalesce(trim(stg.action_taken_text), 'X')
AND coalesce(trim(tgt.node_caption_text), 'X') = coalesce(trim(stg.node_caption_text), 'X')
AND coalesce(trim(tgt.user_profile_id_text), 'X') = coalesce(trim(stg.user_profile_id_text), 'X')
AND coalesce(trim(tgt.authenticated_author_text), 'X') = coalesce(trim(stg.authenticated_author_text), 'X')
AND coalesce(trim(tgt.task_name), 'X') = coalesce(trim(stg.task_name), 'X')
AND coalesce(tgt.task_type_num, -999) = coalesce(stg.task_type_num, -999)
AND coalesce(tgt.lawson_company_num, -999) = coalesce(stg.lawson_company_num, -999)
AND coalesce(trim(tgt.process_level_code), 'X') = coalesce(trim(stg.process_level_code), 'X')
AND coalesce(trim(tgt.source_system_code), 'X') = coalesce(trim(stg.source_system_code), 'X')
AND coalesce(trim(tgt.active_dw_ind), 'X') = coalesce(trim(stg.active_dw_ind), 'X')
AND tgt.valid_to_date=datetime '9999-12-31 23:59:59'
WHERE tgt.workunit_sid IS NULL);

SET
tgt_rec_count =
(SELECT count(*)
FROM {{ params.param_hr_core_dataset_name }}.hr_workunit_activity
WHERE dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE
  AND active_dw_ind='Y');

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
