
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
tolerance_percent = 5   ;
SET
src_rec_count = 
(SELECT count(*)
FROM
{{ params.param_hr_stage_dataset_name }}.student_program_graduation_wrk AS stg
LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.student_program_graduation AS tgt 
for system_time as of timestamp(tableload_start_time,'US/Central') 
ON tgt.student_sid = stg.student_sid
AND tgt.nursing_program_id = stg.nursing_program_id
AND trim(CAST(coalesce(tgt.graduation_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.graduation_date, DATE '1900-01-01') as STRING))
AND trim(CAST(coalesce(tgt.cumulative_gpa, -999) as STRING)) = trim(CAST(coalesce(stg.cumulative_gpa, -999) as STRING))
AND trim(CAST(coalesce(tgt.nursing_school_id, -999) as STRING)) = trim(CAST(coalesce(stg.nursing_school_id, -999) as STRING))
AND coalesce(trim(tgt.source_system_code), 'xx') = coalesce(trim(stg.source_system_code), 'xx')
AND date(tgt.valid_to_date) = DATETIME("9999-12-31")
WHERE tgt.student_sid IS NULL);
-- All the exiting records in core table are updated with current_ts if they are in stage and new records are inserted
-- So src count indicates just inserts but tgt count indicates both inserts and existing records with updated dw_update_date_time
SET
tgt_rec_count =
(SELECT count(*)
FROM
  (SELECT *
   FROM {{ params.param_hr_core_dataset_name }}.student_program_graduation
   WHERE date(valid_to_date) = DATETIME("9999-12-31")
     AND dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE )a);

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
