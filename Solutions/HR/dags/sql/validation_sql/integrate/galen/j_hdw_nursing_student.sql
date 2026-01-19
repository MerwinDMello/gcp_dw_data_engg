
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
tolerance_percent = 5  ;
SET
src_rec_count = 
(SELECT count(*)
FROM
{{ params.param_hr_stage_dataset_name }}.nursing_student_wrk AS stg
LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.nursing_student AS tgt 
for system_time as of timestamp(tableload_start_time,'US/Central') 
ON stg.student_sid = tgt.student_sid
AND trim(CAST(coalesce(tgt.student_num, -999) as STRING)) = trim(CAST(coalesce(stg.student_num, -999) as STRING))
AND upper(trim(coalesce(tgt.student_ssn, 'XXX'))) = upper(trim(coalesce(stg.student_ssn, 'XXX')))
AND upper(trim(coalesce(tgt.student_first_name, 'XXX'))) = upper(trim(coalesce(stg.student_first_name, 'XXX')))
AND upper(trim(coalesce(tgt.student_last_name, 'XXX'))) = upper(trim(coalesce(stg.student_last_name, 'XXX')))
AND upper(trim(coalesce(tgt.student_middle_name, 'XXX'))) = upper(trim(coalesce(stg.student_middle_name, 'XXX')))
AND trim(CAST(coalesce(tgt.birth_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.birth_date, DATE '1900-01-01') as STRING))
AND upper(trim(coalesce(tgt.gender_code, 'XXX'))) = upper(trim(coalesce(stg.gender_code, 'XXX')))
AND upper(trim(coalesce(tgt.ethnic_origin_desc, 'XXX'))) = upper(trim(coalesce(stg.ethnic_origin_desc, 'XXX')))
AND trim(CAST(coalesce(tgt.addr_sid, -999) as STRING)) = trim(CAST(coalesce(stg.addr_sid, -999) as STRING))
AND upper(trim(coalesce(tgt.pell_grant_eligibility_ind, 'XXX'))) = upper(trim(coalesce(stg.pell_grant_eligibility_ind, 'XXX')))
AND upper(trim(coalesce(tgt.first_gen_college_grad_ind, 'XXX'))) = upper(trim(coalesce(stg.first_gen_college_grad_ind, 'XXX')))
AND trim(CAST(coalesce(tgt.employee_num, -999) as STRING)) = trim(CAST(coalesce(stg.employee_num, -999) as STRING))
AND coalesce(trim(tgt.source_system_code), 'XX') = coalesce(trim(stg.source_system_code), 'XX')
AND tgt.valid_to_date = DATETIME('9999-12-31 23:59:59')
WHERE tgt.student_sid IS NULL);

SET
tgt_rec_count =
(SELECT count(*)
FROM
  (SELECT *
   FROM {{ params.param_hr_core_dataset_name }}.nursing_student
   WHERE valid_to_date = DATETIME('9999-12-31 23:59:59')
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
