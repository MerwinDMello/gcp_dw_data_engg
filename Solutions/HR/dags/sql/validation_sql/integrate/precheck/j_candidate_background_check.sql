
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
{{ params.param_hr_stage_dataset_name }}.candidate_background_check_wrk AS wrk
LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.candidate_background_check AS tgt 
for system_time as of timestamp(tableload_start_time,'US/Central')     
    ON wrk.report_sid = tgt.report_sid
    AND coalesce(trim(CAST(wrk.report_create_date_time as STRING)), '1900-01-01') = coalesce(trim(CAST(tgt.report_create_date_time as STRING)), '1900-01-01')
    AND coalesce(trim(wrk.candidate_first_name), 'XXX') = coalesce(trim(tgt.candidate_first_name), 'XXX')
    AND coalesce(trim(wrk.candidate_middle_name), 'XXX') = coalesce(trim(tgt.candidate_middle_name), 'XXX')
    AND coalesce(trim(wrk.candidate_last_name), 'XXX') = coalesce(trim(tgt.candidate_last_name), 'XXX')
    AND coalesce(trim(wrk.social_security_num), 'XXX') = coalesce(trim(tgt.social_security_num), 'XXX')
    AND coalesce(trim(CAST(wrk.report_reopen_date_time as STRING)), '1900-01-01') = coalesce(trim(CAST(tgt.report_reopen_date_time as STRING)), '1900-01-01')
    AND coalesce(trim(CAST(wrk.report_completion_date_time as STRING)), '1900-01-01') = coalesce(trim(CAST(tgt.report_completion_date_time as STRING)), '1900-01-01')
    AND coalesce(trim(wrk.process_level_code), 'XXX') = coalesce(trim(tgt.process_level_code), 'XXX')
    AND coalesce(trim(wrk.recruitment_requisition_num_text), 'XXX') = coalesce(trim(tgt.recruitment_requisition_num_text), 'XXX')
    AND trim(CAST(coalesce(wrk.days_elapsed_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.days_elapsed_cnt, 9) as STRING))
    AND trim(CAST(coalesce(wrk.criminal_search_ordered_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.criminal_search_ordered_cnt, 9) as STRING))
    AND trim(CAST(coalesce(wrk.criminal_search_pending_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.criminal_search_pending_cnt, 9) as STRING))
    AND trim(CAST(coalesce(wrk.motor_vehicle_record_ordered_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.motor_vehicle_record_ordered_cnt, 9) as STRING))
    AND trim(CAST(coalesce(wrk.motor_vehicle_record_pending_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.motor_vehicle_record_pending_cnt, 9) as STRING))
    AND trim(CAST(coalesce(wrk.employment_verification_ordered_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.employment_verification_ordered_cnt, 9) as STRING))
    AND trim(CAST(coalesce(wrk.employment_verification_pending_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.employment_verification_pending_cnt, 9) as STRING))
    AND trim(CAST(coalesce(wrk.education_verification_ordered_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.education_verification_ordered_cnt, 9) as STRING))
    AND trim(CAST(coalesce(wrk.education_verification_pending_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.education_verification_pending_cnt, 9) as STRING))
    AND trim(CAST(coalesce(wrk.license_verification_ordered_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.license_verification_ordered_cnt, 9) as STRING))
    AND trim(CAST(coalesce(wrk.license_verification_pending_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.license_verification_pending_cnt, 9) as STRING))
    AND trim(CAST(coalesce(wrk.personal_reference_ordered_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.personal_reference_ordered_cnt, 9) as STRING))
    AND trim(CAST(coalesce(wrk.personal_reference_pending_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.personal_reference_pending_cnt, 9) as STRING))
    AND trim(CAST(coalesce(wrk.sanction_check_ordered_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.sanction_check_ordered_cnt, 9) as STRING))
    AND trim(CAST(coalesce(wrk.sanction_check_pending_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.sanction_check_pending_cnt, 9) as STRING))
    AND trim(CAST(coalesce(wrk.report_num, 9) as STRING)) = trim(CAST(coalesce(tgt.report_num, 9) as STRING))
    AND coalesce(trim(wrk.source_system_code), 'XXX') = coalesce(trim(tgt.source_system_code), 'XXX')
    AND DATE(tgt.valid_to_date) = DATE('9999-12-31')
WHERE tgt.report_sid IS NULL );

SET
tgt_rec_count =
(SELECT count(*)
FROM {{ params.param_hr_core_dataset_name }}.candidate_background_check
WHERE dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE
AND DATE(valid_to_date) = DATE('9999-12-31'));

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
