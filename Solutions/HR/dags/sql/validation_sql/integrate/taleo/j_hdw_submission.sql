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
tgttablename = 'edwhr.submission';
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
(select count(*)
from 
(
SELECT
        *
      FROM
        {{ params.param_hr_stage_dataset_name }}.submission_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.submission AS tgt for system_time as of timestamp(tableload_start_time,'US/Central')
        ON stg.submission_sid = tgt.submission_sid
         AND coalesce(tgt.submission_num, -999) = coalesce(stg.submission_num, -999)
         AND coalesce(tgt.last_modified_date, DATETIME '1901-01-01') = coalesce(stg.last_modified_date, DATE '1901-01-01')
         AND coalesce(tgt.new_submission_sw, -999) = coalesce(stg.new_submission_sw, -999)
         AND coalesce(tgt.candidate_sid, -999) = coalesce(stg.candidate_sid, -999)
         AND coalesce(tgt.recruitment_requisition_sid, -999) = coalesce(stg.recruitment_requisition_sid, -999)
         AND coalesce(tgt.candidate_profile_sid, -999) = coalesce(stg.candidate_profile_sid, -999)
         AND coalesce(tgt.current_submission_status_id, -999) = coalesce(stg.current_submission_status_id, -999)
         AND coalesce(tgt.current_submission_step_id, -999) = coalesce(stg.current_submission_step_id, -999)
         AND coalesce(tgt.requisition_num, -999) = coalesce(stg.requisition_num, -999)
         AND coalesce(tgt.job_application_num, -999) = coalesce(stg.job_application_num, -999)
         AND coalesce(tgt.candidate_num, -999) = coalesce(stg.candidate_num, -999)
         AND coalesce(tgt.matched_from_requisition_num, -999) = coalesce(stg.matched_from_requisition_num, -999)
         AND coalesce(tgt.current_submission_workflow_id, -999) = coalesce(stg.current_submission_workflow_id, -999)
         AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND coalesce(trim(tgt.submission_source_code), 'X') = coalesce(trim(stg.submission_source_code), 'X')
		 WHERE tgt.submission_sid IS NULL

));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.submission
where date(valid_to_date) = '9999-12-31' and dw_last_update_date_time >= tableload_start_time  - interval 1 minute
) ;

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




