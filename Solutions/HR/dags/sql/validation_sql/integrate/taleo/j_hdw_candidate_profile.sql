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
tgttablename = 'edwhr.candidate_profile';
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
        {{ params.param_hr_stage_dataset_name }}.candidate_profile_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.candidate_profile AS tgt for system_time as of timestamp(tableload_start_time,'US/Central')
		ON stg.candidate_profile_sid = tgt.candidate_profile_sid
         AND trim(CAST(coalesce(tgt.candidate_sid, -999) as STRING)) = trim(CAST(coalesce(stg.candidate_sid, -999) as STRING))
         AND trim(CAST(coalesce(tgt.candidate_profile_num, -999) as STRING)) = trim(CAST(coalesce(stg.candidate_profile_num, -999) as STRING))
         AND coalesce(tgt.submission_date, DATE '1901-01-01') = coalesce(stg.submission_date, DATE '1901-01-01')
         AND coalesce(tgt.completion_date, DATE '1901-01-01') = coalesce(stg.completion_date, DATE '1901-01-01')
         AND coalesce(tgt.creation_date, DATE '1901-01-01') = coalesce(stg.creation_date, DATE '1901-01-01')
         AND trim(CAST(coalesce(tgt.recruitment_source_id, -999) as STRING)) = trim(CAST(coalesce(stg.recruitment_source_id, -999) as STRING))
         AND trim(CAST(coalesce(tgt.recruitment_source_auto_filled_sw, -999) as STRING)) = trim(CAST(coalesce(stg.recruitment_source_auto_filled_sw, -999) as STRING))
         AND trim(CAST(coalesce(tgt.job_application_num, -999) as STRING)) = trim(CAST(coalesce(stg.job_application_num, -999) as STRING))
         AND trim(CAST(coalesce(tgt.requisition_num, -999) as STRING)) = trim(CAST(coalesce(stg.requisition_num, -999) as STRING))
         AND trim(CAST(coalesce(tgt.candidate_num, -999) as STRING)) = trim(CAST(coalesce(stg.candidate_num, -999) as STRING))
         AND DATE(tgt.valid_to_date) ='9999-12-31'
         AND tgt.source_system_code = stg.source_system_code
      WHERE tgt.candidate_profile_sid IS NULL

));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.candidate_profile
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




