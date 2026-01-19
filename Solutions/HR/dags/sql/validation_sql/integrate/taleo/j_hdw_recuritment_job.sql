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
tgttablename = 'edwhr.recruitment_job';
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
select  
  *
FROM
  {{ params.param_hr_stage_dataset_name }}.recruitment_job_wrk AS wrk
WHERE
    
(concat(COALESCE(wrk.recruitment_job_sid,123456),
    COALESCE(wrk.recruitment_job_num,123456),
    UPPER(COALESCE(TRIM(wrk.job_title_name), 'XXX')),
    COALESCE(TRIM(wrk.job_grade_code), 'XXX'),
    COALESCE(wrk.job_schedule_id,123456),
    COALESCE(wrk.overtime_status_id,123456),
    COALESCE(wrk.primary_facility_location_num,123456),
    COALESCE(wrk.recruiter_user_sid,123456),
    COALESCE(wrk.recruitment_job_parameter_sid,123456),
    COALESCE(wrk.recruitment_position_sid,123456),
    COALESCE(wrk.fte_pct,123456),
    COALESCE(TRIM(wrk.source_system_code), 'XXX'))) NOT IN(
  SELECT
    concat(COALESCE(tgt.recruitment_job_sid,123456),
    COALESCE(tgt.recruitment_job_num,123456),
    UPPER(COALESCE(TRIM(tgt.job_title_name), 'XXX')),
    COALESCE(TRIM(tgt.job_grade_code), 'XXX'),
    COALESCE(tgt.job_schedule_id,123456),
    COALESCE(tgt.overtime_status_id,123456),
    COALESCE(tgt.primary_facility_location_num,123456),
    COALESCE(tgt.recruiter_user_sid,123456),
    COALESCE(tgt.recruitment_job_parameter_sid,123456),
    COALESCE(tgt.recruitment_position_sid,123456),
    COALESCE(tgt.fte_pct,123456),
    COALESCE(TRIM(tgt.source_system_code), 'XXX'))
  FROM
    {{ params.param_hr_core_dataset_name }}.recruitment_job AS tgt for system_time as of timestamp(tableload_start_time,'US/Central')
  WHERE
    (tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59")) 
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.recruitment_job
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




