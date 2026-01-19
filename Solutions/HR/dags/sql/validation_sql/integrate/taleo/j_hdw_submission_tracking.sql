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
tgttablename = 'edwhr.submission_tracking';
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
SELECT DISTINCT
        -- changed from Submission_Profile_SID
        wrk.submission_tracking_sid AS submission_tracking_sid,
        current_ts as Valid_From_Date,
        -- wrk.file_date AS valid_from_date,
        wrk.candidate_profile_sid AS candidate_profile_sid,
        -- 20 as Submission_Profile_SID AS ,
        wrk.submission_tracking_num AS submission_tracking_num,
        wrk.creation_date_time AS creation_date_time,
        wrk.event_date_time AS event_date_time,
        wrk.event_detail_text AS event_detail_text,
        wrk.submission_event_id AS submission_event_id,
        wrk.tracking_user_sid AS tracking_user_sid,
        -- 15 as Tracking_User_SID AS,
        wrk.tracking_step_id AS tracking_step_id,
        wrk.tracking_workflow_id AS tracking_workflow_id,
        wrk.sub_status_desc AS sub_status_desc,
        wrk.step_reverted_ind AS step_reverted_ind,
        DATETIME('9999-12-31 23:59:59') AS valid_to_date,
        wrk.source_system_code AS source_system_code,
        DATETIME_TRUNC(current_datetime(), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.submission_tracking_wrk AS wrk
      WHERE (CONCAT(coalesce(trim(cast(wrk.candidate_profile_sid as string)), CAST(0 as STRING)), coalesce(trim(cast(wrk.submission_tracking_num as string)), CAST(0 as STRING)), coalesce(wrk.creation_date_time, DATETIME '1900-01-01'), coalesce(wrk.event_date_time, DATE '1900-01-01'), coalesce(trim(wrk.event_detail_text), 'XXX'), coalesce(trim(cast(wrk.submission_event_id as string)), CAST(0 as STRING)), coalesce(trim(cast(wrk.tracking_user_sid as string)), CAST(0 as STRING)), coalesce(trim(cast(wrk.tracking_step_id as string)), CAST(0 as STRING)), coalesce(trim(cast(wrk.tracking_workflow_id as string)), CAST(0 as STRING)), coalesce(trim(wrk.step_reverted_ind), 'XXX'), coalesce(trim(wrk.sub_status_desc), 'XXX'), coalesce(trim(wrk.source_system_code), 'X'))) NOT IN(
        SELECT 
           CONCAT(coalesce(trim(cast(tgt.candidate_profile_sid as string)), CAST(0 as STRING)),
            coalesce(trim(cast(tgt.submission_tracking_num as string)), CAST(0 as STRING)),
            coalesce(tgt.creation_date_time, DATE '1900-01-01'),
            coalesce(tgt.event_date_time, DATE '1900-01-01'),
            coalesce(trim(tgt.event_detail_text), 'XXX'),
            coalesce(trim(cast(tgt.submission_event_id as string)), CAST(0 as STRING)),
            coalesce(trim(cast(tgt.tracking_user_sid as string)), CAST(0 as STRING)),
            coalesce(trim(cast(tgt.tracking_step_id as string)), CAST(0 as STRING)),
            coalesce(trim(cast(tgt.tracking_workflow_id as string)), CAST(0 as STRING)),
            coalesce(trim(tgt.step_reverted_ind), 'XXX'),
            coalesce(trim(tgt.sub_status_desc), 'XXX'),
            coalesce(trim(tgt.source_system_code), 'X'))
          FROM
            {{ params.param_hr_core_dataset_name }}.submission_tracking AS tgt for system_time as of timestamp(tableload_start_time,'US/Central')
          WHERE (tgt.valid_to_date) = datetime("9999-12-31 23:59:59")
      )

));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.submission_tracking
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




