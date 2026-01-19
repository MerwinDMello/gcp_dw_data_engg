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
sourcesysnm =@p_source;
SET
srctablename = Null;
SET
tgttablename = 'edwhr.candidate_answer';
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
(select count(*)
from 
(
    SELECT
        stg.answer_sid,
        stg.answer_num,
        datetime_trunc(current_datetime('US/Central'), second),
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        stg.answer_desc,
        stg.question_sid,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.candidate_answer_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.candidate_answer AS tgt for system_time as of timestamp(tableload_start_time,'US/Central') 
         ON tgt.answer_sid = stg.answer_sid 
         AND trim(CAST(coalesce(tgt.answer_num, -999) as STRING)) = trim(CAST(coalesce(stg.answer_num, -999) as STRING))
         AND upper(trim(coalesce(tgt.answer_desc, 'XXX'))) = upper(trim(coalesce(stg.answer_desc, 'XXX')))
         AND trim(CAST(coalesce(tgt.question_sid, -999) as STRING)) = trim(CAST(coalesce(stg.question_sid, -999) as STRING))
         AND coalesce(trim(tgt.source_system_code), 'X') = coalesce(trim(stg.source_system_code), 'X')
         AND (tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59")
      WHERE tgt.answer_sid IS NULL
)stg);


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_core_dataset_name }}.candidate_answer
where dw_last_update_date_time >= tableload_start_time  - interval 1 minute
AND DATE(VALID_TO_DATE) = DATE('9999-12-31'));

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

