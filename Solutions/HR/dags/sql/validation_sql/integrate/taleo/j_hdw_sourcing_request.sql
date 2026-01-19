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
tgttablename ='edwhr.sourcing_request';
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
        stg.sourcing_request_sid,
        datetime_trunc(current_datetime('US/Central'), second) AS valid_from_date,
        stg.recruitment_requisition_sid,
        stg.job_board_id,
        stg.source_request_status_id,
        stg.job_board_type_id,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        stg.posting_date,
        stg.unposting_date,
        stg.creation_date,
        stg.requisition_num,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.sourcing_request_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.sourcing_request AS tgt for system_time as of timestamp(tableload_start_time,'US/Central')
        ON stg.sourcing_request_sid = tgt.sourcing_request_sid
         AND trim(CAST(coalesce(tgt.recruitment_requisition_sid, -999) as STRING)) = trim(CAST(coalesce(stg.recruitment_requisition_sid, -999) as STRING))
         AND trim(CAST(coalesce(tgt.job_board_id, -999) as STRING)) = trim(CAST(coalesce(stg.job_board_id, -999) as STRING))
         AND trim(CAST(coalesce(tgt.source_request_status_id, -999) as STRING)) = trim(CAST(coalesce(stg.source_request_status_id, -999) as STRING))
         AND trim(CAST(coalesce(tgt.job_board_type_id, -999) as STRING)) = trim(CAST(coalesce(stg.job_board_type_id, -999) as STRING))
         AND trim(CAST(coalesce(tgt.posting_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.posting_date, DATE '1900-01-01') as STRING))
         AND trim(CAST(coalesce(tgt.unposting_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.unposting_date, DATE '1900-01-01') as STRING))
         AND trim(CAST(coalesce(tgt.creation_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.creation_date, DATE '1900-01-01') as STRING))
         AND trim(CAST(coalesce(tgt.requisition_num, -999) as STRING)) = trim(CAST(coalesce(stg.requisition_num, -999) as STRING))
         AND coalesce(trim(tgt.source_system_code), '') = coalesce(trim(stg.source_system_code), '')
         AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
      WHERE tgt.sourcing_request_sid IS NULL
)stg);


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_core_dataset_name }}.sourcing_request
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

