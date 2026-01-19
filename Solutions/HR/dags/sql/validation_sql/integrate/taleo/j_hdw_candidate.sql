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
tgttablename = 'edwhr.candidate';
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
  {{ params.param_hr_stage_dataset_name }}.candidate_wrk AS stg
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.candidate AS tgt for system_time as of timestamp(tableload_start_time,'US/Central')
ON
  stg.candidate_sid = tgt.candidate_sid
  AND TRIM(CAST(COALESCE(tgt.candidate_num, -999) AS STRING)) = TRIM(CAST(COALESCE(stg.candidate_num, -999) AS STRING))
  AND TRIM(CAST(COALESCE(tgt.in_hiring_process_sw, 9) AS STRING)) = TRIM(CAST(COALESCE(stg.in_hiring_process_sw, 9) AS STRING))
  AND TRIM(CAST(COALESCE(tgt.internal_candidate_sw, 9) AS STRING)) = TRIM(CAST(COALESCE(stg.internal_candidate_sw, 9) AS STRING))
  AND TRIM(CAST(COALESCE(tgt.referred_sw, 9) AS STRING)) = TRIM(CAST(COALESCE(stg.referred_sw, 9) AS STRING))
  AND tgt.last_modified_date_time = stg.last_modified_date_time
  AND tgt.candidate_creation_date_time = stg.candidate_creation_date_time
  AND TRIM(CAST(COALESCE(tgt.residence_location_num, -999) AS STRING)) = TRIM(CAST(COALESCE(stg.residence_location_num, -999) AS STRING))
  AND COALESCE(TRIM(tgt.source_system_code), '') = COALESCE(TRIM(stg.source_system_code), '')
  AND COALESCE(TRIM(tgt.travel_preference_code), '') = COALESCE(TRIM(stg.travel_preference_code), '')
  AND COALESCE(TRIM(tgt.relocation_preference_code), '') = COALESCE(TRIM(stg.relocation_preference_code), '')
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
WHERE
  tgt.candidate_sid IS NULL 

));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.candidate
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




