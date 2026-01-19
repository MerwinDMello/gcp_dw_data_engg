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
tgttablename = 'edwhr.candidate_person';
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
  {{ params.param_hr_stage_dataset_name }}.candidate_person_wrk AS stg
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.candidate_person AS tgt for system_time as of timestamp(tableload_start_time,'US/Central')
ON
  stg.candidate_sid = tgt.candidate_sid
  AND UPPER(TRIM(COALESCE(tgt.first_name, ''))) = UPPER(TRIM(COALESCE(stg.first_name, '')))
  AND UPPER(TRIM(COALESCE(tgt.middle_name, ''))) = UPPER(TRIM(COALESCE(stg.middle_name, '')))
  AND UPPER(TRIM(COALESCE(tgt.last_name, ''))) = UPPER(TRIM(COALESCE(stg.last_name, '')))
  AND UPPER(TRIM(COALESCE(tgt.social_security_num, ''))) = UPPER(TRIM(COALESCE(stg.social_security_num, '')))
  AND UPPER(TRIM(COALESCE(tgt.email_address, ''))) = UPPER(TRIM(COALESCE(stg.email_address, '')))
  AND COALESCE(tgt.birth_date, DATETIME("9999-12-31 23:59:59")) = COALESCE(stg.birth_date, DATETIME("9999-12-31 23:59:59"))
  AND COALESCE(TRIM(tgt.source_system_code), '') = COALESCE(TRIM(stg.source_system_code), '')
  AND UPPER(COALESCE(TRIM(tgt.maiden_name), '')) = UPPER(COALESCE(TRIM(stg.maiden_name), ''))
  AND COALESCE(TRIM(tgt.driver_license_num), '') = COALESCE(TRIM(stg.driver_license_num), '')
  AND COALESCE(TRIM(tgt.driver_license_state_code), '') = COALESCE(TRIM(stg.driver_license_state_code), '')
  AND tgt.valid_to_date =DATETIME("9999-12-31 23:59:59")
WHERE
  tgt.candidate_sid IS NULL

));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.candidate_person
where valid_to_date =DATETIME("9999-12-31 23:59:59") and dw_last_update_date_time >= tableload_start_time  - interval 1 minute
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




