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
tgttablename = 'edwhr.offer';
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
from
        {{ params.param_hr_stage_dataset_name }}.offer_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.offer AS tgt for system_time as of timestamp(tableload_start_time,'US/Central')
        ON stg.offer_sid = tgt.offer_sid
         AND coalesce(tgt.offer_num, -999) = coalesce(stg.offer_num, -999)
         AND trim(CAST(coalesce(tgt.submission_sid, -999) as STRING)) = trim(CAST(coalesce(stg.submission_sid, -999) as STRING))
         AND coalesce(tgt.sequence_num, -999) = coalesce(stg.sequence_num, -999)
         AND upper(trim(coalesce(tgt.offer_name, 'X'))) = upper(trim(coalesce(stg.offer_name, 'X')))
         AND coalesce(tgt.accept_date, DATE '1901-01-01') = coalesce(stg.accept_date, DATE '1901-01-01')
         AND coalesce(tgt.start_date, DATE '1901-01-01') = coalesce(stg.start_date, DATE '1901-01-01')
         AND coalesce(tgt.extend_date, DATE '1901-01-01') = coalesce(stg.extend_date, DATE '1901-01-01')
         AND coalesce(tgt.last_modified_date, DATE '1901-01-01') = coalesce(stg.last_modified_date, DATE '1901-01-01')
         AND coalesce(tgt.last_modified_time, TIME '00:00:00') = coalesce(stg.last_modified_time, TIME '00:00:00')
         AND coalesce(tgt.capture_date, DATE '1901-01-01') = coalesce(stg.capture_date, DATE '1901-01-01')
         AND trim(CAST(coalesce(tgt.salary_amt, -999) as STRING)) = trim(CAST(coalesce(stg.salary_amt, -999) as STRING))
         AND trim(CAST(coalesce(tgt.sign_on_bonus_amt, -999) as STRING)) = trim(CAST(coalesce(stg.sign_on_bonus_amt, -999) as STRING))
         AND trim(CAST(coalesce(tgt.salary_pay_basis_amt, -999) as STRING)) = trim(CAST(coalesce(stg.salary_pay_basis_amt, -999) as STRING))
         AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND coalesce(tgt.hours_per_day_num, -999) = coalesce(stg.hours_per_day_num, -999)
         AND coalesce(trim(tgt.source_system_code), 'X') = coalesce(trim(stg.source_system_code), 'X')
      WHERE tgt.offer_sid IS NULL

));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.offer
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




