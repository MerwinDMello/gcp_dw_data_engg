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
tolerance_percent = 5;
SET
src_rec_count = 
(select count(*)
from 
(
SELECT
  company_pay_grade_schedule_wrk1.company_pay_schedule_sid,
  company_pay_grade_schedule_wrk1.pay_grade_code,
  company_pay_grade_schedule_wrk1.pay_step_num,
  company_pay_grade_schedule_wrk1.eff_from_date,
  current_ts,
  datetime("9999-12-31 23:59:59"),
  company_pay_grade_schedule_wrk1.pay_schedule_code,
  company_pay_grade_schedule_wrk1.grade_sequence_num,
  company_pay_grade_schedule_wrk1.pay_rate_amt,
  company_pay_grade_schedule_wrk1.lawson_company_num,
  '00000' AS process_level_code,
  company_pay_grade_schedule_wrk1.active_dw_ind,
  company_pay_grade_schedule_wrk1.source_system_code,
  current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.company_pay_grade_schedule_wrk1
WHERE
  (
    concat(trim(cast(company_pay_grade_schedule_wrk1.company_pay_schedule_sid as string )),
    trim(company_pay_grade_schedule_wrk1.pay_grade_code),
    trim(cast(company_pay_grade_schedule_wrk1.pay_step_num as string)),
    upper(trim(coalesce(cast(company_pay_grade_schedule_wrk1.grade_sequence_num as string),''))),
    upper(trim(coalesce(cast(company_pay_grade_schedule_wrk1.pay_rate_amt as string),''))),
    company_pay_grade_schedule_wrk1.eff_from_date)
  ) NOT IN(
    SELECT
      concat(trim(cast(company_pay_schedule_sid as string)),
      trim(cast(pay_grade_code as string)),
      trim(cast(pay_step_num as string)),
      upper(trim(coalesce(cast(grade_sequence_num as string), ''))),
      upper(trim(coalesce(cast(pay_rate_amt as string), ''))),
      eff_from_date)
    FROM
      {{ params.param_hr_core_dataset_name }}.company_pay_grade_schedule  for system_time as of timestamp(tableload_start_time,'US/Central') 
    WHERE
      upper(active_dw_ind) = 'Y'
      and upper(source_system_code) = 'L'
      and dw_last_update_date_time < tableload_start_time  - interval 1 minute
  )

));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.company_pay_grade_schedule
WHERE
      upper(active_dw_ind) = 'Y'
      and upper(source_system_code) = 'L'
      and dw_last_update_date_time >= tableload_start_time  - interval 1 minute);

SET
difference = CASE 
              WHEN src_rec_count <> 0 Then CAST(((ABS(tgt_rec_count - src_rec_count)/src_rec_count) * 100) AS INT64)
              WHEN src_rec_count = 0 and tgt_rec_count = 0 Then 0
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




