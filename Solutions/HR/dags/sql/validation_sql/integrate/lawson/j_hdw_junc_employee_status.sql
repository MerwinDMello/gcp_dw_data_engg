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
select * from 
{{ params.param_hr_stage_dataset_name }}.junc_employee_status_wrk
      WHERE concat(junc_employee_status_wrk.employee_sid,junc_employee_status_wrk.status_sid,upper(trim(junc_employee_status_wrk.status_type_code)),upper(trim(junc_employee_status_wrk.status_code)),junc_employee_status_wrk.employee_num,junc_employee_status_wrk.source_system_code) NOT IN(
        SELECT 
            concat(junc_employee_status.employee_sid
            ,junc_employee_status.status_sid
            ,upper(trim(junc_employee_status.status_type_code))
            ,upper(trim(junc_employee_status.status_code))
            ,junc_employee_status.employee_num
            ,junc_employee_status.source_system_code)
          FROM
            {{ params.param_hr_core_dataset_name }}.junc_employee_status for system_time as of timestamp(tableload_start_time,'US/Central') 
          WHERE junc_employee_status.valid_to_date = datetime("9999-12-31 23:59:59")
      )
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_core_dataset_name }}.junc_employee_status
where date(valid_to_date) = '9999-12-31' and dw_last_update_date_time >= tableload_start_time  - interval 1 minute 
AND source_system_code = 'L'
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




