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
sourcesysnm ='kronos';
SET
srctablename = Null;
SET
tgttablename = concat('edwhr.','time_entry_pay_code_detail');
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
select
stg.employee_num,
stg.kronos_num,
stg.clock_library_code,
stg.kronos_pay_code_seq_num,
stg.valid_from_date,
stg.valid_to_date,
stg.kronos_pay_code,
stg.rounded_clocked_hour_num,
stg.pay_summary_group_code,
stg.lawson_company_num,
stg.process_level_code,
stg.source_system_code,
stg.dw_last_update_date_time
from {{ params.param_hr_stage_dataset_name }}.time_entry_pay_code_detail_wrk stg

left join {{ params.param_hr_core_dataset_name }}.time_entry_pay_code_detail as tgt for system_time as of timestamp(tableload_start_time,'US/Central') 

on tgt.employee_num = stg.employee_num

and tgt.kronos_num = stg.kronos_num
and tgt.clock_library_code = stg.clock_library_code
and tgt.kronos_pay_code_seq_num = stg.kronos_pay_code_seq_num
and date(tgt.valid_to_date) = "9999-12-31"

where tgt.employee_num is null
and tgt.kronos_num is null
and tgt.clock_library_code is null
and tgt.kronos_pay_code_seq_num is null 
)stg);


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.time_entry_pay_code_detail
where dw_last_update_date_time >= tableload_start_time  - interval 1 minute);

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




