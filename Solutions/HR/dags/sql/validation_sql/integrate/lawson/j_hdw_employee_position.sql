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
select  
  *
  from
{{ params.param_hr_stage_dataset_name }}.paemppos pos
inner join (select employee_sid,employee_num,lawson_company_num,source_system_code from {{ params.param_hr_base_views_dataset_name }}.employee where date(valid_to_date) = '9999-12-31' group by 1,2,3,4 )emp
on ((pos.employee) =  (emp.employee_num) and
(pos.company) = (emp.lawson_company_num))
left outer join (select position_sid,position_code,lawson_company_num,source_system_code from {{ params.param_hr_base_views_dataset_name }}.job_position where date(valid_to_date) = '9999-12-31' group by 1,2,3,4 )jp
on ((pos.position) =  (jp.position_code) and
(pos.company) = (jp.lawson_company_num))
left outer join (select dept_sid,dept_code,process_level_code,lawson_company_num,source_system_code from {{ params.param_hr_base_views_dataset_name }}.department where date(valid_to_date) = '9999-12-31' group by 1,2,3,4,5 )dpt
on ((pos.department) =  (dpt.dept_code) and
(pos.process_level) = (dpt.process_level_code) and
(pos.company) = (dpt.lawson_company_num))
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.employee_position
where date(valid_to_date) = '9999-12-31' and delete_ind = 'A' AND source_system_code = 'L'
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




