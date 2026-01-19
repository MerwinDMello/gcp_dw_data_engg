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
  {{ params.param_hr_stage_dataset_name }}.employee_requisition_wrk stg
  WHERE
  concat(stg.employee_sid,
    stg.requisition_sid,
    UPPER(TRIM(stg.action_type_code)),
    stg.eff_from_date,
    UPPER(TRIM(stg.action_code)),
    UPPER(TRIM(stg.user_id_text)),
    stg.work_unit_num,
    UPPER(TRIM(CAST(stg.lawson_company_num AS string))),
    UPPER(TRIM(stg.process_level_code)),
    stg.requisition_num,
    stg.employee_num) NOT IN(
  SELECT
    concat(tgt.employee_sid,
    tgt.requisition_sid,
    UPPER(TRIM(tgt.action_type_code)),
    tgt.eff_from_date,
    UPPER(TRIM(tgt.action_code)),
    UPPER(TRIM(tgt.user_id_text)),
    tgt.work_unit_num,
    UPPER(TRIM(CAST(tgt.lawson_company_num AS string))),
    UPPER(TRIM(tgt.process_level_code)),
    tgt.requisition_num,
    tgt.employee_num)
  FROM
    {{ params.param_hr_core_dataset_name }}.employee_requisition AS tgt for system_time as of timestamp(tableload_start_time,'US/Central') 
  WHERE
    DATE(tgt.valid_to_date) = "9999-12-31")
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.employee_requisition
where dw_last_update_date_time >= tableload_start_time  - interval 1 minute
and DATE(valid_to_date) = "9999-12-31"
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




