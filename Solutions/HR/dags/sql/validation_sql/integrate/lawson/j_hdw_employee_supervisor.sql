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
tolerance_percent = 0;
SET
src_rec_count = 
(select count(*)
from 
(
SELECT
  ee.employee_sid AS employee_sid,
  COALESCE(es.supervisor_sid, 0) AS supervisor_sid,
  current_ts AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  cast(trim(cast(empl.employee as string)) as int64) AS employee_num,
  trim(empl.supervisor) AS supervisor_code,
  cast(trim(cast(empl.company as string)) as int64) AS lawson_company_num,
  trim(empl.process_level) AS process_level_code,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.employee AS empl
INNER JOIN (
SELECT
    employee.employee_sid,
    employee.employee_num,
    employee.lawson_company_num
  FROM
    {{ params.param_hr_base_views_dataset_name }}.employee
  WHERE
    date(employee.valid_to_date) = ("9999-12-31")
    AND UPPER(employee.source_system_code) = 'L'
  GROUP BY
    1,
    2,
    3 ) AS ee
ON
  upper(trim(cast(empl.employee as string))) = upper(trim(cast(ee.employee_num as string)))
  AND upper(trim(cast(empl.company as string))) = upper(trim(cast(ee.lawson_company_num as string)))
LEFT OUTER JOIN (
  SELECT
    supervisor.supervisor_sid,
    supervisor.supervisor_code,
    supervisor.lawson_company_num
  FROM
    {{ params.param_hr_base_views_dataset_name }}.supervisor
  WHERE
    date(supervisor.valid_to_date) = "9999-12-31"
    AND UPPER(supervisor.source_system_code) = 'L'
  GROUP BY
    1,
    2,
    3 ) AS es
ON
  upper(trim(cast(empl.supervisor as string))) = upper(trim(cast(es.supervisor_code as string)))
  AND upper(trim(cast(empl.company as string))) = upper(trim(cast(es.lawson_company_num as string)))
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.employee_supervisor
where date(valid_to_date) = '9999-12-31' and delete_ind ='A' AND source_system_code = 'L'
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




