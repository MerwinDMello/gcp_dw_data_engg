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
  emp.employee_sid,
  stg.seq_nbr AS review_sequence_num,
  current_ts AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  emp1.employee_sid AS reviewer_employee_sid,
  stg.sched_date AS scheduled_review_date,
  TRIM(stg.code) AS review_type_code,
  stg.actual_date AS actual_review_date,
  TRIM(stg.rating) AS performance_rating_code,
  stg.date_stamp AS last_update_date,
  CAST(stg.time_stamp AS TIME) AS last_update_time,
  LEFT(TRIM(stg.user_id),7) AS last_updated_3_4_login_code,
  stg.total_score AS total_score_num,
  TRIM(stg.description) AS review_desc,
  TRIM(stg.rev_schedule) AS review_schedule_type_code,
  stg.next_review AS next_review_date,
  stg.next_rev_code AS next_review_code,
  stg.last_review AS last_review_date,
  CAST(stg.employee AS int64) AS employee_num,
  stg.company AS lawson_company_num,
  '00000' AS process_level_code,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.review AS stg
INNER JOIN
  {{ params.param_hr_base_views_dataset_name }}.employee AS emp
ON
  UPPER(TRIM(CAST(emp.employee_num AS string))) = UPPER(TRIM(stg.employee))
  AND emp.lawson_company_num = stg.company
  AND DATE(emp.valid_to_date) = "9999-12-31"
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.employee AS emp1
ON
  emp1.employee_num = stg.by_employee
  AND emp1.lawson_company_num = stg.company
  AND DATE(emp1.valid_to_date) = "9999-12-31"
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12,
  13,
  14,
  15,
  16,
  17,
  18,
  19,
  20,
  21,
  22,
  23 
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.employee_performance_review
where source_system_code = 'L'
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




