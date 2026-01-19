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
  e.employee_sid,
  p.position_sid,
  z.effect_date AS eff_from_date,
  TRIM(z.hca_span_code) AS span_code,
  TRIM(z.a_field) AS access_role_code,
  TRIM(z.job_code),
  z.employee AS employee_num,
  TRIM(g.coid),
  TRIM(g.company_code),
  TRIM(CAST(z.hca_dept AS STRING)) AS dept_code,
  TRIM(z.position_ud) AS position_code,
  TRIM(z.comp_nbr) AS employee_34_login_code,
  z.company AS lawson_company_num,
  TRIM(z.process_level) AS process_level_code,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.zvuserdata AS z
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.employee AS e
ON
  UPPER(TRIM(CAST(z.employee AS STRING))) = UPPER(TRIM(CAST(e.employee_num AS STRING)))
  AND UPPER(TRIM(CAST(z.company AS STRING))) = UPPER(TRIM(CAST(e.lawson_company_num AS STRING)))
  AND date(e.valid_to_date) = "9999-12-31"
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.job_position AS p
ON
  UPPER(TRIM(CAST(z.position_ud AS STRING))) = UPPER(TRIM(CAST(p.position_code AS STRING)))
  AND UPPER(TRIM(CAST(z.company AS STRING))) = UPPER(TRIM(CAST(p.lawson_company_num AS STRING)))
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.gl_lawson_dept_crosswalk AS g
ON
  UPPER(TRIM(CAST(e.gl_company_num AS STRING))) = UPPER(TRIM(CAST(g.gl_company_num AS STRING)))
  AND UPPER(TRIM(CAST(e.account_unit_num AS STRING))) = UPPER(TRIM(CAST(g.account_unit_num AS STRING)))
WHERE
  z.effect_date IS NOT NULL
  AND e.employee_sid IS NOT NULL
  AND p.position_sid IS NOT NULL
  AND z.company IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY p.position_sid, e.employee_sid, effect_date ORDER BY p.position_sid, e.employee_sid, effect_date DESC) = 1 
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.employee_position_security_role
where  dw_last_update_date_time >= tableload_start_time  - interval 1 minute
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




