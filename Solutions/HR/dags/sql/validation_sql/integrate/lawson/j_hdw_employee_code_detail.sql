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
        coalesce(ee.employee_sid, 0) AS employee_sid,
        emp.type AS employee_type_code,
        emp.emp_app AS employee_sw,
        emp.code AS employee_code,
        CASE
          WHEN trim(emp.subject) = '' THEN '-'
          ELSE trim(emp.subject)
        END AS employee_code_subject_code,
        emp.seq_nbr AS employee_code_seq_num,
        current_ts AS valid_from_date,
        emp.employee AS employee_num,
        emp.date_acquired AS acquired_date,
        emp.renew_date,
        emp.date_returned AS certification_renew_date,
        emp.lic_number AS license_num_text,
        emp.profic_level AS proficiency_level_text,
        emp.verified_flag AS verified_ind,
        emp.instructor AS employee_code_detail_text,
        emp.co_sponsored AS company_sponsored_ind,
        emp.skill_source AS skill_source_code,
        emp.company AS lawson_company_num,
        '00000' AS process_level_code,
        emp.state AS state_code,
        datetime("9999-12-31 23:59:59") AS valid_to_date,
        'Y' AS active_dw_ind,
        'A' AS delete_ind,
        'L' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.empcodes AS emp
        INNER JOIN (
          SELECT
              employee.employee_sid,
              employee.employee_num,
              employee.lawson_company_num
            FROM
              {{ params.param_hr_core_dataset_name }}.employee for system_time as of timestamp(tableload_start_time,'US/Central') 
            WHERE valid_to_date = datetime("9999-12-31 23:59:59")
            GROUP BY 1, 2, 3
        ) AS ee ON emp.employee = ee.employee_num
         AND emp.company = ee.lawson_company_num
      WHERE emp.emp_app <> 1
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.employee_code_detail
where  active_dw_ind = 'Y') ;

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




