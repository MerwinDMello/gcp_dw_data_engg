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
FROM
        {{ params.param_hr_stage_dataset_name }}.jobrelate AS stg
        INNER JOIN (
          SELECT
              job_position.position_sid,
              job_position.eff_from_date,
              job_position.position_code,
              job_position.lawson_company_num
            FROM
              {{ params.param_hr_core_dataset_name }}.job_position
            WHERE job_position.valid_to_date = DATETIME("9999-12-31 23:59:59") 
             AND upper(job_position.source_system_code) = 'L'
            QUALIFY row_number() OVER (PARTITION BY job_position.position_sid ORDER BY job_position.eff_from_date DESC) = 1
        ) AS jp ON stg.position = jp.position_code
         AND stg.company = jp.lawson_company_num
        LEFT OUTER JOIN (
          SELECT
              job_code.job_code_sid,
              job_code.eff_from_date,
              job_code.job_code,
              job_code.lawson_company_num
            FROM
              {{ params.param_hr_core_dataset_name }}.job_code
            WHERE job_code.valid_to_date = DATETIME("9999-12-31 23:59:59") 
             AND upper(job_code.source_system_code) = 'L'
            QUALIFY row_number() OVER (PARTITION BY job_code_sid ORDER BY eff_from_date DESC) = 1
        ) AS jd ON stg.company = jd.lawson_company_num
         AND stg.job_code = jd.job_code
        LEFT OUTER JOIN (
          SELECT
              hr_company.hr_company_sid,
              hr_company.valid_from_date,
              hr_company.lawson_company_num
            FROM
              {{ params.param_hr_core_dataset_name }}.hr_company
            WHERE hr_company.valid_to_date = DATETIME("9999-12-31 23:59:59") 
            QUALIFY row_number() OVER (PARTITION BY hr_company.hr_company_sid ORDER BY hr_company.valid_from_date DESC) = 1
        ) AS comp ON stg.company = comp.lawson_company_num
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.job_personnel_code
where date(valid_to_date) = '9999-12-31'  AND source_system_code = 'L'
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




