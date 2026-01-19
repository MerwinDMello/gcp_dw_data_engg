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
        stg.company AS gl_company_num,
        stg.acct_unit AS account_unit_num,
        current_ts AS valid_from_date,
        datetime("9999-12-31 23:59:59") AS valid_to_date,
        stg.hca_coid AS coid,
        stg.hca_unit AS unit_num,
        cast(stg.hca_dept as string) AS dept_num,
        CASE
          WHEN trim(stg.process_level) IS NULL
           OR trim(stg.process_level) = '' THEN '00000'
          ELSE concat(substr('00000', 1, 5 - length(trim(stg.process_level))), trim(stg.process_level))
        END AS process_level_code,
        stg.hr_company AS lawson_company_num,
        
        concat(CASE
          WHEN stg.hr_company IS NULL
           OR trim(cast (stg.hr_company as string)) = '' THEN '00000'
          ELSE concat(substr('00000', 1, 5 - length(trim(cast(stg.hr_company as string)))), trim(cast(stg.hr_company as string)))
        END, '-', CASE
          WHEN trim(stg.process_level) IS NULL
           OR trim(stg.process_level) = '' THEN '00000'
          ELSE concat(substr('00000', 1, 5 - length(trim(stg.process_level))), trim(stg.process_level))
        END, '-', CASE
          WHEN trim(stg.acct_unit) IS NULL
           OR trim(stg.acct_unit) = '' THEN '00000'
          ELSE concat(substr('00000', 1, 5 - length(trim(stg.acct_unit))), trim(stg.acct_unit))
        END) AS security_key_text,
        'H' AS company_code,
        'L' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.zxauxref as stg
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk
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




