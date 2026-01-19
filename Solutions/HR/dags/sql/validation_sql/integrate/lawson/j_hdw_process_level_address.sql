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
  prs.company AS lawson_company_num,
  CASE
    WHEN coalesce(trim(prs.process_level), '') = '' THEN '00000'
    ELSE prs.process_level
  END AS process_level_code,
  concat(
    substr(
      '00000',
      1,
      5 - length(trim(cast(prs.company as string)))
    ),
    trim(cast(prs.company as string)),
    '-',
    CASE
      WHEN trim(prs.process_level) IS NULL
      OR trim(prs.process_level) = '' THEN '00000'
      ELSE concat(
        substr('00000', 1, 5 - length(trim(prs.process_level))),
        trim(prs.process_level)
      )
    END,
    '-',
    '00000'
  ) AS security_key_text,
  'L' AS source_system_code
FROM
  {{ params.param_hr_stage_dataset_name }}.prsystem AS prs
  INNER JOIN 
  {{ params.param_hr_base_views_dataset_name }}.process_level AS proc ON CASE
    WHEN coalesce(trim(prs.process_level), '') = '' THEN CAST(NULL as STRING)
    ELSE concat(
      substr('00000', 1, 5 - length(trim(prs.process_level))),
      trim(prs.process_level)
    )
  END = proc.process_level_code
  AND prs.company = proc.lawson_company_num
  AND upper(proc.source_system_code) = 'L'
  AND proc.valid_to_date = datetime("9999-12-31 23:59:59")
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.process_level_address
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




