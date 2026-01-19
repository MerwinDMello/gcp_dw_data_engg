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
tgttablename = 'edwhr.ref_posting_status';
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
        
        stgg.posting_status_code AS posting_status_codee
      FROM
        (
          SELECT DISTINCT
              substr(concat(trim(taleo_jobinformation.postingstatus), repeat(' ', 10)), 1, 10) AS posting_status_code,
              'T' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.taleo_jobinformation
          UNION DISTINCT
          SELECT DISTINCT
              CASE
                WHEN CASE
                   cast(ats_hcm_jobposting_stg.postingstatus as int64)
                  WHEN SAFE_CAST('' AS INT64) THEN 0
                  ELSE SAFE_CAST(ats_hcm_jobposting_stg.postingstatus as INT64)
                END = 1 THEN 'Not Posted'
                WHEN CASE
                   cast(ats_hcm_jobposting_stg.postingstatus as int64)
                  WHEN SAFE_CAST('' AS INT64) THEN 0
                  ELSE SAFE_CAST(ats_hcm_jobposting_stg.postingstatus as INT64)
                END = 2 THEN 'Posted    '
                WHEN CASE
                   cast(ats_hcm_jobposting_stg.postingstatus as int64)
                  WHEN SAFE_CAST('' AS INT64) THEN 0
                  ELSE SAFE_CAST(ats_hcm_jobposting_stg.postingstatus as INT64)
                END = 3 THEN 'Exported  '
                WHEN CASE
                   cast(ats_hcm_jobposting_stg.postingstatus as int64)
                  WHEN SAFE_CAST('' AS INT64) THEN 0
                  ELSE SAFE_CAST(ats_hcm_jobposting_stg.postingstatus as INT64)
                END = 4 THEN 'Submitted '
                ELSE '          '
              END AS posting_status_code,
              'B' AS source_system_code
            FROM
              {{ params.param_hr_stage_dataset_name }}.ats_hcm_jobposting_stg
        ) AS stgg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.ref_posting_status AS tgt for system_time as of timestamp(tableload_start_time,'US/Central')
		 ON coalesce(trim(tgt.posting_status_code), 'XXX') = coalesce(trim(stgg.posting_status_code), 'XXX')
         AND upper(trim(tgt.source_system_code)) = upper(trim(stgg.source_system_code))
      WHERE tgt.posting_status_id IS NULL

));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.ref_posting_status
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




