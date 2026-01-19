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
tgttablename = 'edwhr.address';
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
        CAST(xwlk.sk AS INT64) AS addr_sid,
        stg.addr_type_code,
        stg.addr_line_1_text,
        stg.addr_line_2_text,
        stg.addr_line_3_text,
        stg.addr_line_4_text,
        stg.city_name,
        stg.state_code,
        stg.zip_code,
        stg.county_name,
        stg.country_code,
        stg.location_code,
        stg.source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_address_wrk1 AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON concat(trim(stg.addr_type_code), '-', trim(coalesce(stg.addr_line_1_text, '')), '-', trim(coalesce(stg.addr_line_2_text, '')), '-', trim(coalesce(stg.addr_line_3_text, '')), '-', trim(coalesce(stg.addr_line_4_text, '')), '-', trim(coalesce(stg.city_name, '')), '-', trim(coalesce(stg.state_code, '')), '-', trim(coalesce(stg.zip_code, '')), '-', trim(coalesce(stg.county_name, '')), '-', '', '-', trim(coalesce(stg.location_code, ''))) = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'ADDRESS'
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.address AS tgt for system_time as of timestamp(tableload_start_time,'US/Central') 
        ON xwlk.sk = tgt.addr_sid
      WHERE tgt.addr_sid IS NULL
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
      QUALIFY row_number() OVER (PARTITION BY addr_sid, stg.addr_type_code ORDER BY stg.source_system_code DESC) = 1
  
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.address
where dw_last_update_date_time >= tableload_start_time  - interval 1 minute
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




