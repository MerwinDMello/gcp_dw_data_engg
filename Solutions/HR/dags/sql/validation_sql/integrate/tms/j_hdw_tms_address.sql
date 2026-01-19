
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
(SELECT count(*)
FROM
(SELECT '1' AS cnt
  FROM
(SELECT stg.sk AS addr_sid,
        stg.addr_type_cd AS addr_type_code,
        stg.addr1 addr_line_1_text,
        stg.addr2 AS addr_line_2_text,
        stg.addr3 AS addr_line_3_text,
        stg.addr4 AS addr_line_4_text,
        stg.city AS city_name,
        stg.state AS state_code,
        stg.zip AS zip_code,
        stg.county AS county_name,
        stg.country_code AS country_code,
        stg.location_code AS location_code,
        stg.source_system_code AS source_system_code,
        current_ts AS dw_last_update_date_time
FROM
(
  SELECT DISTINCT
      xwlk.sk,
      'PWR' AS addr_type_cd,
      trim(coalesce(pwr.work_history_address, '')) AS addr1,
      CAST(NULL as STRING) AS addr2,
      CAST(NULL as STRING) AS addr3,
      CAST(NULL as STRING) AS addr4,
      trim(coalesce(pwr.work_history_city, '')) AS city,
      '' AS state,
      trim(coalesce(pwr.work_history_postal_code, '')) AS zip,
      '' AS county,
      trim(coalesce(pwr.work_history_country, '')) AS country_code,
      CAST(NULL as STRING) AS location_code,
      'M' AS source_system_code
    FROM
      {{ params.param_hr_stage_dataset_name }}.work_history_report AS pwr
      INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON upper(concat('PWR', '-', trim(coalesce(pwr.work_history_address, '')), '-', trim(coalesce(pwr.work_history_city, '')), '-', trim(coalesce(pwr.work_history_postal_code, '')), '-', trim(coalesce(pwr.work_history_country, '')))) = upper(xwlk.sk_source_txt)
        AND upper(xwlk.sk_type) = 'ADDRESS'
) AS stg
LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.address
AS tgt
for system_time as of timestamp(tableload_start_time,'US/Central') 
ON stg.sk = tgt.addr_sid
WHERE tgt.addr_sid IS NULL
      GROUP BY 1,
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
               14) p) o);

SET
tgt_rec_count =
(SELECT count(*)
FROM {{ params.param_hr_core_dataset_name }}.address
WHERE dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE);

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
