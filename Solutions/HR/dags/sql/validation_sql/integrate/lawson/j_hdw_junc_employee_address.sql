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
  ee.employee_sid AS employee_sid,
  'EMP' AS addr_type_code,
  ead.addr_sid AS address_sid,
  empl.employee AS employee_num,
  current_ts AS valid_from_date,
  datetime("9999-12-31 23:59:59") AS valid_to_date,
  empl.company AS lawson_company_num,
  empl.process_level AS process_level_code,
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
      employee.valid_to_date = datetime("9999-12-31 23:59:59")
      AND upper(trim(employee.source_system_code)) = 'L'
    GROUP BY
      1,
      2,
      3
  ) AS ee ON empl.employee = ee.employee_num
  AND empl.company = ee.lawson_company_num
  INNER JOIN (
    SELECT
      address.addr_sid,
      address.addr_type_code,
      address.addr_line_1_text,
      address.addr_line_2_text,
      address.addr_line_3_text,
      address.addr_line_4_text,
      address.city_name,
      address.zip_code,
      address.county_name,
      address.state_code,
      address.country_code
    FROM
      {{ params.param_hr_base_views_dataset_name }}.address
    WHERE
      upper(address.addr_type_code) = 'EMP'
      AND upper(trim(address.source_system_code)) = 'L'
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
      11
  ) AS ead ON upper(trim(empl.addr1)) = upper(trim(ead.addr_line_1_text))
  AND upper(trim(empl.addr2)) = upper(trim(ead.addr_line_2_text))
  AND upper(trim(empl.addr3)) = upper(trim(ead.addr_line_3_text))
  AND upper(trim(empl.addr4)) = upper(trim(ead.addr_line_4_text))
  AND upper(trim(empl.city)) = upper(trim(ead.city_name))
  AND upper(trim(empl.state)) = upper(trim(ead.state_code))
  AND upper(trim(empl.zip)) = upper(trim(ead.zip_code))
  AND upper(trim(empl.county)) = upper(trim(ead.county_name))
  AND upper(trim(empl.country_code)) = upper(trim(ead.country_code))
UNION
ALL
SELECT
  ee.employee_sid,
  'LOC' AS addr_type_code,
  ead.addr_sid AS address_sid,
  pemp.employee AS employee_num,
  current_ts AS valid_from_date,
  datetime("9999-12-31 23:59:59") AS valid_to_date,
  pemp.company AS lawson_company_num,
  '00000' AS process_level_code,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.paemployee AS pemp
  INNER JOIN (
    SELECT
      employee.employee_sid,
      employee.employee_num,
      employee.lawson_company_num
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee
    WHERE
      employee.valid_to_date = datetime("9999-12-31 23:59:59")
      AND upper(trim(employee.source_system_code)) = 'L'
    GROUP BY
      1,
      2,
      3
  ) AS ee ON pemp.employee = ee.employee_num
  AND pemp.company = ee.lawson_company_num
  INNER JOIN {{ params.param_hr_stage_dataset_name }}.pcodesdtl AS pcod ON trim(coalesce(pemp.locat_code, 'X')) = trim(pcod.code)
  AND upper(pcod.type) = 'LO'
  INNER JOIN (
    SELECT
      address.addr_sid,
      address.addr_type_code,
      address.addr_line_1_text,
      address.addr_line_2_text,
      address.addr_line_3_text,
      address.addr_line_4_text,
      address.city_name,
      address.zip_code,
      address.county_name,
      address.state_code,
      address.country_code,
      address.location_code
    FROM
      {{ params.param_hr_base_views_dataset_name }}.address
    WHERE
      upper(address.addr_type_code) = 'LOC'
      AND upper(trim(address.source_system_code)) = 'L'
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
      12
  ) AS ead ON upper(trim(pcod.addr1)) = upper(trim(ead.addr_line_1_text))
  AND upper(trim(pcod.addr2)) = upper(trim(ead.addr_line_2_text))
  AND upper(trim(pcod.addr3)) = upper(trim(ead.addr_line_3_text))
  AND upper(trim(pcod.addr4)) = upper(trim(ead.addr_line_4_text))
  AND upper(trim(pcod.city)) = upper(trim(ead.city_name))
  AND upper(trim(pcod.state)) = upper(trim(ead.state_code))
  AND upper(trim(pcod.zip)) = upper(trim(ead.zip_code))
  AND upper(trim(pcod.county)) = upper(trim(ead.county_name))
  AND upper(trim(pcod.country_code)) = upper(trim(ead.country_code))
  AND upper(trim(coalesce(pcod.code, 'X'))) = upper(trim(coalesce(ead.location_code, 'X')))
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
  10
));


SET
tgt_rec_count =
(select count(*)
from {{ params.param_hr_base_views_dataset_name }}.junc_employee_address
where date(valid_to_date) = '9999-12-31' AND source_system_code = 'L'
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




