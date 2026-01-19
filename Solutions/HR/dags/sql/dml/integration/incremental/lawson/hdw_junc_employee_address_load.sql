BEGIN DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET
  current_ts = datetime_trunc(current_datetime('US/Central'), second);

/*  TRUNCATE WORK TABLE     */
TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_address_wrk;

/*  INSERT THE RECORDS INTO THE WORK TABLE  */
BEGIN TRANSACTION;

INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.employee_address_wrk (
    employee_sid,
    addr_type_code,
    addr_sid,
    employee_num,
    valid_from_date,
    valid_to_date,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  ee.employee_sid AS employee_sid,
  'EMP' AS addr_type_code,
  ead.addr_sid AS address_sid,
  empl.employee AS employee_num,
  /*FOR FIRST LOAD POPULATE DATE_HIRED/ADJ_HIRE_DATE OR A LOWER VALUE OF EFF_FROM_DATE*/
  /*CASE WHEN TGT.EMPLOYEE_SID IS NULL AND TRIM(EMPL.ADJ_HIRE_DATE) <> '' AND  EMPL.ADJ_HIRE_DATE > '1900-01-01' AND EMPL.ADJ_HIRE_DATE <> DATE '1950-01-01' THEN EMPL.ADJ_HIRE_DATE
   WHEN TGT.EMPLOYEE_SID IS NULL AND TRIM(EMPL.ADJ_HIRE_DATE) = '' THEN 
   CASE WHEN TRIM(EMPL.DATE_HIRED) <> '' AND  EMPL.DATE_HIRED > '1900-01-01' AND EMPL.DATE_HIRED <> DATE '1950-01-01' THEN EMPL.DATE_HIRED
   ELSE DATE '1900-01-01'
   END
   ELSE DATE '1900-01-01'
   END AS EFF_FROM_DATE*/
  --current_date() AS valid_from_date,
  --DATE '9999-12-31' AS valid_to_date,
  current_ts AS valid_from_date,
  datetime("9999-12-31 23:59:59") AS valid_to_date,
  empl.company AS lawson_company_num,
  /*,CASE WHEN TRIM(EMPL.PROCESS_LEVEL) IS NULL OR TRIM(EMPL.PROCESS_LEVEL) = '' THEN '00000' END AS PROCESS_LEVEL_CODE */
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
  10;

/*UNION ALL
 SELECT
 ee.employee_sid AS employee_sid,
 'EMP' AS addr_type_code,
 ead.addr_sid AS address_sid,
 empl.employee AS employee_num,
 current_date() AS valid_from_date,
 DATE '9999-12-31' AS valid_to_date,
 empl.company AS lawson_company_num,
 /*,CASE WHEN TRIM(EMPL.PROCESS_LEVEL) IS NULL OR TRIM(EMPL.PROCESS_LEVEL) = '' THEN '00000' END AS PROCESS_LEVEL_CODE
 CASE
 WHEN trim(empl.process_level) IS NULL
 OR trim(empl.process_level) = '' THEN '00000'
 ELSE concat(substr('00000', 1, 5 - length(trim(empl.process_level))), trim(empl.process_level))
 END AS process_level_code,
 'A' AS source_system_code,
 datetime_trunc(current_datetime(), SECOND) AS dw_last_update_date_time
 FROM
 {{ params.param_hr_stage_dataset_name }} .msh_employee_stg AS empl
 INNER JOIN (
 SELECT
 employee.employee_sid,
 employee.employee_num,
 employee.lawson_company_num
 FROM
 {{ params.param_hr_base_views_dataset_name }}.employee
 WHERE upper(employee.valid_to_date) = '9999-12-31'
 AND upper(employee.source_system_code) = 'A'
 GROUP BY 1, 2, 3
 ) AS ee ON trim(empl.employee) = trim(ee.employee_num)
 AND trim(empl.company) = trim(ee.lawson_company_num)
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
 WHERE upper(address.addr_type_code) = 'EMP'
 AND upper(address.source_system_code) = 'A'
 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
 ) AS ead ON trim(empl.addr1) = trim(ead.addr_line_1_text)
 AND trim(empl.addr2) = trim(ead.addr_line_2_text)
 AND trim(empl.addr3) = trim(ead.addr_line_3_text)
 AND trim(empl.addr4) = trim(ead.addr_line_4_text)
 AND trim(empl.city) = trim(ead.city_name)
 AND trim(empl.state) = trim(ead.state_code)
 AND trim(empl.zip) = trim(ead.zip_code)
 AND trim(empl.county) = trim(ead.county_name)
 AND trim(empl.country_code) = trim(ead.country_code)
 UNION ALL
 SELECT
 ee.employee_sid,
 'LOC' AS addr_type_code,
 ead.addr_sid AS address_sid,
 pemp.employee AS employee_num,
 current_date() AS valid_from_date,
 DATE '9999-12-31' AS valid_to_date,
 pemp.company AS lawson_company_num,
 '00000' AS process_level_code,
 'A' AS source_system_code,
 datetime_trunc(current_datetime(), SECOND) AS dw_last_update_date_time
 FROM
 {{ params.param_hr_stage_dataset_name }} .msh_paemployee_stg AS pemp
 INNER JOIN (
 SELECT
 employee.employee_sid,
 employee.employee_num,
 employee.lawson_company_num
 FROM
 {{ params.param_hr_base_views_dataset_name }}.employee
 WHERE upper(employee.valid_to_date) = '9999-12-31'
 AND upper(employee.source_system_code) = 'A'
 GROUP BY 1, 2, 3
 ) AS ee ON trim(pemp.employee) = trim(ee.employee_num)
 AND trim(pemp.company) = trim(ee.lawson_company_num)
 INNER JOIN {{ params.param_hr_stage_dataset_name }} .msh_pcodesdtl_stg AS pcod ON trim(coalesce(pemp.locat_code, 'X')) = trim(pcod.code)
 AND upper(pcod.r_type) = 'LO'
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
 WHERE upper(address.addr_type_code) = 'LOC'
 AND upper(address.source_system_code) = 'A'
 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
 ) AS ead ON trim(pcod.addr1) = trim(ead.addr_line_1_text)
 AND trim(pcod.addr2) = trim(ead.addr_line_2_text)
 AND trim(pcod.addr3) = trim(ead.addr_line_3_text)
 AND trim(pcod.addr4) = trim(ead.addr_line_4_text)
 AND trim(pcod.city) = trim(ead.city_name)
 AND trim(pcod.state) = trim(ead.state_code)
 AND trim(pcod.zip) = trim(ead.zip_code)
 AND trim(pcod.county) = trim(ead.county_name)
 AND trim(pcod.country_code) = trim(ead.country_code)
 AND upper(trim(coalesce(pcod.code, 'X'))) = upper(trim(coalesce(ead.location_code, 'X')))
 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10  */
/* Updating junc_employee_address */
UPDATE
  {{ params.param_hr_core_dataset_name }}.junc_employee_address AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  dw_last_update_date_time = current_ts
WHERE
  tgt.valid_to_date = datetime("9999-12-31 23:59:59")
  AND (tgt.employee_sid || tgt.addr_sid) NOT IN(
    SELECT
      employee_sid || addr_sid
    FROM
      {{ params.param_hr_stage_dataset_name }}.employee_address_wrk
  )
  AND trim(tgt.source_system_code) = 'L';

UPDATE
  {{ params.param_hr_core_dataset_name }}.junc_employee_address AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.employee_address_wrk AS stg
WHERE
  tgt.employee_sid = stg.employee_sid
  AND tgt.addr_sid = stg.addr_sid
  AND upper(trim(tgt.addr_type_code)) = upper(trim(stg.addr_type_code))
  AND (
    tgt.lawson_company_num <> stg.lawson_company_num
    OR trim(tgt.process_level_code) <> trim(stg.process_level_code)
    OR tgt.employee_num <> stg.employee_num
  )
  AND tgt.valid_to_date = datetime("9999-12-31 23:59:59")
  AND trim(tgt.source_system_code) = 'L';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.junc_employee_address (
    employee_sid,
    addr_type_code,
    addr_sid,
    employee_num,
    valid_from_date,
    valid_to_date,
    delete_ind,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  employee_address_wrk.employee_sid,
  employee_address_wrk.addr_type_code,
  employee_address_wrk.addr_sid,
  employee_address_wrk.employee_num,
  current_ts as valid_from_date,
  datetime("9999-12-31 23:59:59") as valid_to_date,
  'A' AS delete_ind,
  employee_address_wrk.lawson_company_num,
  employee_address_wrk.process_level_code,
  employee_address_wrk.source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.employee_address_wrk
WHERE
  (
    employee_address_wrk.employee_sid || employee_address_wrk.addr_sid || employee_address_wrk.employee_num || trim(employee_address_wrk.addr_type_code) || employee_address_wrk.lawson_company_num || trim(employee_address_wrk.process_level_code)
  ) NOT IN(
    SELECT
      junc_employee_address.employee_sid || junc_employee_address.addr_sid || junc_employee_address.employee_num ||trim(junc_employee_address.addr_type_code) || junc_employee_address.lawson_company_num || trim(junc_employee_address.process_level_code)
    FROM
      {{ params.param_hr_base_views_dataset_name }}.junc_employee_address
    WHERE
      junc_employee_address.valid_to_date = datetime("9999-12-31 23:59:59")
  );

UPDATE
  {{ params.param_hr_core_dataset_name }}.junc_employee_address AS empl
SET
  delete_ind = 'D',
  valid_to_date = current_ts - INTERVAL 1 SECOND
WHERE
  upper(empl.delete_ind) = 'A'
  AND (empl.lawson_company_num || empl.employee_num) NOT IN(
    SELECT
      DISTINCT employee.company || employee.employee
    FROM
      {{ params.param_hr_stage_dataset_name }}.employee
      /*UNION DISTINCT
       SELECT DISTINCT AS STRUCT
       msh_employee_stg.company,
       msh_employee_stg.employee
       FROM
       {{ params.param_hr_stage_dataset_name }} .msh_employee_stg*/
  )
  AND trim(empl.source_system_code) = 'L';

UPDATE
  {{ params.param_hr_core_dataset_name }}.junc_employee_address AS empl
SET
  delete_ind = 'A'
WHERE
  upper(empl.delete_ind) = 'D'
  AND (empl.lawson_company_num || empl.employee_num) IN(
    SELECT
      DISTINCT employee.company || employee.employee
    FROM
      {{ params.param_hr_stage_dataset_name }}.employee
      /*UNION DISTINCT
       SELECT DISTINCT AS STRUCT
       msh_employee_stg.company,
       msh_employee_stg.employee
       FROM
       {{ params.param_hr_stage_dataset_name }} .msh_employee_stg*/
  )
  AND trim(empl.source_system_code) = 'L';

/* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
    select
      count(*)
    from
      (
        select
          employee_sid,
          addr_sid,
          valid_from_date
        from
          {{ params.param_hr_core_dataset_name }}.junc_employee_address
        group by
          employee_sid,
          addr_sid,
          valid_from_date
        having
          count(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat(
  'Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.junc_employee_address'
);

ELSE COMMIT TRANSACTION;

END IF;

END;