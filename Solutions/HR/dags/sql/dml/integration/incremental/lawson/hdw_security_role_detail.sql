  /*Delete from Core Table- This is Nightly Truncate*/
BEGIN
DECLARE
  DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);
BEGIN TRANSACTION;

DELETE
FROM
  {{ params.param_hr_core_dataset_name }}.security_role_detail
WHERE
  1=1;
  
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.security_role_detail (lawson_company_num,
    process_level_code,
    access_role_code,
    span_code,
    dept_code,
    view_only_span_code,
    last_update_date,
    source_system_code,
    dw_last_update_date_time)
SELECT
  znsecurity.company AS lawson_company_num,
  znsecurity.process_level AS process_level_code,
  znsecurity.hca_accessrole AS access_role_code,
  znsecurity.hca_span_code AS span_code,
  CAST(znsecurity.hca_dept AS STRING) AS dept_code,
  trim(znsecurity.hca_inq_spancd) AS view_only_span_code,
  znsecurity.date_stamp AS last_update_date,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.znsecurity QUALIFY ROW_NUMBER() OVER (PARTITION BY lawson_company_num, process_level_code, access_role_code, span_code, znsecurity.hca_dept ORDER BY lawson_company_num, process_level_code, access_role_code, span_code, znsecurity.hca_dept DESC) = 1 ;
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Lawson_Company_Num,
      Process_Level_Code,
      Access_Role_Code,
      Span_Code,
      Dept_Code
    FROM
      {{ params.param_hr_core_dataset_name }}.security_role_detail
    GROUP BY
      Lawson_Company_Num,
      Process_Level_Code,
      Access_Role_Code,
      Span_Code,
      Dept_Code
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.security_role_detail');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;