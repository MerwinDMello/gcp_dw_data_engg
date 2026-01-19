BEGIN
DECLARE
  DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

BEGIN TRANSACTION;
DELETE
FROM
  {{ params.param_hr_core_dataset_name }}.span_code_detail
WHERE
  1=1;
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.span_code_detail (span_code,
    system_code,
    lawson_company_num,
    process_level_code,
    unit_num,
    coid,
    company_code,
    last_update_date,
    source_system_code,
    dw_last_update_date_time)
SELECT
  a.hca_span_code AS s_span_code,
  a.hca_syscode AS s_system_code,
  a.company AS s_lawson_company_num,
  a.process_level AS s_process_level_code,
  CONCAT(SUBSTR('00000', 1, 5 - LENGTH(TRIM(CAST(hca_unit AS STRING)))), TRIM(CAST(hca_unit AS STRING))) AS s_unit,
  CONCAT(SUBSTR('00000', 1, 5 - LENGTH(TRIM(CAST(hca_coid AS STRING)))), TRIM(CAST(hca_coid AS STRING))) AS s_coid,
  f.company_code AS s_company_code,
  a.date_stamp AS s_last_update_date,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.zsspancode AS a
LEFT OUTER JOIN
  {{ params.param_pub_views_dataset_name }}.fact_facility AS f
ON
  CONCAT(SUBSTR('00000', 1, 5 - LENGTH(UPPER(TRIM(CAST(hca_coid AS STRING))))), UPPER(TRIM(CAST(hca_coid AS STRING)))) = UPPER(TRIM(f.coid)) QUALIFY ROW_NUMBER() OVER (PARTITION BY s_span_code, s_system_code, s_lawson_company_num, s_process_level_code, s_unit ORDER BY s_span_code, s_system_code, s_lawson_company_num, s_process_level_code, s_unit DESC) = 1 ;
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Span_Code,
      System_Code,
      Lawson_Company_Num,
      Process_Level_Code,
      Unit_Num
    FROM
      {{ params.param_hr_core_dataset_name }}.span_code_detail
    GROUP BY
      Span_Code,
      System_Code,
      Lawson_Company_Num,
      Process_Level_Code,
      Unit_Num
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.span_code_detail');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;
