CREATE TEMPORARY TABLE vtl1 AS
SELECT
  stg.candidate,
  stg.device_type,
  stg.device_num,
  stg.valid_from_date,
  stg.source_system_code
FROM (
  SELECT
    stg_0.candidate,
    stg_0.device_type,
    stg_0.device_num,
    stg_0.valid_from_date,
    stg_0.source_system_code
  FROM (
    SELECT
      stg_1.candidate,
      CASE
        WHEN UPPER(stg_1.device_type) = 'HOMEPHONE' THEN 'HOME'
        WHEN UPPER(stg_1.device_type) = 'MOBILEPHONE' THEN 'CELL'
        WHEN UPPER(stg_1.device_type) = 'WORKPHONE' THEN 'WORK'
    END
      AS device_type,
      stg_1.device_num AS device_num,
      stg_1.file_date AS valid_from_date,
      'T' AS source_system_code
    FROM (
      SELECT
        cast(tmp.number as int64) AS candidate,
        tmp.device_type,
        SUBSTR(TRIM(tmp.device_value), 1, 50) AS device_num,
        tmp.file_date,
        COALESCE(CAST(SUBSTR(TRIM(CAST(tmp.lastmodifieddate AS STRING)), 1, 19) AS DATETIME), DATETIME '1901-01-01 00:00:00') AS last_modified_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_candidate UNPIVOT(device_value FOR device_type IN (taleo_candidate.homephone AS 'homephone',
            taleo_candidate.mobilephone AS 'mobilephone',
            taleo_candidate.workphone AS 'workphone')) AS tmp
      WHERE
        (tmp.number) IS NOT NULL ) AS stg_1 QUALIFY ROW_NUMBER() OVER (PARTITION BY stg_1.candidate, stg_1.device_type ORDER BY UNIX_SECONDS(cast(stg_1.last_modified_date_time as timestamp)) DESC) = 1 ) AS stg_0
  UNION DISTINCT
  SELECT
    CAST(stg_0.candidate as INT64),
    stg_0.device_type,
    stg_0.device_num,
    stg_0.valid_from_date AS valid_from_date,
    stg_0.source_system_code
  FROM (
    SELECT
      stg_1.candidate ,
      CASE
        WHEN stg_1.device_type = 'primarycontactinfo_telephonenumber_subscribernumber' THEN 'HOME'
        WHEN stg_1.device_type = 'primarycontactinfo_mobilephone_mobilephonenumber_subscribernumber' THEN 'CELL'
        WHEN stg_1.device_type = 'primarycontactinfo_alternatephone_subscribernumber' THEN 'WORK'
    END
      AS device_type,
      stg_1.device_num AS device_num,
      CURRENT_DATE('US/Central') AS valid_from_date,
      'B' AS source_system_code
    FROM (
      SELECT
        tmp.candidate ,
        tmp.device_type,
        SUBSTR(TRIM(tmp.device_value), 1, 50) AS device_num,
        COALESCE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', SUBSTR(TRIM(CAST(tmp.updatestamp AS STRING)), 1, 19)), DATETIME '1901-01-01 01:01:01') AS last_modified_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_candidate_stg UNPIVOT(device_value FOR device_type 
         IN (ats_cust_v2_candidate_stg.primarycontactinfo_telephonenumber_subscribernumber,
             ats_cust_v2_candidate_stg.primarycontactinfo_mobilephone_mobilephonenumber_subscribernumber,
            ats_cust_v2_candidate_stg.primarycontactinfo_alternatephone_subscribernumber
 )) AS tmp
      WHERE
       tmp.candidate IS NOT NULL and coalesce(trim(upper(tmp.device_value)), '')  <> '') AS stg_1 QUALIFY ROW_NUMBER() OVER (PARTITION BY stg_1.candidate, stg_1.device_type ORDER BY UNIX_SECONDS(CAST(stg_1.last_modified_date_time AS TIMESTAMP)) DESC) = 1 ) AS stg_0 ) AS stg ;

         /*  Truncate Worktable Table     */
TRUNCATE TABLE
  {{ params.param_hr_stage_dataset_name }}.junc_candidate_communication_device_wrk;
BEGIN
DECLARE
  DUP_COUNT INT64;
-- BEGIN TRANSACTION; /*  Load Work Table with working Data */
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.junc_candidate_communication_device_wrk (communication_device_sid,
    candidate_sid,
    communication_device_type_code,
    valid_from_date,
    valid_to_date,
    source_system_code,
    dw_last_update_date_time)
SELECT
  x.communication_device_sid,
  x.candidate_sid,
  x.communication_device_type_code,
  x.valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  x.source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM (
  SELECT
    tgt.communication_device_sid AS communication_device_sid,
    can.candidate_sid AS candidate_sid,
    stg.device_type AS communication_device_type_code,
    stg.valid_from_date AS valid_from_date,
    stg.source_system_code AS source_system_code
  FROM
    vtl1 AS stg
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.communication_device AS tgt
  ON
    UPPER(COALESCE(stg.device_num, 'AAAAAA')) = UPPER(COALESCE(tgt.communication_device_value, 'AAAAAA'))
  INNER JOIN
    {{ params.param_hr_base_views_dataset_name }}.candidate AS can
  ON
    COALESCE(stg.candidate, 0) = COALESCE(can.candidate_num, 0)
    AND UPPER(can.source_system_code) = UPPER(stg.source_system_code)
    AND can.valid_to_date = DATETIME("9999-12-31 23:59:59") ) AS x ; 
	
	/* Test Unique Primary Index constraint set in Terdata */
-- SET
--   DUP_COUNT = (
--   SELECT
--     COUNT(*)
--   FROM (
--     SELECT
--       communication_device_sid,candidate_sid,communication_device_type_code,valid_from_date
--     FROM
--       {{ params.param_hr_stage_dataset_name }}.junc_candidate_communication_device_wrk
--     GROUP BY
--      communication_device_sid,candidate_sid,communication_device_type_code,valid_from_date
--     HAVING
--       COUNT(*) > 1 ) );
-- IF
--   DUP_COUNT <> 0 THEN
-- ROLLBACK TRANSACTION; RAISE
-- USING
--   MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_hr_stage_dataset_name }}.junc_candidate_communication_device_wrk');
--   ELSE
-- COMMIT TRANSACTION;
-- END IF
--   ;
END
  ;
DROP TABLE
  vtl1;