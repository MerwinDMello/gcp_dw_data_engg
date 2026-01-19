BEGIN
DECLARE
  current_ts DATETIME;
DECLARE
  DUP_COUNT INT64;
SET
  current_ts = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
BEGIN TRANSACTION;
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.communication_device (communication_device_sid,
    communication_device_value,
    source_system_code,
    dw_last_update_date_time)
SELECT
  (
  SELECT
    COALESCE(MAX(communication_device_sid), CAST(0 AS int64))
  FROM
    {{ params.param_hr_base_views_dataset_name }}.communication_device ) + CAST(ROW_NUMBER() OVER (ORDER BY UPPER(y.srctxt)) AS int64) AS communication_device_sid,
  y.srctxt AS communication_device_value,
  y.source_system_code,
  TIMESTAMP_TRUNC(CURRENT_DATETIME(), SECOND) AS dw_last_update_date_time
FROM (
  SELECT
    stg.srctxt AS srctxt,
    stg.source_system_code
  FROM ( (
      SELECT
        DISTINCT SUBSTR(TRIM(taleo_candidate.homephone), 1, 50) AS srctxt,
        'T' AS source_system_code
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_candidate
      UNION DISTINCT
      SELECT
        DISTINCT SUBSTR(TRIM(taleo_candidate.mobilephone), 1, 50) AS srctxt,
        'T' AS source_system_code
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_candidate
      UNION DISTINCT
      SELECT
        DISTINCT SUBSTR(TRIM(taleo_candidate.workphone), 1, 50) AS srctxt,
        'T' AS source_system_code
      FROM
        {{ params.param_hr_stage_dataset_name }}.taleo_candidate )
    UNION ALL (
      SELECT
        DISTINCT SUBSTR(TRIM(ats_cust_v2_candidate_stg.primarycontactinfo_telephonenumber_subscribernumber), 1, 50) AS srctxt,
        'B' AS source_system_code
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_candidate_stg
      UNION DISTINCT
      SELECT
        DISTINCT SUBSTR(TRIM(ats_cust_v2_candidate_stg.primarycontactinfo_mobilephone_mobilephonenumber_subscribernumber), 1, 50) AS srctxt,
        'B' AS source_system_code
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_candidate_stg
      UNION DISTINCT
      SELECT
        DISTINCT SUBSTR(TRIM(ats_cust_v2_candidate_stg.primarycontactinfo_alternatephone_subscribernumber), 1, 50) AS srctxt,
        'B' AS source_system_code
      FROM
        {{ params.param_hr_stage_dataset_name }}.ats_cust_v2_candidate_stg ) ) AS stg QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(srctxt)
    ORDER BY
      UPPER(stg.source_system_code) DESC) = 1 ) AS y
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.communication_device AS tgt
ON
  UPPER(COALESCE(y.srctxt, 'AAAAAA')) = UPPER(COALESCE(tgt.communication_device_value, 'AAAAAA'))
WHERE
  tgt.communication_device_sid IS NULL ;
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Communication_Device_SID
    FROM
      {{ params.param_hr_core_dataset_name }}.communication_device
    GROUP BY
      Communication_Device_SID
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: .{{ params.param_hr_core_dataset_name }}.communication_device');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;