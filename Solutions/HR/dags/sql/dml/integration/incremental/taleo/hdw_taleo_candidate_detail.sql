BEGIN
DECLARE
DUP_COUNT INT64;
DECLARE current_dt DATETIME;
SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

BEGIN TRANSACTION; 
/*  Close the previous records from Target table for same key for any Changes  */ /*  Insert the New Records/Chnages into the Target Table  */ /* Begin Transaction Block Starts Here */
UPDATE
  {{ params.param_hr_core_dataset_name }}.candidate_detail AS tgt
SET
  valid_to_date = DATETIME(current_dt - INTERVAL 1 SECOND),
  dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
FROM
  {{ params.param_hr_stage_dataset_name }}.candidate_detail_wrk AS stg
WHERE
  tgt.candidate_sid = stg.candidate_sid
  AND UPPER(TRIM(COALESCE(tgt.element_detail_entity_text, ''))) = UPPER(TRIM(COALESCE(stg.element_detail_entity_text, '')))
  AND UPPER(TRIM(COALESCE(tgt.element_detail_type_text, ''))) = UPPER(TRIM(COALESCE(stg.element_detail_type_text, '')))
  AND tgt.element_detail_seq = stg.element_detail_seq
  AND tgt.source_system_code = stg.source_system_code
  AND (COALESCE(tgt.element_detail_id, -999) <> COALESCE(stg.element_detail_id, -999)
OR
(Case when UPPER(TRIM(COALESCE(stg.element_detail_value_text, ''))) ='0.0' Then
cast(stg.element_detail_value_text as FLOAT64) <> cast(tgt.element_detail_value_text as FLOAT64)
ELSE
UPPER(TRIM(COALESCE(tgt.element_detail_value_text, ''))) <> UPPER(TRIM(COALESCE(stg.element_detail_value_text, '')))
END
))
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");
  
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.candidate_detail (candidate_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  stg.candidate_sid,
  stg.element_detail_entity_text,
  stg.element_detail_type_text,
  stg.element_detail_seq,
  current_dt,
  DATETIME("9999-12-31 23:59:59"),
  stg.element_detail_id,
  stg.element_detail_value_text,
  stg.source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.candidate_detail_wrk AS stg
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS tgt
ON
  stg.candidate_sid = tgt.candidate_sid
  AND UPPER(TRIM(COALESCE(tgt.element_detail_entity_text, ''))) = UPPER(TRIM(COALESCE(stg.element_detail_entity_text, '')))
  AND UPPER(TRIM(COALESCE(tgt.element_detail_type_text, ''))) = UPPER(TRIM(COALESCE(stg.element_detail_type_text, '')))
  AND tgt.element_detail_seq = stg.element_detail_seq
  AND tgt.source_system_code = stg.source_system_code
  AND TRIM(CAST(COALESCE(tgt.element_detail_id, -999) AS STRING)) = TRIM(CAST(COALESCE(stg.element_detail_id, -999) AS STRING))
  
  AND Case when UPPER(TRIM(COALESCE(stg.element_detail_value_text, ''))) ='0.0' Then 
  cast(stg.element_detail_value_text as FLOAT64) = cast(tgt.element_detail_value_text as FLOAT64)
  ELSE
  UPPER(TRIM(COALESCE(tgt.element_detail_value_text, ''))) = UPPER(TRIM(COALESCE(stg.element_detail_value_text, '')))
  END
   
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
WHERE
  tgt.candidate_sid IS NULL ; /* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      candidate_sid,element_detail_entity_text,element_detail_type_text,element_detail_seq,valid_from_date
    FROM
      {{ params.param_hr_core_dataset_name }}.candidate_detail
    GROUP BY
      candidate_sid,element_detail_entity_text,element_detail_type_text,element_detail_seq,valid_from_date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.candidate_detail');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;