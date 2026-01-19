  /*  Close the previous records from Target table for same key for any Changes  */
   /*  Insert the New Records/Chnages into the Target Table  */
    /* Begin Transaction Block Starts Here */
BEGIN
	  DECLARE dup_count INT64;
	  DECLARE current_dt DATETIME;
	  SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

	  BEGIN TRANSACTION;
	  UPDATE  {{ params.param_hr_core_dataset_name }}.offer_detail AS tgt
SET
  valid_to_date = DATETIME(current_dt - INTERVAL 1 SECOND),
  dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)

FROM
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk AS stg
WHERE
  tgt.offer_sid = stg.offer_sid
  AND tgt.element_detail_entity_text = stg.element_detail_entity_text
  AND tgt.element_detail_type_text = stg.element_detail_type_text
  AND tgt.element_detail_seq_num = stg.element_detail_seq_num
  AND (COALESCE(tgt.element_detail_id, -999) <> COALESCE(stg.element_detail_id, -999)
    OR COALESCE(CAST(tgt.element_detail_value_text AS string), '-9') <> COALESCE(CAST(stg.element_detail_value_text AS string), '-9')
    OR COALESCE(TRIM(tgt.source_system_code), 'X') <> COALESCE(TRIM(stg.source_system_code), 'X'))
  AND tgt.valid_to_date =DATETIME("9999-12-31 23:59:59");
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.offer_detail (offer_sid,
    element_detail_entity_text,
    element_detail_type_text,
    element_detail_seq_num,
    valid_from_date,
    valid_to_date,
    element_detail_id,
    element_detail_value_text,
    source_system_code,
    dw_last_update_date_time)
SELECT
  stg.offer_sid,
  stg.element_detail_entity_text,
  stg.element_detail_type_text,
  stg.element_detail_seq_num,
  current_dt AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  stg.element_detail_id,
  stg.element_detail_value_text,
  stg.source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.offer_detail_wrk AS stg
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.offer_detail AS tgt
ON
  tgt.offer_sid = stg.offer_sid
  AND tgt.element_detail_entity_text = stg.element_detail_entity_text
  AND tgt.element_detail_type_text = stg.element_detail_type_text
  AND tgt.element_detail_seq_num = stg.element_detail_seq_num
  AND TRIM(CAST(COALESCE(tgt.element_detail_id, -999) AS STRING)) = TRIM(CAST(COALESCE(stg.element_detail_id, -999) AS STRING))
  AND COALESCE(CAST(tgt.element_detail_value_text AS string), '-9')=COALESCE(CAST(STG.element_detail_value_text AS string),'-9')
  AND COALESCE(TRIM(tgt.source_system_code), 'X') = COALESCE(TRIM(stg.source_system_code), 'X')
  AND tgt.valid_to_date =DATETIME("9999-12-31 23:59:59")
WHERE
  tgt.offer_sid IS NULL ; /* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      offer_sid,element_detail_entity_text,element_detail_type_text,element_detail_seq_num,valid_from_date
    FROM
      {{ params.param_hr_core_dataset_name }}.offer_detail
    GROUP BY
      offer_sid,element_detail_entity_text,element_detail_type_text,element_detail_seq_num,valid_from_date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
    ROLLBACK TRANSACTION; 

  RAISE USING MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.offer_detail');
ELSE
  COMMIT TRANSACTION;
END IF
  ;
END
  ;