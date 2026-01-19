BEGIN
	  DECLARE dup_count INT64;
	  DECLARE current_dt DATETIME;
	  SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
  

  /*  Close the previous records from Target table for same key for any Changes  */
   /*  Insert the New Records/Chnages into the Target Table  */ 
   /* Begin Transaction Block Starts Here */
   BEGIN TRANSACTION;
UPDATE
  {{ params.param_hr_core_dataset_name }}.candidate AS tgt
SET
  valid_to_date = DATETIME(current_dt - INTERVAL 1 SECOND),
  dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
FROM
  {{ params.param_hr_stage_dataset_name }}.candidate_wrk AS stg
WHERE
  tgt.candidate_sid = stg.candidate_sid
  AND (TRIM(CAST(COALESCE(tgt.candidate_num, -999) AS STRING)) <> TRIM(CAST(COALESCE(stg.candidate_num, -999) AS STRING))
    OR TRIM(CAST(COALESCE(tgt.in_hiring_process_sw, 9) AS STRING)) <> TRIM(CAST(COALESCE(stg.in_hiring_process_sw, 9) AS STRING))
    OR TRIM(CAST(COALESCE(tgt.internal_candidate_sw, 9) AS STRING)) <> TRIM(CAST(COALESCE(stg.internal_candidate_sw, 9) AS STRING))
    OR TRIM(CAST(COALESCE(tgt.referred_sw, 9) AS STRING)) <> TRIM(CAST(COALESCE(stg.referred_sw, 9) AS STRING))
    OR tgt.last_modified_date_time <> stg.last_modified_date_time
    OR tgt.candidate_creation_date_time <> stg.candidate_creation_date_time
    OR TRIM(CAST(COALESCE(tgt.residence_location_num, -999) AS STRING)) <> TRIM(CAST(COALESCE(stg.residence_location_num, -999) AS STRING))
    OR COALESCE(TRIM(tgt.source_system_code), '') <> COALESCE(TRIM(stg.source_system_code), '')
    OR COALESCE(TRIM(tgt.travel_preference_code), '') <> COALESCE(TRIM(stg.travel_preference_code), '')
    OR COALESCE(TRIM(tgt.relocation_preference_code), '') <> COALESCE(TRIM(stg.relocation_preference_code), ''))
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.candidate (candidate_sid,
    valid_from_date,
    candidate_num,
    in_hiring_process_sw,
    internal_candidate_sw,
    referred_sw,
    last_modified_date_time,
    candidate_creation_date_time,
    residence_location_num,
    travel_preference_code,
    relocation_preference_code,
    valid_to_date,
    source_system_code,
    dw_last_update_date_time)
SELECT
  stg.candidate_sid,
  current_dt AS valid_from_date,
  stg.candidate_num,
  stg.in_hiring_process_sw,
  stg.internal_candidate_sw,
  stg.referred_sw,
  stg.last_modified_date_time,
  stg.candidate_creation_date_time,
  stg.residence_location_num,
  stg.travel_preference_code,
  stg.relocation_preference_code,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  stg.source_system_code,
  stg.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.candidate_wrk AS stg
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.candidate AS tgt
ON
  stg.candidate_sid = tgt.candidate_sid
  AND TRIM(CAST(COALESCE(tgt.candidate_num, -999) AS STRING)) = TRIM(CAST(COALESCE(stg.candidate_num, -999) AS STRING))
  AND TRIM(CAST(COALESCE(tgt.in_hiring_process_sw, 9) AS STRING)) = TRIM(CAST(COALESCE(stg.in_hiring_process_sw, 9) AS STRING))
  AND TRIM(CAST(COALESCE(tgt.internal_candidate_sw, 9) AS STRING)) = TRIM(CAST(COALESCE(stg.internal_candidate_sw, 9) AS STRING))
  AND TRIM(CAST(COALESCE(tgt.referred_sw, 9) AS STRING)) = TRIM(CAST(COALESCE(stg.referred_sw, 9) AS STRING))
  AND tgt.last_modified_date_time = stg.last_modified_date_time
  AND tgt.candidate_creation_date_time = stg.candidate_creation_date_time
  AND TRIM(CAST(COALESCE(tgt.residence_location_num, -999) AS STRING)) = TRIM(CAST(COALESCE(stg.residence_location_num, -999) AS STRING))
  AND COALESCE(TRIM(tgt.source_system_code), '') = COALESCE(TRIM(stg.source_system_code), '')
  AND COALESCE(TRIM(tgt.travel_preference_code), '') = COALESCE(TRIM(stg.travel_preference_code), '')
  AND COALESCE(TRIM(tgt.relocation_preference_code), '') = COALESCE(TRIM(stg.relocation_preference_code), '')
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
WHERE
  tgt.candidate_sid IS NULL ;

    /* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Candidate_SID ,Valid_From_Date
    FROM
      {{ params.param_hr_core_dataset_name }}.candidate
    GROUP BY
      Candidate_SID ,Valid_From_Date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.candidate');
  ELSE
COMMIT TRANSACTION;
END IF;
END  ;