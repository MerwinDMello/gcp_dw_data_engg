BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_dt DATETIME;
  SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

  BEGIN TRANSACTION;

  /*  Close the previous records from Target table for same key for any Changes  */ /*  Insert the New Records/Chnages into the Target Table  */
UPDATE
  {{ params.param_hr_core_dataset_name }}.candidate_person AS tgt
SET
  valid_to_date = DATEtime(current_dt - INTERVAL 1 SECOND),
  dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
FROM
  {{ params.param_hr_stage_dataset_name }}.candidate_person_wrk AS stg
WHERE
  tgt.candidate_sid = stg.candidate_sid
  AND (UPPER(TRIM(COALESCE(tgt.first_name, ''))) <> UPPER(TRIM(COALESCE(stg.first_name, '')))
    OR UPPER(TRIM(COALESCE(tgt.middle_name, ''))) <> UPPER(TRIM(COALESCE(stg.middle_name, '')))
    OR UPPER(TRIM(COALESCE(tgt.last_name, ''))) <> UPPER(TRIM(COALESCE(stg.last_name, '')))
    OR UPPER(TRIM(COALESCE(tgt.social_security_num, ''))) <> UPPER(TRIM(COALESCE(stg.social_security_num, '')))
    OR UPPER(TRIM(COALESCE(tgt.email_address, ''))) <> UPPER(TRIM(COALESCE(stg.email_address, '')))
    OR COALESCE(tgt.birth_date, DATETIME("9999-12-31 23:59:59")) <> COALESCE(stg.birth_date, DATETIME("9999-12-31 23:59:59"))
    OR COALESCE(TRIM(tgt.source_system_code), '') <> COALESCE(TRIM(stg.source_system_code), '')
    OR UPPER(COALESCE(TRIM(tgt.maiden_name), '')) <> UPPER(COALESCE(TRIM(stg.maiden_name), ''))
    OR COALESCE(TRIM(tgt.driver_license_num), '') <> COALESCE(TRIM(stg.driver_license_num), '')
    OR COALESCE(TRIM(tgt.driver_license_state_code), '') <> COALESCE(TRIM(stg.driver_license_state_code), ''))
  AND tgt.valid_to_date =DATETIME("9999-12-31 23:59:59");
  
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.candidate_person (candidate_sid,
    valid_from_date,
    first_name,
    middle_name,
    last_name,
    social_security_num,
    email_address,
    maiden_name,
    driver_license_num,
    driver_license_state_code,
    birth_date,
    valid_to_date,
    source_system_code,
    dw_last_update_date_time)
SELECT
  stg.candidate_sid,
  --stg.valid_from_date,
  current_dt,
  stg.first_name,
  stg.middle_name,
  stg.last_name,
  stg.social_security_num,
  stg.email_address,
  stg.maiden_name,
  stg.driver_license_num,
  stg.driver_license_state_code,
  stg.birth_date,
  --stg.valid_to_date,
  DATETIME("9999-12-31 23:59:59"),
  stg.source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.candidate_person_wrk AS stg
LEFT OUTER JOIN
  {{ params.param_hr_base_views_dataset_name }}.candidate_person AS tgt
ON
  stg.candidate_sid = tgt.candidate_sid
  AND UPPER(TRIM(COALESCE(tgt.first_name, ''))) = UPPER(TRIM(COALESCE(stg.first_name, '')))
  AND UPPER(TRIM(COALESCE(tgt.middle_name, ''))) = UPPER(TRIM(COALESCE(stg.middle_name, '')))
  AND UPPER(TRIM(COALESCE(tgt.last_name, ''))) = UPPER(TRIM(COALESCE(stg.last_name, '')))
  AND UPPER(TRIM(COALESCE(tgt.social_security_num, ''))) = UPPER(TRIM(COALESCE(stg.social_security_num, '')))
  AND UPPER(TRIM(COALESCE(tgt.email_address, ''))) = UPPER(TRIM(COALESCE(stg.email_address, '')))
  AND COALESCE(tgt.birth_date, DATETIME("9999-12-31 23:59:59")) = COALESCE(stg.birth_date, DATETIME("9999-12-31 23:59:59"))
  AND COALESCE(TRIM(tgt.source_system_code), '') = COALESCE(TRIM(stg.source_system_code), '')
  AND UPPER(COALESCE(TRIM(tgt.maiden_name), '')) = UPPER(COALESCE(TRIM(stg.maiden_name), ''))
  AND COALESCE(TRIM(tgt.driver_license_num), '') = COALESCE(TRIM(stg.driver_license_num), '')
  AND COALESCE(TRIM(tgt.driver_license_state_code), '') = COALESCE(TRIM(stg.driver_license_state_code), '')
  AND tgt.valid_to_date =DATETIME("9999-12-31 23:59:59")
WHERE
  tgt.candidate_sid IS NULL ;


  /* Test Unique Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
        candidate_sid,valid_from_date          
        from {{ params.param_hr_core_dataset_name }}.candidate_person
        group by 
        candidate_sid,valid_from_date
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table:{{ params.param_hr_core_dataset_name }}.candidate_person' );
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;