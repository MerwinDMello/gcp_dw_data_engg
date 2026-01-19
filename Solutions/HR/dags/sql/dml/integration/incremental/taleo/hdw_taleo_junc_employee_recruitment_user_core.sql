BEGIN
DECLARE
  DUP_COUNT INT64;
  DECLARE current_dt DATETIME;
  SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
  BEGIN TRANSACTION;

  /*  Retire records that have changed */
UPDATE
  {{ params.param_hr_core_dataset_name }}.junc_employee_recruitment_user AS tgt
SET
  valid_to_date = current_dt - INTERVAL 1 SECOND,
  dw_last_update_date_time = (wrk.dw_last_update_date_time
  )
FROM
  {{ params.param_hr_stage_dataset_name }}.junc_employee_recruitment_user_wrk AS wrk
WHERE
  wrk.employee_sid = tgt.employee_sid
  AND wrk.recruitment_user_sid = tgt.recruitment_user_sid
  AND COALESCE(TRIM(wrk.primary_facility_ind), 'X') <> COALESCE(TRIM(tgt.primary_facility_ind), 'X')
  AND (tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59"); 
  
  /*  Insert the New Records/Changes into the Target Table  */
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.junc_employee_recruitment_user (employee_sid,
    recruitment_user_sid,
    valid_from_date,
    primary_facility_ind,
    valid_to_date,
    source_system_code,
    dw_last_update_date_time)
SELECT
  wrk.employee_sid,
  wrk.recruitment_user_sid,
  current_dt as valid_from_date,
  -- pulls FROM  wrk TABLE File_Date 
  wrk.primary_facility_ind,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  wrk.source_system_code,
  DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.junc_employee_recruitment_user_wrk AS wrk
WHERE
  (wrk.employee_sid,
    wrk.recruitment_user_sid,
    wrk.primary_facility_ind) NOT IN(
  SELECT
    AS STRUCT tgt.employee_sid,
    tgt.recruitment_user_sid,
    tgt.primary_facility_ind
  FROM
    {{ params.param_hr_base_views_dataset_name }}.junc_employee_recruitment_user AS tgt
  WHERE
    (tgt.valid_to_date) = DATETIME("9999-12-31 23:59:59") ) ;

      /* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      employee_sid,recruitment_user_sid,valid_from_date
    FROM {{ params.param_hr_core_dataset_name }}.junc_employee_recruitment_user    
    GROUP BY employee_sid,recruitment_user_sid,valid_from_date      
    HAVING COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table {{ params.param_hr_core_dataset_name }}.junc_employee_recruitment_user');
  ELSE
COMMIT TRANSACTION;
END IF;
END  ;