BEGIN
DECLARE
  DUP_COUNT INT64;
DECLARE
  current_ts datetime;
SET
  current_ts = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) ;
BEGIN TRANSACTION; /*  Close the previous records from Target table for same key for any Changes  */ /*  Insert the New Records/Chnages into the Target Table  */
UPDATE
  {{ params.param_hr_core_dataset_name }}.employee_requisition AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  dw_last_update_date_time = stg.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.employee_requisition_wrk AS stg
WHERE
  tgt.employee_sid = stg.employee_sid
  AND tgt.eff_from_date = stg.eff_from_date
  AND tgt.requisition_sid = stg.requisition_sid
  AND UPPER(TRIM(COALESCE(tgt.action_type_code, 'X'))) = UPPER(TRIM(COALESCE(stg.action_type_code, 'X')))
  AND ( UPPER(TRIM(COALESCE(tgt.action_code, '-999'))) <> UPPER(TRIM(COALESCE(stg.action_code, '-999')))
    OR UPPER(TRIM(COALESCE(tgt.user_id_text, 'X'))) <> UPPER(TRIM(COALESCE(stg.user_id_text, 'X')))
    OR TRIM(CAST(COALESCE(tgt.work_unit_num, -999) AS STRING)) <> TRIM(CAST(COALESCE(stg.work_unit_num, -999) AS STRING))
    OR TRIM(CAST(COALESCE(tgt.lawson_company_num, -999) AS STRING)) <> TRIM(CAST(COALESCE(stg.lawson_company_num, -999) AS STRING))
    OR UPPER(TRIM(COALESCE(tgt.process_level_code, '-999'))) <> UPPER(TRIM(COALESCE(stg.process_level_code, '-999')))
    OR TRIM(CAST(COALESCE(tgt.requisition_num, -999) AS STRING)) <> TRIM(CAST(COALESCE(stg.requisition_num, -999) AS STRING))
    OR TRIM(CAST(COALESCE(tgt.employee_num, -999) AS STRING)) <> TRIM(CAST(COALESCE(stg.employee_num, -999) AS STRING)))
  AND DATE(tgt.valid_to_date) = "9999-12-31"
 AND tgt.source_system_code = 'L';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.employee_requisition (employee_sid,
    requisition_sid,
    valid_from_date,
    valid_to_date,
    action_type_code,
    eff_from_date,
    action_code,
    delete_ind,
    user_id_text,
    work_unit_num,
    lawson_company_num,
    process_level_code,
    requisition_num,
    employee_num,
    source_system_code,
    dw_last_update_date_time)
SELECT
  stg.employee_sid AS employee_sid,
  stg.requisition_sid AS requisition_sid,
  current_ts AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  stg.action_type_code AS action_type_code,
  stg.eff_from_date AS eff_from_date,
  stg.action_code AS action_code,
  'A' AS delete_ind,
  stg.user_id_text AS user_id_text,
  stg.work_unit_num AS work_unit_num,
  stg.lawson_company_num AS lawson_company_num,
  stg.process_level_code AS process_level_code,
  stg.requisition_num AS requisition_num,
  stg.employee_num AS employee_num,
  stg.source_system_code AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.employee_requisition_wrk AS stg
WHERE
  (stg.employee_sid,
    stg.requisition_sid,
    UPPER(TRIM(stg.action_type_code)),
    stg.eff_from_date,
    UPPER(TRIM(stg.action_code)),
    UPPER(TRIM(stg.user_id_text)),
    stg.work_unit_num,
    UPPER(TRIM(CAST(stg.lawson_company_num AS string))),
    UPPER(TRIM(stg.process_level_code)),
    stg.requisition_num,
    stg.employee_num) NOT IN(
  SELECT
    AS STRUCT tgt.employee_sid,
    tgt.requisition_sid,
    UPPER(TRIM(tgt.action_type_code)),
    tgt.eff_from_date,
    UPPER(TRIM(tgt.action_code)),
    UPPER(TRIM(tgt.user_id_text)),
    tgt.work_unit_num,
    UPPER(TRIM(CAST(tgt.lawson_company_num AS string))),
    UPPER(TRIM(tgt.process_level_code)),
    tgt.requisition_num,
    tgt.employee_num
  FROM
    {{ params.param_hr_core_dataset_name }}.employee_requisition AS tgt
  WHERE
    DATE(tgt.valid_to_date) = "9999-12-31") ; /*  UPDATE  DELETE_IND */

UPDATE
  {{ params.param_hr_core_dataset_name }}.employee_requisition AS empl
SET
  delete_ind = 'D'
WHERE
  UPPER(empl.delete_ind) = 'A'
  AND (empl.lawson_company_num,
    empl.employee_num) NOT IN(
  SELECT
    DISTINCT AS STRUCT employee.lawson_company_num,
    employee.employee_num
  FROM
    {{ params.param_hr_core_dataset_name }}.employee )
    AND empl.source_system_code = 'L';

UPDATE
  {{ params.param_hr_core_dataset_name }}.employee_requisition AS empl
SET
  delete_ind = 'A'
WHERE
  UPPER(empl.delete_ind) = 'D'
  AND (empl.lawson_company_num,
    empl.employee_num) IN(
  SELECT
    DISTINCT AS STRUCT employee.lawson_company_num,
    employee.employee_num
  FROM
    {{ params.param_hr_core_dataset_name }}.employee )
    AND empl.source_system_code = 'L'; /* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Employee_SID,
      Requisition_SID,
      Action_Type_Code,
      Eff_From_Date,
      Valid_From_Date
    FROM
      {{ params.param_hr_core_dataset_name }}.employee_requisition
    GROUP BY
      Employee_SID,
      Requisition_SID,
      Action_Type_Code,
      Eff_From_Date,
      Valid_From_Date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.employee_requisition');
  ELSE
COMMIT TRANSACTION;
END IF
  ; 
  
  /*  RETIRE RECORD ON 2ND RETIRE LOGIC */
UPDATE
  {{ params.param_hr_core_dataset_name }}.employee_requisition AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  dw_last_update_date_time = current_ts
WHERE
  tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND (tgt.employee_sid,
    tgt.requisition_sid,
    UPPER(TRIM(tgt.action_type_code)),
    tgt.eff_from_date) NOT IN(
  SELECT
    DISTINCT AS STRUCT employee_requisition_wrk.employee_sid,
    employee_requisition_wrk.requisition_sid,
    UPPER(TRIM(employee_requisition_wrk.action_type_code)),
    employee_requisition_wrk.eff_from_date
  FROM
    {{ params.param_hr_stage_dataset_name }}.employee_requisition_wrk );
END
  ;