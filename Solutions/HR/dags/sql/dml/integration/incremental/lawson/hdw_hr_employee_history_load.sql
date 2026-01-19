BEGIN DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);  

/*  Generate the surrogate keys for HR_Employee_History */
CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}','hrhistory',"company||'-'||employee", 'EMPLOYEE');

/*  Truncate Worktable Table     */
TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.hr_employee_history_wrk;

/*  Load Work Table with working Data */
INSERT INTO {{ params.param_hr_stage_dataset_name }}.hr_employee_history_wrk (employee_sid, valid_from_date, lawson_element_num, hr_employee_element_date, lawson_object_id, sequence_num, hr_employee_value_num, hr_employee_value_alphanumeric_text, hr_employee_value_date, action_object_identifier, user_3_4_login_code, data_type_flag, position_level_sequence_num, last_update_date, last_update_time, valid_to_date, employee_num, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time)
    SELECT
        -- EMP_trg.EMPLOYEE_SID AS EMPLOYEE_SID
        cast(wlk.sk as int64),
        current_ts AS valid_from_date,
        emphst.fld_nbr AS lawson_element_num,
        emphst.beg_date AS hr_employee_element_date,
        cast(emphst.obj_id as int64) AS lawson_object_id,
        emphst.seq_nbr AS sequence_num,
        emphst.n_value AS hr_employee_value_num,
        emphst.a_value AS hr_employee_value_alphanumeric_text,
        emphst.d_value AS hr_employee_value_date,
        cast(emphst.act_obj_id as int64) AS action_object_identifier,
        emphst.user_id AS user_3_4_login_code,
        emphst.data_type AS data_type_flag,
        emphst.pos_level AS position_level_sequence_num,
        emphst.date_stamp AS last_update_date,
        CAST(emphst.time_stamp as TIME) AS last_update_time,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        emphst.employee AS employee_num,
        emphst.company AS lawson_company_num,
        CASE
          WHEN trim(emp.process_level) IS NULL THEN '00000'
          ELSE trim(emp.process_level)
        END AS process_level_code,
        'L' AS source_system_code,
        emphst.dw_last_update_date_time AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.hrhistory AS emphst
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS wlk 
        ON upper(substr(concat(emphst.company, '-', emphst.employee), 1, 255)) = upper(wlk.sk_source_txt)
        AND upper(wlk.sk_type) = 'EMPLOYEE'
        LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.employee AS emp 
        ON emphst.company = emp.company
        AND emphst.employee = emp.employee;
    
BEGIN TRANSACTION;

UPDATE
  {{ params.param_hr_core_dataset_name }}.hr_employee_history AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.hr_employee_history_wrk AS stg
WHERE
  tgt.employee_sid = stg.employee_sid
  AND tgt.lawson_object_id = stg.lawson_object_id
  AND tgt.source_system_code = stg.source_system_code
  AND tgt.hr_employee_element_date = stg.hr_employee_element_date
  AND tgt.lawson_element_num = stg.lawson_element_num
  AND tgt.sequence_num = stg.sequence_num
  AND (tgt.hr_employee_value_num <> stg.hr_employee_value_num
  OR upper(trim(coalesce(tgt.hr_employee_value_alphanumeric_text, ''))) <> upper(trim(coalesce(stg.hr_employee_value_alphanumeric_text, '')))
  OR tgt.hr_employee_value_date <> stg.hr_employee_value_date
  OR upper(trim(coalesce(tgt.user_3_4_login_code, ''))) <> upper(trim(coalesce(stg.user_3_4_login_code, '')))
  OR upper(trim(coalesce(tgt.data_type_flag, ''))) <> upper(trim(coalesce(stg.data_type_flag, '')))
  OR tgt.position_level_sequence_num <> stg.position_level_sequence_num
  OR cast(tgt.last_update_date as STRING) <> cast(stg.last_update_date as STRING)
  OR cast(tgt.last_update_time as STRING) <> cast(stg.last_update_time as STRING)
  OR tgt.action_object_identifier <> stg.action_object_identifier
  OR tgt.lawson_company_num <> stg.lawson_company_num
  OR tgt.process_level_code <> stg.process_level_code)
  AND tgt.valid_to_date = datetime("9999-12-31 23:59:59")
  AND tgt.source_system_code = 'L';

/* insert the new records*/ 
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.hr_employee_history (
    employee_sid,
    valid_from_date,
    lawson_element_num,
    hr_employee_element_date,
    lawson_object_id,
    sequence_num,
    hr_employee_value_num,
    hr_employee_value_alphanumeric_text,
    hr_employee_value_date,
    action_object_identifier,
    user_3_4_login_code,
    data_type_flag,
    position_level_sequence_num,
    last_update_date,
    last_update_time,
    valid_to_date,
    delete_ind,
    employee_num,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  stg.employee_sid,
  --stg.valid_from_date,
  current_ts as valid_from_date,
  stg.lawson_element_num,
  stg.hr_employee_element_date,
  stg.lawson_object_id,
  stg.sequence_num,
  stg.hr_employee_value_num,
  stg.hr_employee_value_alphanumeric_text,
  stg.hr_employee_value_date,
  stg.action_object_identifier,
  stg.user_3_4_login_code,
  stg.data_type_flag,
  stg.position_level_sequence_num,
  stg.last_update_date,
  stg.last_update_time,
  --stg.valid_to_date,
  datetime("9999-12-31 23:59:59") as valid_to_date,
  'A' AS delete_ind,
  stg.employee_num,
  stg.lawson_company_num,
  stg.process_level_code,
  stg.source_system_code,
  current_ts
FROM
  -- ,stg.DW_LAST_UPDATE_DATE_TIME
  {{ params.param_hr_stage_dataset_name }}.hr_employee_history_wrk AS stg
  LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_employee_history AS tgt ON tgt.employee_sid = stg.employee_sid
  AND tgt.lawson_object_id = stg.lawson_object_id
  AND tgt.hr_employee_element_date = stg.hr_employee_element_date
  AND tgt.lawson_element_num = stg.lawson_element_num
  AND tgt.sequence_num = stg.sequence_num
  AND tgt.valid_to_date = datetime("9999-12-31 23:59:59")
  AND tgt.source_system_code = 'L'
  where tgt.lawson_object_id IS NULL
  AND tgt.hr_employee_element_date IS NULL
  AND tgt.lawson_element_num IS NULL
  AND tgt.sequence_num IS NULL;

/*Retire the records if the recordsin core matches with records in HISTERR */
/*Below code is replace above as STRUCT was giving error so takenstandard sql*/
update
  {{ params.param_hr_core_dataset_name }}.hr_employee_history emp
set
  valid_to_date = current_ts - interval '1' second,
  dw_last_update_date_time = current_ts
where
  emp.valid_to_date = datetime("9999-12-31 23:59:59")
  and (
    emp.lawson_company_num || emp.employee_num || emp.lawson_element_num || emp.hr_employee_element_date || emp.sequence_num || emp.lawson_object_id
  ) in (
    select
      company || employee || cast(fld_nbr as INT64) || effect_date || seq_nbr || obj_id
    from
      {{ params.param_hr_stage_dataset_name }}.histerr
    group by
      1
  )
  AND emp.source_system_code = 'L';

/* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
    select
      count(*)
    from
      (
        select
          employee_sid,
          lawson_element_num,
          hr_employee_element_date,
          lawson_object_id,
          sequence_num,
          valid_from_date
        from
          {{ params.param_hr_core_dataset_name }}.hr_employee_history
        group by
          employee_sid,
          lawson_element_num,
          hr_employee_element_date,
          lawson_object_id,
          sequence_num,
          valid_from_date
        having
          count(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat(
  'Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.hr_employee_history'
);

ELSE COMMIT TRANSACTION;

END IF;

END;