BEGIN DECLARE DUP_COUNT INT64;

DECLARE current_ts datetime;

SET
  current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

/*  Truncate Work Table     */
TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.emp_disciplinary_action_dtl_wk;

/*  Insert the records into the Work Table  */
INSERT INTO
  {{ params.param_hr_stage_dataset_name }}.emp_disciplinary_action_dtl_wk (
    employee_sid,
    disciplinary_ind,
    disciplinary_action_num,
    disciplinary_seq_num,
    valid_from_date,
    valid_to_date,
    eff_from_date,
    status_flag,
    follow_up_category_name,
    follow_up_type_name,
    follow_up_outcome_desc,
    follow_up_performed_by_employee_sid,
    follow_up_comment_desc,
    last_update_date,
    last_update_user_34_login_code,
    employee_num,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  emp.employee_sid,
  CASE
    WHEN upper(pa.pagrdistep_type) = '' THEN '0'
    ELSE pa.pagrdistep_type
  END AS disciplinary_ind,
  pa.griev_dis_nbr,
  pa.seq_nbr,
  current_ts AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  pa.effect_date,
  cast(pa.status as string) AS status,
  trim(pa.follow_cat),
  trim(pa.follow_type),
  trim(pa.outcome),
  pa.performed_by,
  trim(pa.r_comment),
  pa.date_stamp,
  left(trim(pa.user_id), 7),
  pa.employee,
  pa.company,
  '00000' AS process_level_code,
  'L' AS source_system_code,
  current_ts AS dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.pagrdistep AS pa
  INNER JOIN {{ params.param_hr_core_dataset_name }}.employee AS emp ON pa.company = emp.lawson_company_num
  AND pa.employee = emp.employee_num
  AND valid_to_date = DATETIME("9999-12-31 23:59:59");

BEGIN TRANSACTION;

/*UPDATE VLD_TO_DATE from records that are not presnt in Staging table*/
UPDATE
  {{ params.param_hr_core_dataset_name }}.employee_disciplinary_action_detail AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND
WHERE
  tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND (
    coalesce(trim(cast(tgt.employee_sid as string)), ''),
    coalesce(trim(tgt.disciplinary_ind), ''),
    coalesce(
      trim(cast(tgt.disciplinary_action_num as string)),
      ''
    ),
    coalesce(
      trim(cast(tgt.disciplinary_seq_num as string)),
      ''
    )
  ) NOT IN(
    SELECT
      AS STRUCT coalesce(trim(cast(stg.employee_sid as string)), ''),
      coalesce(trim(stg.disciplinary_ind), ''),
      coalesce(
        trim(cast(stg.disciplinary_action_num as string)),
        ''
      ),
      coalesce(
        trim(cast(stg.disciplinary_seq_num as string)),
        ''
      )
    FROM
      {{ params.param_hr_stage_dataset_name }}.emp_disciplinary_action_dtl_wk AS stg
  )
  and tgt.source_system_code = 'L';

UPDATE
  {{ params.param_hr_core_dataset_name }}.employee_disciplinary_action_detail AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND,
  dw_last_update_date_time = current_ts
FROM
  {{ params.param_hr_stage_dataset_name }}.emp_disciplinary_action_dtl_wk AS stg
WHERE
  coalesce(trim(cast(tgt.employee_sid as string)), '') = coalesce(trim(cast(stg.employee_sid as string)), '')
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
  AND upper(coalesce(tgt.disciplinary_ind, '')) = upper(coalesce(stg.disciplinary_ind, ''))
  AND upper(
    coalesce(cast(tgt.disciplinary_action_num as string), '')
  ) = upper(
    coalesce(cast(stg.disciplinary_action_num as string), '')
  )
  AND upper(
    coalesce(cast(tgt.disciplinary_seq_num as string), '')
  ) = upper(
    coalesce(cast(stg.disciplinary_seq_num as string), '')
  )
  AND (
    coalesce(stg.eff_from_date, DATE '1900-01-01') <> coalesce(tgt.eff_from_date, DATE '1900-01-01')
    OR coalesce(trim(stg.status_flag), '') <> coalesce(trim(tgt.status_flag), '')
    OR coalesce(trim(stg.follow_up_category_name), '') <> coalesce(trim(tgt.follow_up_category_name), '')
    OR coalesce(trim(stg.follow_up_type_name), '') <> coalesce(trim(tgt.follow_up_type_name), '')
    OR coalesce(trim(stg.follow_up_outcome_desc), '') <> coalesce(trim(tgt.follow_up_outcome_desc), '')
    OR coalesce(
      trim(
        cast(
          stg.follow_up_performed_by_employee_sid as string
        )
      ),
      ''
    ) <> coalesce(
      trim(
        cast(
          tgt.follow_up_performed_by_employee_sid as string
        )
      ),
      ''
    )
    OR coalesce(trim(stg.follow_up_comment_desc), '') <> coalesce(trim(tgt.follow_up_comment_desc), '')
    OR coalesce(stg.last_update_date, DATE '1900-01-01') <> coalesce(tgt.last_update_date, DATE '1900-01-01')
    OR coalesce(trim(stg.last_update_user_34_login_code), '') <> coalesce(trim(tgt.last_update_user_34_login_code), '')
    OR trim(cast(stg.employee_num as string)) <> trim(cast(tgt.employee_num as string))
    OR trim(cast(stg.lawson_company_num as string)) <> trim(cast(tgt.lawson_company_num as string))
    OR coalesce(trim(stg.process_level_code), '00000') <> trim(tgt.process_level_code)
  )
  and tgt.source_system_code = 'L';

INSERT INTO
  {{ params.param_hr_core_dataset_name }}.employee_disciplinary_action_detail (
    employee_sid,
    disciplinary_ind,
    disciplinary_action_num,
    disciplinary_seq_num,
    valid_from_date,
    valid_to_date,
    eff_from_date,
    status_flag,
    follow_up_category_name,
    follow_up_type_name,
    follow_up_outcome_desc,
    follow_up_performed_by_employee_sid,
    follow_up_comment_desc,
    last_update_date,
    last_update_user_34_login_code,
    employee_num,
    lawson_company_num,
    process_level_code,
    source_system_code,
    dw_last_update_date_time
  )
SELECT
  emp_disciplinary_action_dtl_wk.employee_sid,
  emp_disciplinary_action_dtl_wk.disciplinary_ind,
  emp_disciplinary_action_dtl_wk.disciplinary_action_num,
  emp_disciplinary_action_dtl_wk.disciplinary_seq_num,
  current_ts,
  DATETIME("9999-12-31 23:59:59"),
  emp_disciplinary_action_dtl_wk.eff_from_date,
  emp_disciplinary_action_dtl_wk.status_flag,
  emp_disciplinary_action_dtl_wk.follow_up_category_name,
  emp_disciplinary_action_dtl_wk.follow_up_type_name,
  emp_disciplinary_action_dtl_wk.follow_up_outcome_desc,
  emp_disciplinary_action_dtl_wk.follow_up_performed_by_employee_sid,
  emp_disciplinary_action_dtl_wk.follow_up_comment_desc,
  emp_disciplinary_action_dtl_wk.last_update_date,
  emp_disciplinary_action_dtl_wk.last_update_user_34_login_code,
  emp_disciplinary_action_dtl_wk.employee_num,
  emp_disciplinary_action_dtl_wk.lawson_company_num,
  emp_disciplinary_action_dtl_wk.process_level_code,
  emp_disciplinary_action_dtl_wk.source_system_code,
  emp_disciplinary_action_dtl_wk.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.emp_disciplinary_action_dtl_wk
WHERE
  (
    trim(
      cast(
        emp_disciplinary_action_dtl_wk.employee_sid as string
      )
    ),
    trim(emp_disciplinary_action_dtl_wk.disciplinary_ind),
    coalesce(
      emp_disciplinary_action_dtl_wk.disciplinary_action_num,
      0
    ),
    coalesce(
      emp_disciplinary_action_dtl_wk.disciplinary_seq_num,
      0
    )
  ) NOT IN(
    SELECT
      AS STRUCT trim(
        cast(
          employee_disciplinary_action_detail.employee_sid as string
        )
      ),
      trim(
        employee_disciplinary_action_detail.disciplinary_ind
      ),
      coalesce(
        employee_disciplinary_action_detail.disciplinary_action_num,
        0
      ),
      coalesce(
        employee_disciplinary_action_detail.disciplinary_seq_num,
        0
      )
    FROM
      {{ params.param_hr_core_dataset_name }}.employee_disciplinary_action_detail
    WHERE
      employee_disciplinary_action_detail.valid_to_date = DATETIME("9999-12-31 23:59:59")
  );

/* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
    select
      count(*)
    from
      (
        select
          Employee_Num,
          Kronos_Num,
          Clock_Library_Code,
          Kronos_Pay_Code_Seq_Num,
          Valid_From_Date
        from
          {{ params.param_hr_core_dataset_name }}.time_entry_pay_code_detail
        group by
          Employee_Num,
          Kronos_Num,
          Clock_Library_Code,
          Kronos_Pay_Code_Seq_Num,
          Valid_From_Date
        having
          count(*) > 1
      )
  );

IF DUP_COUNT <> 0 THEN ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat(
  'Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.time_entry_pay_code_detail'
);

ELSE COMMIT TRANSACTION;

END IF;

END;