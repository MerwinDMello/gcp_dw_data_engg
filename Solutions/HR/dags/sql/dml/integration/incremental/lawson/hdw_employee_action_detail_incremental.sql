BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_ts datetime;
  SET current_ts = datetime_trunc(current_datetime('US/Central'), second);

--current_ts AS valid_from_date,
--DATETIME("9999-12-31 23:59:59") AS valid_to_date
/*  TRUNCATE WORK TABLE     */

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_inc;

/*  INSERT THE RECORDS INTO THE WORK TABLE  */

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_inc (employee_sid, hr_company_sid, company, action_code, effect_date, action_nbr, employee, ant_end_date, reason_01, user_id, date_stamp, action_type, description, pos_level, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time)
    SELECT
        empl.employee_sid,
        cmpy.hr_company_sid,
        pah.company,
        pah.action_code,
        cast(pah.effect_date as date) as effect_date,
        pah.action_nbr,
        pah.employee,
        cast(pah.ant_end_date as date)as ant_end_date,
        pah.reason_01,
        pah.user_id,
        cast(pah.date_stamp as date)as date_stamp,
        pah.action_type,
        pct.description,
        pah.pos_level,
        pah.company AS lawson_company_num,
        '00000' AS process_level_code,
        'L' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.persacthst AS pah
        INNER JOIN -- FROM VTL1 PAH
        {{ params.param_hr_stage_dataset_name }}.paactreas AS pct ON trim(pah.reason_01) = trim(pct.act_reason_cd)
         AND trim(pah.action_code) = trim(pct.action_code)
         AND pah.company = pct.company
        INNER JOIN --  JV
        {{ params.param_hr_base_views_dataset_name }}.employee AS empl ON pah.employee = empl.employee_num
         AND upper(empl.active_dw_ind) = 'Y'
         AND pah.company = empl.lawson_company_num
        INNER JOIN -- JV
        --  JV
        {{ params.param_hr_base_views_dataset_name }}.hr_company AS cmpy ON pah.company = cmpy.lawson_company_num
         AND upper(empl.active_dw_ind) = 'Y'
         AND upper(cmpy.active_dw_ind) = 'Y'
        --  and pah.action_code in ('1XFERPOS', '1XFEREIN-S', '1ADDPOS', '1STOPPOS', '1TERMPEND', '9 BNSALARY', '1XFERINT')
      WHERE upper(trim(pah.action_code)) IN(
        -- JV
        '1XFERPOS', '1XFEREIN-S', '1ADDPOS', '1STOPPOS', '1TERMPEND', '9 BNSALARY', '1XFERINT'
      )
    UNION DISTINCT
    SELECT
        empl.employee_sid,
        cmpy.hr_company_sid,
        cast(mpah.company as integer)as company ,
        coalesce(mrc.hca_action_cd, mpah.action_code) AS action_code,
        mpah.effect_date,
        cast(mpah.action_nbr as integer) as action_nbr,
        cast(mpah.employee as integer)as employee,
        mpah.ant_end_date,
        mpah.reason_01,
        mpah.user_id,
        mpah.date_stamp,
        mpah.action_type,
        rar.action_reason_desc,
        cast(mpah.pos_level as integer) as pos_level ,
        cast(mpah.company as integer) as lawson_company_num,
        '00000' AS process_level_code,
        'A' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.msh_persacthst_stg AS mpah
        INNER JOIN /*INNER JOIN $NCR_STG_SCHEMA.MSH_PAACTREAS_STG MPCT
        ON (MPAH.REASON_01 = MPCT.ACT_REASON_CD --AND MPAH.ACTION_CODE = MPCT.ACTION_CODE)
        AND MPAH.COMPANY = MPCT.COMPANY */
        {{ params.param_hr_base_views_dataset_name }}.ref_action_reason AS rar ON  trim(mpah.reason_01) = trim( rar.action_reason_text)
         AND mpah.company = rar.lawson_company_num
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS empl ON mpah.employee = empl.employee_num
         AND upper(empl.active_dw_ind) = 'Y'
         AND mpah.company = empl.lawson_company_num
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_company AS cmpy ON mpah.company = cmpy.lawson_company_num
         AND upper(empl.active_dw_ind) = 'Y'
         AND upper(cmpy.active_dw_ind) = 'Y'
        LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.mh_to_hca_reason_code AS mrc ON trim(mpah.reason_01) = trim(mrc.mh_reason_cd)
      WHERE  upper(trim(mrc.hca_action_cd ))IN(
        '1XFERPOS', '1XFEREIN-S', '1ADDPOS', '1STOPPOS', '1TERMPEND', '9 BNSALARY', '1XFERINT'
      )
      QUALIFY row_number() OVER (PARTITION BY mpah.action_nbr, mpah.employee, mrc.hca_action_cd, mpah.effect_date, mpah.action_type, mpah.company ORDER BY mpah.date_stamp DESC) = 1
  ;

/*  TRUNCATE WORK TABLE     */

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_3_inc;

/* VOLATILE TABLE TO REDURE IO */

  CREATE TEMPORARY TABLE vtl1
    AS
      SELECT
          *
        FROM
          {{ params.param_hr_base_views_dataset_name }}.hr_employee_history AS hrhist
        WHERE hrhist.lawson_element_num IN(
          2088, 15, 899, 293, 19, 17, 106, 126, 14, 63, 135, 1414, 64, 136, 68, 18, 21, 16, 73, 28, 288, 30
        )
         AND upper(hrhist.source_system_code) = 'L'
         AND date(hrhist.valid_to_date) = DATE("9999-12-31")
       UNION DISTINCT
      SELECT
          hrhist.*
        FROM
         {{ params.param_hr_base_views_dataset_name }}.hr_employee_history AS hrhist
        WHERE hrhist.lawson_element_num IN(
          -- 27989521
          15, 899, 293, 19, 17, 106, 126, 14, 63, 135, 1414, 64, 136, 68, 18, 21, 16, 73, 28, 288, 30
        )
         AND upper(hrhist.source_system_code) = 'A'
         AND date(hrhist.valid_to_date) = DATE("9999-12-31")
		 
  ;


  CREATE TEMPORARY TABLE vtl2
    AS
      SELECT
          employee_action_detail_wrk_inc.*
        FROM
          {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_inc
  ;


/*  INSERT THE RECORDS INTO THE WORK TABLE  */

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_3_inc (employee_sid, hr_company_sid, company, action_code, effect_date, action_nbr, employee, ant_end_date, reason_01, user_id, date_stamp, action_type, description, pos_level, lawson_company_num, process_level_code, hr_employee_value_alphanumeric_text, hr_employee_value_num, hr_employee_value_date, lawson_element_num, sequence_num, val_type, source_system_code, dw_last_update_date_time)
    SELECT
        wrk.employee_sid,
        wrk.hr_company_sid,
        wrk.company,
        wrk.action_code,
        wrk.effect_date,
        wrk.action_nbr,
        wrk.employee,
        wrk.ant_end_date,
        wrk.reason_01,
        wrk.user_id,
        wrk.date_stamp,
        wrk.action_type,
        wrk.description,
        wrk.pos_level,
        wrk.lawson_company_num,
        wrk.process_level_code,
        hrhist.hr_employee_value_alphanumeric_text,
        hrhist.hr_employee_value_num,
        hrhist.hr_employee_value_date,
        hrhist.lawson_element_num,
        hrhist.sequence_num,
        'FROM',
        wrk.source_system_code AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        vtl1 AS hrhist
        INNER JOIN -- EDWHR.HR_EMPLOYEE_HISTORY HRHIST
        vtl2 AS wrk ON hrhist.employee_sid = wrk.employee_sid
         AND hrhist.position_level_sequence_num = wrk.pos_level
         AND hrhist.hr_employee_element_date < wrk.effect_date
      WHERE hrhist.lawson_element_num IN(
        -- -$NCR_STG_SCHEMA.EMPLOYEE_ACTION_DETAIL_WRK_INC WRK
        -- JV
        2088, 15, 899, 293, 19, 17, 106, 126, 14, 63, 135, 1414, 64, 136, 68, 18, 21, 16, 73, 28, 288, 30
      )
      QUALIFY row_number() OVER (PARTITION BY hrhist.employee_sid, hrhist.lawson_element_num, hrhist.position_level_sequence_num, hrhist.sequence_num, wrk.action_code, wrk.effect_date, wrk.action_nbr ORDER BY hrhist.hr_employee_element_date DESC, hrhist.sequence_num DESC) = 1
  ;

/* DROP AND RECREATE VOLATILE TABLE TO REDURE IO */

  DROP TABLE vtl1;

  DROP TABLE vtl2;

  CREATE TEMPORARY TABLE vtl1
    AS
      SELECT
          hrhist.*
        FROM
          {{ params.param_hr_base_views_dataset_name }}.hr_employee_history AS hrhist
        WHERE hrhist.lawson_element_num IN(
          2088, 15, 899, 293, 19, 17, 106, 126, 14, 63, 135, 1414, 64, 136, 68, 18, 21, 16, 73, 28, 288, 30
        )
         AND upper(hrhist.source_system_code) = 'L'
         AND date(hrhist.valid_to_date) = DATE("9999-12-31")
       UNION DISTINCT
       SELECT
           hrhist.*
        FROM
         {{ params.param_hr_base_views_dataset_name }}.hr_employee_history AS hrhist
        WHERE hrhist.lawson_element_num IN(
          -- 27989521
          15, 899, 293, 19, 17, 106, 126, 14, 63, 135, 1414, 64, 136, 68, 18, 21, 16, 73, 28, 288, 30
        )
         AND upper(hrhist.source_system_code) = 'A'
        AND date(hrhist.valid_to_date) = DATETIME("9999-12-31")
  ;

  CREATE TEMPORARY TABLE vtl2
    AS
      SELECT
          employee_action_detail_wrk_inc.*
        FROM
          {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_inc
  ;


  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_3_inc (employee_sid, hr_company_sid, company, action_code, effect_date, action_nbr, employee, ant_end_date, reason_01, user_id, date_stamp, action_type, description, pos_level, lawson_company_num, process_level_code, hr_employee_value_alphanumeric_text, hr_employee_value_num, hr_employee_value_date, lawson_element_num, sequence_num, val_type, source_system_code, dw_last_update_date_time)
    SELECT
        wrk.employee_sid,
        wrk.hr_company_sid,
        wrk.company,
        wrk.action_code,
        wrk.effect_date,
        wrk.action_nbr,
        wrk.employee,
        wrk.ant_end_date,
        wrk.reason_01,
        wrk.user_id,
        wrk.date_stamp,
        wrk.action_type,
        wrk.description,
        wrk.pos_level,
        wrk.lawson_company_num,
        wrk.process_level_code,
        hrhist.hr_employee_value_alphanumeric_text,
        hrhist.hr_employee_value_num,
        hrhist.hr_employee_value_date,
        hrhist.lawson_element_num,
        hrhist.sequence_num,
        'TO',
        wrk.source_system_code AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        -- FROM EDWHR.HR_EMPLOYEE_HISTORY HRHIST
        -- INNER JOIN $NCR_STG_SCHEMA.EMPLOYEE_ACTION_DETAIL_WRK_INC WRK
        vtl1 AS hrhist
        INNER JOIN -- EDWHR.HR_EMPLOYEE_HISTORY HRHIST
        vtl2 AS wrk ON hrhist.employee_sid = wrk.employee_sid
         AND hrhist.position_level_sequence_num = wrk.pos_level
         AND hrhist.hr_employee_element_date <= wrk.effect_date
      WHERE hrhist.lawson_element_num IN(
        -- -$NCR_STG_SCHEMA.EMPLOYEE_ACTION_DETAIL_WRK_INC WRK
        -- JV
        2088, 15, 899, 293, 19, 17, 106, 126, 14, 63, 135, 1414, 64, 136, 68, 18, 21, 16, 73, 28, 288, 30
      )
      QUALIFY row_number() OVER (PARTITION BY hrhist.employee_sid, hrhist.lawson_element_num, hrhist.position_level_sequence_num, hrhist.sequence_num, wrk.action_code, wrk.effect_date, wrk.action_nbr ORDER BY hrhist.hr_employee_element_date DESC, hrhist.sequence_num DESC) = 1
  ;

/* DROP VOLATILE TABLE */

  DROP TABLE vtl1;

  DROP TABLE vtl2;

  CREATE TEMPORARY TABLE wrk
    CLUSTER BY action_type
    AS
      SELECT
          wrk.employee,
          wrk.company,
          wrk.action_code,
          coalesce(wrk.action_type, '') AS action_type,
          wrk.employee_sid,
          wrk.action_nbr AS action_sequence_num,
          wrk.effect_date AS action_from_date,
          max(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND upper(wrk.source_system_code) = 'L'
             AND wrk.lawson_element_num = 2088 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS from_auxiliary_status_code,
          max(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 15 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS from_dept_code,
          max(CAST(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 899 THEN wrk.hr_employee_value_num
            ELSE NULL
          END as NUMERIC)) AS from_employee_pay_rate_amount,
          max(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 293 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS from_employee_work_schedule_code,
          max(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 19 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS from_job_code,
          max(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 17 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS from_location_code,
          max(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 106 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS from_overtime_plan_code,
          max(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 126 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS from_position_code,
          max(CAST(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 126 THEN wrk.pos_level
            ELSE NULL
          END as INT64)) AS from_position_level_num,
          max(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 14 THEN CASE
              WHEN coalesce(trim(wrk.hr_employee_value_alphanumeric_text), '') = '' THEN '00000'
              ELSE concat(substr('00000', 1, 5 - length(trim(wrk.hr_employee_value_alphanumeric_text))), trim(wrk.hr_employee_value_alphanumeric_text))
            END
            ELSE CAST(NULL as STRING)
          END) AS from_process_level_code,
          max(CAST(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 63 THEN wrk.hr_employee_value_num
            ELSE NULL
          END as NUMERIC)) AS from_payment_frequency,
          max(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 135 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS from_payment_grade_code,
          max(CAST(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 134 THEN wrk.hr_employee_value_num
            ELSE NULL
          END as NUMERIC)) AS from_payment_step_num,
          max(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 64 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS from_salary_class_code,
          max(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 136 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS from_schedule_code,
          max(CAST(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 68 THEN wrk.hr_employee_value_num
            ELSE NULL
          END as NUMERIC)) AS from_standard_hour,
          max(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 18 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS from_supervisor_code,
          max(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 21 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS from_union_code,
          max(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 16 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS from_user_level,
          max(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 73 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS from_excemption_flag,
          max(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 28 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS from_expense_account_unit,
          max(CAST(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 288 THEN wrk.hr_employee_value_num
            ELSE NULL
          END as INT64)) AS from_expense_company,
          max(CAST(CASE
            WHEN upper(wrk.val_type) = 'FROM'
             AND wrk.lawson_element_num = 30 THEN wrk.hr_employee_value_num
            ELSE NULL
          END as INT64)) AS from_expense_sub_account_num,
          trim(wrk.description) AS hr_code_desc,
          wrk.date_stamp AS last_update_date,
          wrk.effect_date AS last_transfer_eff_date,
          wrk.reason_01 AS reason_desc,
          max(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND upper(wrk.source_system_code) = 'L'
             AND wrk.lawson_element_num = 2088 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS to_auxiliary_status_code,
          max(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 15 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS to_dept_code,
          max(CAST(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 899 THEN wrk.hr_employee_value_num
            ELSE NULL
          END as NUMERIC)) AS to_employee_pay_rate_amt,
          max(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 293 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS to_employee_schedule_code,
          max(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 19 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS to_job_code,
          max(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 17 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS to_location_code,
          max(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 106 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS to_overtime_plan_code,
          max(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 126 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS to_position_code,
          max(CAST(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 126 THEN wrk.pos_level
            ELSE NULL
          END as INT64)) AS to_position_level_num,
          max(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 14 THEN CASE
              WHEN coalesce(trim(wrk.hr_employee_value_alphanumeric_text), '') = '' THEN '00000'
              ELSE concat(substr('00000', 1, 5 - length(trim(wrk.hr_employee_value_alphanumeric_text))), trim(wrk.hr_employee_value_alphanumeric_text))
            END
            ELSE CAST(NULL as STRING)
          END) AS to_process_level_code,
          max(CAST(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 63 THEN wrk.hr_employee_value_num
            ELSE NULL
          END as NUMERIC)) AS to_payment_frequency,
          max(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 135 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS to_pay_grade_code,
          max(CAST(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 134 THEN wrk.hr_employee_value_num
            ELSE NULL
          END as NUMERIC)) AS to_pay_step_num,
          max(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 64 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS to_salary_class_code,
          max(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 136 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS to_schedule_code,
          max(CAST(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 68 THEN wrk.hr_employee_value_num
            ELSE NULL
          END as NUMERIC)) AS to_standard_hour,
          max(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 18 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS to_supervisor_code,
          max(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 21 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS to_union_code,
          max(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 16 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS to_user_level,
          max(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 73 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS to_exemption_flag,
          max(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 28 THEN wrk.hr_employee_value_alphanumeric_text
            ELSE NULL
          END ) AS to_expense_account_unit,
          max(CAST(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 30 THEN wrk.hr_employee_value_num
            ELSE NULL
          END as INT64)) AS to_expense_sub_account_num,
          max(CASE
            WHEN upper(wrk.action_code) = '1TERMPEND' THEN 'T'
            ELSE 'X'
          END) AS transfer_termination_flag,
          max(CAST(CASE
            WHEN upper(wrk.val_type) = 'TO'
             AND wrk.lawson_element_num = 288 THEN wrk.hr_employee_value_num
            ELSE NULL
          END as INT64)) AS to_expense_company_num,
          wrk.lawson_company_num,
          wrk.process_level_code,
          wrk.source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_3_inc AS wrk
        GROUP BY 1, 2, 3, wrk.action_type, 5, 6, 7, 31, 32, 34, 59, 60, 61, upper(CASE

          WHEN upper(wrk.action_code) = '1TERMPEND' THEN 'T'
          ELSE 'X'
        END
        
        );

/*  TRUNCATE WORK TABLE     */

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_5_inc;

/*  INSERT THE RECORDS INTO THE WORK TABLE  */

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_5_inc (employee_num, company, employee_action_sid, action_code, employee_sid, action_sequence_num, action_from_date, from_auxiliary_status_code, from_dept_code, from_employee_pay_rate_amount, from_employee_work_schedule_code, from_job_code, from_location_code, from_overtime_plan_code, from_position_code, from_position_level_num, from_process_level_code, from_payment_frequency, from_payment_grade_code, from_payment_step_num, from_salary_class_code, from_schedule_code, from_standard_hour, from_supervisor_code, from_union_code, from_user_level, from_excemption_flag, from_expense_account_unit, from_expense_company, from_expense_sub_account_num, hr_code_desc, last_update_date, last_transfer_eff_date, reason_desc, to_auxiliary_status_code, to_dept_code, to_employee_pay_rate_amt, to_employee_schedule_code, to_job_code, to_location_code, to_overtime_plan_code, to_position_code, to_position_level_num, to_process_level_code, to_payment_frequency, to_pay_grade_code, to_pay_step_num, to_salary_class_code, to_schedule_code, to_standard_hour, to_supervisor_code, to_union_code, to_user_level, to_exemption_flag, to_expense_account_unit, to_expense_sub_account_num, transfer_termination_flag, to_expense_company_num, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time)
    SELECT
        wrk.employee,
        wrk.company,
        pa.person_action_sid AS employee_action_sid,
        wrk.action_code,
        wrk.employee_sid,
        wrk.action_sequence_num,
        wrk.action_from_date,
        cast(wrk.from_auxiliary_status_code as STRING),
        cast(wrk.from_dept_code as string),
        wrk.from_employee_pay_rate_amount,
        cast(wrk.from_employee_work_schedule_code as STRING),
        cast(wrk.from_job_code as string),
        cast(wrk.from_location_code as STRING),
        cast(wrk.from_overtime_plan_code as STRING),
        cast(wrk.from_position_code as STRING),
        wrk.from_position_level_num,
        cast(wrk.from_process_level_code as STRING),
        wrk.from_payment_frequency,
        cast(wrk.from_payment_grade_code as STRING),
        wrk.from_payment_step_num,
        cast(wrk.from_salary_class_code as STRING),
        cast(wrk.from_schedule_code as STRING),
        wrk.from_standard_hour,
        cast(wrk.from_supervisor_code as STRING),
        cast(wrk.from_union_code as STRING),
        cast(wrk.from_user_level as string),
        cast(wrk.from_excemption_flag as string),
        cast(wrk.from_expense_account_unit as string),
        wrk.from_expense_company  ,
        wrk.from_expense_sub_account_num ,
        wrk.hr_code_desc,
        wrk.last_update_date,
        wrk.last_transfer_eff_date,
        wrk.reason_desc,
        cast(wrk.to_auxiliary_status_code as string),
        cast(wrk.to_dept_code as STRING),
        wrk.to_employee_pay_rate_amt,
        cast(wrk.to_employee_schedule_code as STRING),
        cast(wrk.to_job_code as STRING),
        cast(wrk.to_location_code as STRING),
        cast(wrk.to_overtime_plan_code as STRING),
        cast(wrk.to_position_code as STRING),
        wrk.to_position_level_num,
        cast(wrk.to_process_level_code as STRING),
        wrk.to_payment_frequency,
        cast(wrk.to_pay_grade_code as STRING),
        wrk.to_pay_step_num,
        cast(wrk.to_salary_class_code as STRING),
        cast(wrk.to_schedule_code as STRING),
        wrk.to_standard_hour,
        cast(wrk.to_supervisor_code as STRING),
        cast(wrk.to_union_code as STRING),
        cast(wrk.to_user_level as string),
        cast(wrk.to_exemption_flag as string),
        cast(wrk.to_expense_account_unit as string),
        wrk.to_expense_sub_account_num,
        cast(wrk.transfer_termination_flag as string),
        wrk.to_expense_company_num,
        wrk.lawson_company_num,
        cast(wrk.process_level_code as STRING),
        cast(wrk.source_system_code as STRING),
        wrk.dw_last_update_date_time
      FROM
        wrk
        INNER JOIN (
          SELECT
              per_act.person_action_sid,
              per_act.lawson_company_num,
              per_act.employee_num,
              per_act.action_code,
              coalesce(trim(per_act.action_type_code), '') AS action_type_code,
              per_act.action_sequence_num
            FROM
              {{ params.param_hr_base_views_dataset_name }}.person_action AS per_act
            WHERE date(per_act.valid_to_date) = DATE("9999-12-31")
            QUALIFY row_number() OVER (PARTITION BY per_act.lawson_company_num, per_act.employee_num, trim(per_act.action_code), upper(trim(action_type_code)), per_act.action_sequence_num ORDER BY per_act.eff_from_date DESC) = 1
        ) AS pa ON wrk.company = pa.lawson_company_num
         AND wrk.employee = pa.employee_num
         AND upper(trim(wrk.action_code)) = upper(trim(pa.action_code))
         AND upper(trim(wrk.action_type)) = upper(trim(pa.action_type_code))
         AND wrk.action_sequence_num = pa.action_sequence_num
  ;



/*  TRUNCATE WORK TABLE     */

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_6_inc;

BEGIN TRANSACTION ;
INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_6_inc (employee_num, employee_action_sid, eff_from_date, action_code, employee_sid, action_sequence_num, action_from_date, action_to_date, from_auxiliary_status_code, from_dept_code, from_employee_pay_rate_amount, from_employee_work_schedule_code, from_job_code, from_location_code, from_overtime_plan_code, from_position_code, from_position_level_num, from_process_level_code, from_payment_frequency, from_payment_grade_code, from_payment_step_num, from_salary_class_code, from_schedule_code, from_standard_hour, from_supervisor_code, from_union_code, from_user_level, from_excemption_flag, from_expense_account_unit, from_expense_company, from_expense_sub_account_num, hr_code_desc, last_update_date, last_transfer_eff_date, reason_desc, to_auxiliary_status_code, to_dept_code, to_employee_pay_rate_amt, to_employee_schedule_code, to_job_code, to_location_code, to_overtime_plan_code, to_position_code, to_position_level_num, to_process_level_code, to_payment_frequency, to_pay_grade_code, to_pay_step_num, to_salary_class_code, to_schedule_code, to_standard_hour, to_supervisor_code, to_union_code, to_user_level, to_exemption_flag, to_expense_account_unit, to_expense_sub_account_num, transfer_termination_flag, to_expense_company_num, active_dw_ind, security_key_text, delete_ind, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time, valid_from_date, valid_to_date)
  WITH txn_reset_ind AS (
    SELECT
        -- EFF_TO_DATE,
        -- DATE '9999-12-31' AS EFF_TO_DATE,
        -- TRIM(COMPANY)||'-'||CASE WHEN TRIM(COALESCE(TO_PROCESS_LEVEL_CODE,'')) = '' THEN '00000' ELSE TRIM(TO_PROCESS_LEVEL_CODE) END||'-'||CASE WHEN TRIM(COALESCE(TO_DEPT_CODE,'')) = '' THEN '00000' ELSE TRIM(TO_DEPT_CODE) END AS SECURITY_KEY_TEXT,
        employee_action_detail_wrk_5_inc.employee_num,
        employee_action_detail_wrk_5_inc.employee_action_sid,
        employee_action_detail_wrk_5_inc.action_from_date,
        employee_action_detail_wrk_5_inc.action_code,
        employee_action_detail_wrk_5_inc.employee_sid,
        employee_action_detail_wrk_5_inc.action_sequence_num,
        employee_action_detail_wrk_5_inc.from_auxiliary_status_code,
        employee_action_detail_wrk_5_inc.from_dept_code,
        employee_action_detail_wrk_5_inc.from_employee_pay_rate_amount,
        employee_action_detail_wrk_5_inc.from_employee_work_schedule_code,
        employee_action_detail_wrk_5_inc.from_job_code,
        employee_action_detail_wrk_5_inc.from_location_code,
        employee_action_detail_wrk_5_inc.from_overtime_plan_code,
        employee_action_detail_wrk_5_inc.from_position_code,
        employee_action_detail_wrk_5_inc.from_position_level_num,
        employee_action_detail_wrk_5_inc.from_process_level_code,
        employee_action_detail_wrk_5_inc.from_payment_frequency,
        employee_action_detail_wrk_5_inc.from_payment_grade_code,
        employee_action_detail_wrk_5_inc.from_payment_step_num,
        employee_action_detail_wrk_5_inc.from_salary_class_code,
        employee_action_detail_wrk_5_inc.from_schedule_code,
        employee_action_detail_wrk_5_inc.from_standard_hour,
        employee_action_detail_wrk_5_inc.from_supervisor_code,
        employee_action_detail_wrk_5_inc.from_union_code,
        employee_action_detail_wrk_5_inc.from_user_level,
        employee_action_detail_wrk_5_inc.from_excemption_flag,
        employee_action_detail_wrk_5_inc.from_expense_account_unit,
        employee_action_detail_wrk_5_inc.from_expense_company,
        employee_action_detail_wrk_5_inc.from_expense_sub_account_num,
        employee_action_detail_wrk_5_inc.hr_code_desc,
        employee_action_detail_wrk_5_inc.last_update_date,
        employee_action_detail_wrk_5_inc.last_transfer_eff_date,
        employee_action_detail_wrk_5_inc.reason_desc,
        employee_action_detail_wrk_5_inc.to_auxiliary_status_code,
        employee_action_detail_wrk_5_inc.to_dept_code,
        employee_action_detail_wrk_5_inc.to_employee_pay_rate_amt,
        employee_action_detail_wrk_5_inc.to_employee_schedule_code,
        employee_action_detail_wrk_5_inc.to_job_code,
        employee_action_detail_wrk_5_inc.to_location_code,
        employee_action_detail_wrk_5_inc.to_overtime_plan_code,
        employee_action_detail_wrk_5_inc.to_position_code,
        employee_action_detail_wrk_5_inc.to_position_level_num,
        employee_action_detail_wrk_5_inc.to_process_level_code,
        employee_action_detail_wrk_5_inc.to_payment_frequency,
        employee_action_detail_wrk_5_inc.to_pay_grade_code,
        employee_action_detail_wrk_5_inc.to_pay_step_num,
        employee_action_detail_wrk_5_inc.to_salary_class_code,
        employee_action_detail_wrk_5_inc.to_schedule_code,
        employee_action_detail_wrk_5_inc.to_standard_hour,
        employee_action_detail_wrk_5_inc.to_supervisor_code,
        employee_action_detail_wrk_5_inc.to_union_code,
        employee_action_detail_wrk_5_inc.to_user_level,
        employee_action_detail_wrk_5_inc.to_exemption_flag,
        employee_action_detail_wrk_5_inc.to_expense_account_unit,
        employee_action_detail_wrk_5_inc.to_expense_sub_account_num,
        employee_action_detail_wrk_5_inc.transfer_termination_flag,
        employee_action_detail_wrk_5_inc.to_expense_company_num,
        employee_action_detail_wrk_5_inc.company,
        employee_action_detail_wrk_5_inc.lawson_company_num,
        employee_action_detail_wrk_5_inc.process_level_code,
        employee_action_detail_wrk_5_inc.source_system_code,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_auxiliary_status_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind1,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_dept_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind2,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_employee_pay_rate_amount IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind3,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_employee_work_schedule_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind4,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_job_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind5,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_location_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind6,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_overtime_plan_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind7,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_position_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind8,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_position_level_num IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind9,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_process_level_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind10,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_payment_frequency IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind11,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_payment_grade_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind12,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_payment_step_num IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind13,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_salary_class_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind14,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_schedule_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind15,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_standard_hour IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind16,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_supervisor_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind17,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_union_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind18,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_user_level IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind19,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_excemption_flag IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind20,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_expense_account_unit IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind21,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_expense_company IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind22,
        CASE
          WHEN employee_action_detail_wrk_5_inc.from_expense_sub_account_num IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind23,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_auxiliary_status_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind24,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_dept_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind25,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_employee_pay_rate_amt IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind26,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_employee_schedule_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind27,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_job_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind28,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_location_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind29,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_overtime_plan_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind30,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_position_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind31,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_position_level_num IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind32,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_process_level_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind33,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_payment_frequency IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind34,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_pay_grade_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind35,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_pay_step_num IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind36,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_salary_class_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind37,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_schedule_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind38,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_standard_hour IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind39,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_supervisor_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind40,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_union_code IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind41,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_user_level IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind42,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_exemption_flag IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind43,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_expense_account_unit IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind44,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_expense_sub_account_num IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind45,
        CASE
          WHEN employee_action_detail_wrk_5_inc.to_expense_company_num IS NOT NULL THEN 1
          ELSE 0
        END AS reset_ind46
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_5_inc
  ), txn_cum_reset_ind AS (
    SELECT
        txn_t.employee_num,
        txn_t.employee_action_sid,
        txn_t.action_from_date,
        txn_t.action_code,
        txn_t.employee_sid,
        txn_t.action_sequence_num,
        txn_t.from_auxiliary_status_code,
        txn_t.from_dept_code,
        txn_t.from_employee_pay_rate_amount,
        txn_t.from_employee_work_schedule_code,
        txn_t.from_job_code,
        txn_t.from_location_code,
        txn_t.from_overtime_plan_code,
        txn_t.from_position_code,
        txn_t.from_position_level_num,
        txn_t.from_process_level_code,
        txn_t.from_payment_frequency,
        txn_t.from_payment_grade_code,
        txn_t.from_payment_step_num,
        txn_t.from_salary_class_code,
        txn_t.from_schedule_code,
        txn_t.from_standard_hour,
        txn_t.from_supervisor_code,
        txn_t.from_union_code,
        txn_t.from_user_level,
        txn_t.from_excemption_flag,
        txn_t.from_expense_account_unit,
        txn_t.from_expense_company,
        txn_t.from_expense_sub_account_num,
        txn_t.hr_code_desc,
        txn_t.last_update_date,
        txn_t.last_transfer_eff_date,
        txn_t.reason_desc,
        txn_t.to_auxiliary_status_code,
        txn_t.to_dept_code,
        txn_t.to_employee_pay_rate_amt,
        txn_t.to_employee_schedule_code,
        txn_t.to_job_code,
        txn_t.to_location_code,
        txn_t.to_overtime_plan_code,
        txn_t.to_position_code,
        txn_t.to_position_level_num,
        txn_t.to_process_level_code,
        txn_t.to_payment_frequency,
        txn_t.to_pay_grade_code,
        txn_t.to_pay_step_num,
        txn_t.to_salary_class_code,
        txn_t.to_schedule_code,
        txn_t.to_standard_hour,
        txn_t.to_supervisor_code,
        txn_t.to_union_code,
        txn_t.to_user_level,
        txn_t.to_exemption_flag,
        txn_t.to_expense_account_unit,
        txn_t.to_expense_sub_account_num,
        txn_t.transfer_termination_flag,
        txn_t.to_expense_company_num,
        txn_t.company,
        txn_t.lawson_company_num,
        txn_t.process_level_code,
        txn_t.source_system_code,
        sum(txn_t.reset_ind1) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind1,
        sum(txn_t.reset_ind2) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind2,
        sum(txn_t.reset_ind3) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind3,
        sum(txn_t.reset_ind4) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind4,
        sum(txn_t.reset_ind5) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind5,
        sum(txn_t.reset_ind6) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind6,
        sum(txn_t.reset_ind7) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind7,
        sum(txn_t.reset_ind8) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind8,
        sum(txn_t.reset_ind9) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind9,
        sum(txn_t.reset_ind10) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind10,
        sum(txn_t.reset_ind11) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind11,
        sum(txn_t.reset_ind12) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind12,
        sum(txn_t.reset_ind13) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind13,
        sum(txn_t.reset_ind14) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind14,
        sum(txn_t.reset_ind15) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind15,
        sum(txn_t.reset_ind16) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind16,
        sum(txn_t.reset_ind17) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind17,
        sum(txn_t.reset_ind18) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind18,
        sum(txn_t.reset_ind19) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind19,
        sum(txn_t.reset_ind20) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind20,
        sum(txn_t.reset_ind21) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind21,
        sum(txn_t.reset_ind22) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind22,
        sum(txn_t.reset_ind23) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind23,
        sum(txn_t.reset_ind24) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind24,
        sum(txn_t.reset_ind25) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind25,
        sum(txn_t.reset_ind26) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind26,
        sum(txn_t.reset_ind27) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind27,
        sum(txn_t.reset_ind28) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind28,
        sum(txn_t.reset_ind29) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind29,
        sum(txn_t.reset_ind30) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind30,
        sum(txn_t.reset_ind31) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind31,
        sum(txn_t.reset_ind32) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind32,
        sum(txn_t.reset_ind33) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind33,
        sum(txn_t.reset_ind34) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind34,
        sum(txn_t.reset_ind35) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind35,
        sum(txn_t.reset_ind36) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind36,
        sum(txn_t.reset_ind37) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind37,
        sum(txn_t.reset_ind38) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind38,
        sum(txn_t.reset_ind39) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind39,
        sum(txn_t.reset_ind40) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind40,
        sum(txn_t.reset_ind41) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind41,
        sum(txn_t.reset_ind42) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind42,
        sum(txn_t.reset_ind43) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind43,
        sum(txn_t.reset_ind44) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind44,
        sum(txn_t.reset_ind45) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind45,
        sum(txn_t.reset_ind46) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS cum_reset_ind46
      FROM
        txn_reset_ind AS txn_t
  )
  SELECT
      txn_t.employee_num,
      txn_t.employee_action_sid,
      txn_t.action_from_date AS eff_from_date,
      txn_t.action_code,
      txn_t.employee_sid,
      txn_t.action_sequence_num,
      txn_t.action_from_date,
      DATE '9999-12-31' AS action_to_date,
      max(txn_t.from_auxiliary_status_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind1 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_auxiliary_status_code,
      max(txn_t.from_dept_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind2 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_dept_code,
      max(txn_t.from_employee_pay_rate_amount) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind3 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_employee_pay_rate_amount,
      max(txn_t.from_employee_work_schedule_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind4 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_employee_work_schedule_code,
      max(txn_t.from_job_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind5 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_job_code,
      max(txn_t.from_location_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind6 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_location_code,
      max(txn_t.from_overtime_plan_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind7 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_overtime_plan_code,
      max(txn_t.from_position_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind8 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_position_code,
      max(txn_t.from_position_level_num) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind9 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_position_level_num,
      max(txn_t.from_process_level_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind10 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_process_level_code,
      max(txn_t.from_payment_frequency) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind11 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_payment_frequency,
      max(txn_t.from_payment_grade_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind12 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_payment_grade_code,
      max(txn_t.from_payment_step_num) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind13 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_payment_step_num,
      max(txn_t.from_salary_class_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind14 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_salary_class_code,
      max(txn_t.from_schedule_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind15 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_schedule_code,
      max(txn_t.from_standard_hour) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind16 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_standard_hour,
      max(txn_t.from_supervisor_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind17 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_supervisor_code,
      max(txn_t.from_union_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind18 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_union_code,
      max(txn_t.from_user_level) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind19 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_user_level,
      max(txn_t.from_excemption_flag) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind20 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_excemption_flag,
      max(txn_t.from_expense_account_unit) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind21 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_expense_account_unit,
      max(txn_t.from_expense_company) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind22 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_expense_company,
      max(txn_t.from_expense_sub_account_num) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind23 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS from_expense_sub_account_num,
      txn_t.hr_code_desc,
      txn_t.last_update_date,
      txn_t.last_transfer_eff_date,
      txn_t.reason_desc,
      max(txn_t.to_auxiliary_status_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind24 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_auxiliary_status_code,
      max(txn_t.to_dept_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind25 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_dept_code,
      max(txn_t.to_employee_pay_rate_amt) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind26 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_employee_pay_rate_amt,
      max(txn_t.to_employee_schedule_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind27 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_employee_schedule_code,
      max(txn_t.to_job_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind28 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_job_code,
      max(txn_t.to_location_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind29 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_location_code,
      max(txn_t.to_overtime_plan_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind30 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_overtime_plan_code,
      max(txn_t.to_position_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind31 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_position_code,
      max(txn_t.to_position_level_num) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind32 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_position_level_num,
      max(txn_t.to_process_level_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind33 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_process_level_code,
      max(txn_t.to_payment_frequency) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind34 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_payment_frequency,
      max(txn_t.to_pay_grade_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind35 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_pay_grade_code,
      max(txn_t.to_pay_step_num) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind36 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_pay_step_num,
      max(txn_t.to_salary_class_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind37 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_salary_class_code,
      max(txn_t.to_schedule_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind38 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_schedule_code,
      max(txn_t.to_standard_hour) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind39 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_standard_hour,
      max(txn_t.to_supervisor_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind40 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_supervisor_code,
      max(txn_t.to_union_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind41 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_union_code,
      max(txn_t.to_user_level) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind42 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_user_level,
      max(txn_t.to_exemption_flag) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind43 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_exemption_flag,
      max(txn_t.to_expense_account_unit) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind44 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_expense_account_unit,
      max(txn_t.to_expense_sub_account_num) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind45 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_expense_sub_account_num,
      txn_t.transfer_termination_flag,
      max(txn_t.to_expense_company_num) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind46 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) AS to_expense_company_num,
      'Y' AS active_dw_ind,
      concat(CASE
        WHEN txn_t.company IS NULL THEN '00000'
        ELSE concat(substr('00000', 1, 5 - length(trim(cast(txn_t.company as string)))), trim(cast(txn_t.company as string)))
      END, '-', CASE
        WHEN trim(CAST(max(txn_t.to_process_level_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind33 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) as STRING)) IS NULL
         OR trim(CAST(max(txn_t.to_process_level_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind33 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) as STRING)) = '' THEN '00000'
        ELSE concat(substr('00000', 1, 5 - length(trim(CAST(max(txn_t.to_process_level_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind33 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) as STRING)))), trim(CAST(max(txn_t.to_process_level_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind33 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) as STRING)))
      END, '-', CASE
        WHEN trim(coalesce(CAST(max(txn_t.to_dept_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind25 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) as STRING), '')) = '' THEN '00000'
        ELSE concat(substr('00000', 1, 5 - length(trim(CAST(max(txn_t.to_dept_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind25 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) as STRING)))), trim(CAST(max(txn_t.to_dept_code) OVER (PARTITION BY txn_t.employee_action_sid, txn_t.employee_sid, txn_t.cum_reset_ind25 ORDER BY txn_t.action_from_date ROWS UNBOUNDED PRECEDING) as STRING)))
      END) AS security_key_text,
      'A' AS delete_ind,
      txn_t.lawson_company_num,
      txn_t.process_level_code,
      txn_t.source_system_code AS source_system_code,
      timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
      current_datetime('US/Central') AS valid_from_date,
      DATETIME('9999-12-31 23:59:59') AS valid_to_date
    FROM
      txn_cum_reset_ind AS txn_t
;

/* Test Unique Primary Index constraint set in Terdata on  employee_action_detail_wrk_6_in */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select employee_action_sid ,eff_from_date ,valid_from_date 
        from {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_6_inc
        group by employee_action_sid ,eff_from_date ,valid_from_date 		
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: employee_action_detail_wrk_6_inc');
    ELSE
      COMMIT TRANSACTION;
    END IF;  
  
  
  UPDATE {{ params.param_hr_core_dataset_name }}.employee_action_detail AS tgt SET valid_to_date = (current_datetime('US/Central') - INTERVAL 1 second), active_dw_ind = 'N', dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) FROM {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_6_inc AS stg WHERE tgt.employee_action_sid = stg.employee_action_sid
 AND tgt.eff_from_date = stg.action_from_date
 AND date(tgt.valid_to_date) = DATE '9999-12-31'
 --AND tgt.source_system_code = 'L'
 --AND tgt.lawson_company_num <> 300
 AND (coalesce(tgt.action_sequence_num, 123456789 ) <> coalesce(stg.action_sequence_num, 123456789 )
 OR tgt.action_from_date <> stg.action_from_date
 OR tgt.action_to_date <> stg.action_to_date
 OR coalesce(trim(tgt.from_auxiliary_status_code), 'XXX') <> coalesce(trim(stg.from_auxiliary_status_code), 'XXX')
 OR coalesce(trim(tgt.from_dept_code), 'XXX') <> coalesce(trim(stg.from_dept_code), 'XXX')
 OR coalesce(tgt.from_employee_pay_rate_amount, 123456789) <> coalesce(stg.from_employee_pay_rate_amount, 123456789)
 OR coalesce(trim(tgt.from_employee_work_schedule_code), 'XXX') <> coalesce(trim(stg.from_employee_work_schedule_code), 'XXX')
 OR coalesce(trim(tgt.from_job_code), 'XXX') <> coalesce(trim(stg.from_job_code), 'XXX')
 OR coalesce(trim(tgt.from_location_code), 'XXX') <> coalesce(trim(stg.from_location_code), 'XXX')
 OR coalesce(trim(tgt.from_overtime_plan_code), 'XXX') <> coalesce(trim(stg.from_overtime_plan_code), 'XXX')
 OR coalesce(trim(tgt.from_position_code), 'XXX') <> coalesce(trim(stg.from_position_code), 'XXX')
 OR coalesce(tgt.from_position_level_num, 123456789 ) <> coalesce(stg.from_position_level_num,123456789)
 OR coalesce(trim(tgt.from_process_level_code), 'XXX') <> coalesce(trim(stg.from_process_level_code), 'XXX')
 OR coalesce(tgt.from_payment_frequency, 123456789) <> coalesce(stg.from_payment_frequency,123456789)
 OR coalesce(trim(tgt.from_payment_grade_code), 'XXX') <> coalesce(trim(stg.from_payment_grade_code), 'XXX')
 OR coalesce(tgt.from_payment_step_num,123456789) <> coalesce(stg.from_payment_step_num,123456789)
 OR coalesce(trim(tgt.from_salary_class_code), 'XXX') <> coalesce(trim(stg.from_salary_class_code), 'XXX')
 OR coalesce(trim(tgt.from_schedule_code), 'XXX') <> coalesce(trim(stg.from_schedule_code), 'XXX')
 OR coalesce(tgt.from_standard_hour, 123456789) <> coalesce(stg.from_standard_hour, 123456789)
 OR coalesce(trim(tgt.from_supervisor_code), 'XXX') <> coalesce(trim(stg.from_supervisor_code), 'XXX')
 OR coalesce(trim(tgt.from_union_code), 'XXX') <> coalesce(trim(stg.from_union_code), 'XXX')
 OR coalesce(trim(tgt.from_user_level), 'XXX') <> coalesce(trim(stg.from_user_level), 'XXX')
 OR coalesce(trim(tgt.from_excemption_flag), 'XXX') <> coalesce(trim(stg.from_excemption_flag), 'XXX')
 OR coalesce(trim(tgt.from_expense_account_unit), 'XXX') <> coalesce(trim(stg.from_expense_account_unit), 'XXX')
 OR coalesce(tgt.from_expense_company,123456789) <> coalesce(stg.from_expense_company,123456789)
 OR coalesce(tgt.from_expense_sub_account_num, 123456789) <> coalesce(stg.from_expense_sub_account_num, 123456789)
 OR coalesce(trim(tgt.hr_code_desc), 'XXX') <> coalesce(trim(stg.hr_code_desc), 'XXX')
 OR tgt.last_update_date <> stg.last_update_date
 OR tgt.last_transfer_eff_date <> stg.last_transfer_eff_date
 OR coalesce(trim(tgt.reason_desc), 'XXX') <> coalesce(trim(stg.reason_desc), 'XXX')
 OR coalesce(trim(tgt.to_auxiliary_status_code), 'XXX') <> coalesce(trim(stg.to_auxiliary_status_code), 'XXX')
 OR coalesce(trim(tgt.to_dept_code), 'XXX') <> coalesce(trim(stg.to_dept_code), 'XXX')
 OR coalesce(tgt.to_employee_pay_rate_amt,123456789) <> coalesce(stg.to_employee_pay_rate_amt,123456789)
 OR coalesce(trim(tgt.to_employee_schedule_code), 'XXX') <> coalesce(trim(stg.to_employee_schedule_code), 'XXX')
 OR coalesce(trim(tgt.to_job_code), 'XXX') <> coalesce(trim(stg.to_job_code), 'XXX')
 OR coalesce(trim(tgt.to_location_code), 'XXX') <> coalesce(trim(stg.to_location_code), 'XXX')
 OR coalesce(trim(tgt.to_overtime_plan_code), 'XXX') <> coalesce(trim(stg.to_overtime_plan_code), 'XXX')
 OR coalesce(trim(tgt.to_position_code), 'XXX') <> coalesce(trim(stg.to_position_code), 'XXX')
 OR coalesce(tgt.to_position_level_num,123456789) <> coalesce(stg.to_position_level_num,123456789)
 OR coalesce(trim(tgt.to_process_level_code), 'XXX') <> coalesce(trim(stg.to_process_level_code), 'XXX')
 OR coalesce(tgt.to_payment_frequency,123456789) <> coalesce(stg.to_payment_frequency,123456789)
 OR coalesce(trim(tgt.to_pay_grade_code), 'XXX') <> coalesce(trim(stg.to_pay_grade_code), 'XXX')
 OR coalesce(tgt.to_pay_step_num,123456789) <> coalesce(stg.to_pay_step_num,123456789)
 OR coalesce(trim(tgt.to_salary_class_code), 'XXX') <> coalesce(trim(stg.to_salary_class_code), 'XXX')
 OR coalesce(trim(tgt.to_schedule_code), 'XXX') <> coalesce(trim(stg.to_schedule_code), 'XXX')
 OR coalesce(tgt.to_standard_hour,123456789) <> coalesce(stg.to_standard_hour,123456789)
 OR coalesce(trim(tgt.to_supervisor_code), 'XXX') <> coalesce(trim(stg.to_supervisor_code), 'XXX')
 OR coalesce(trim(tgt.to_union_code), 'XXX') <> coalesce(trim(stg.to_union_code), 'XXX')
 OR coalesce(trim(tgt.to_user_level), 'XXX') <> coalesce(trim(stg.to_user_level), 'XXX')
 OR coalesce(trim(tgt.to_exemption_flag), 'XXX') <> coalesce(trim(stg.to_exemption_flag), 'XXX')
 OR coalesce(trim(tgt.to_expense_account_unit), 'XXX') <> coalesce(trim(stg.to_expense_account_unit), 'XXX')
 OR coalesce(tgt.to_expense_sub_account_num,123456789) <> coalesce(stg.to_expense_sub_account_num,123456789)
 OR coalesce(trim(tgt.transfer_termination_flag), 'XXX') <> coalesce(trim(stg.transfer_termination_flag), 'XXX')
 OR coalesce(tgt.to_expense_company_num,123456789) <> coalesce(stg.to_expense_company_num,123456789)
 OR coalesce(trim(tgt.security_key_text), 'XXX') <> coalesce(trim(stg.security_key_text), 'XXX')
 OR coalesce(trim(tgt.process_level_code), 'XXX') <> coalesce(trim(stg.process_level_code), 'XXX')
 OR coalesce(trim(tgt.source_system_code), 'XXX') <> coalesce(trim(stg.source_system_code), 'XXX'));
/*  INSERT THE NEW RECORDS/CHANGES INTO THE TARGET TABLE  */
 INSERT INTO {{ params.param_hr_core_dataset_name }}.employee_action_detail (employee_num, employee_action_sid, eff_from_date, action_code, employee_sid, action_sequence_num, action_from_date, action_to_date, from_auxiliary_status_code, from_dept_code, from_employee_pay_rate_amount, from_employee_work_schedule_code, from_job_code, from_location_code, from_overtime_plan_code, from_position_code, from_position_level_num, from_process_level_code, from_payment_frequency, from_payment_grade_code, from_payment_step_num, from_salary_class_code, from_schedule_code, from_standard_hour, from_supervisor_code, from_union_code, from_user_level, from_excemption_flag, from_expense_account_unit, from_expense_company, from_expense_sub_account_num, hr_code_desc, last_update_date, last_transfer_eff_date, reason_desc, to_auxiliary_status_code, to_dept_code, to_employee_pay_rate_amt, to_employee_schedule_code, to_job_code, to_location_code, to_overtime_plan_code, to_position_code, to_position_level_num, to_process_level_code, to_payment_frequency, to_pay_grade_code, to_pay_step_num, to_salary_class_code, to_schedule_code, to_standard_hour, to_supervisor_code, to_union_code, to_user_level, to_exemption_flag, to_expense_account_unit, to_expense_sub_account_num, transfer_termination_flag, to_expense_company_num, active_dw_ind, security_key_text, delete_ind, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time, valid_from_date, valid_to_date)
    SELECT
        -- EFF_TO_DATE,
        employee_action_detail_wrk_6_inc.employee_num,
        employee_action_detail_wrk_6_inc.employee_action_sid,
        employee_action_detail_wrk_6_inc.eff_from_date,
        employee_action_detail_wrk_6_inc.action_code,
        employee_action_detail_wrk_6_inc.employee_sid,
        employee_action_detail_wrk_6_inc.action_sequence_num,
        employee_action_detail_wrk_6_inc.action_from_date,
        employee_action_detail_wrk_6_inc.action_to_date,
        employee_action_detail_wrk_6_inc.from_auxiliary_status_code,
        employee_action_detail_wrk_6_inc.from_dept_code,
        employee_action_detail_wrk_6_inc.from_employee_pay_rate_amount,
        employee_action_detail_wrk_6_inc.from_employee_work_schedule_code,
        employee_action_detail_wrk_6_inc.from_job_code,
        employee_action_detail_wrk_6_inc.from_location_code,
        employee_action_detail_wrk_6_inc.from_overtime_plan_code,
        employee_action_detail_wrk_6_inc.from_position_code,
        employee_action_detail_wrk_6_inc.from_position_level_num,
        employee_action_detail_wrk_6_inc.from_process_level_code,
        employee_action_detail_wrk_6_inc.from_payment_frequency,
        employee_action_detail_wrk_6_inc.from_payment_grade_code,
        employee_action_detail_wrk_6_inc.from_payment_step_num,
        employee_action_detail_wrk_6_inc.from_salary_class_code,
        employee_action_detail_wrk_6_inc.from_schedule_code,
        employee_action_detail_wrk_6_inc.from_standard_hour,
        employee_action_detail_wrk_6_inc.from_supervisor_code,
        employee_action_detail_wrk_6_inc.from_union_code,
        employee_action_detail_wrk_6_inc.from_user_level,
        employee_action_detail_wrk_6_inc.from_excemption_flag,
        employee_action_detail_wrk_6_inc.from_expense_account_unit,
        employee_action_detail_wrk_6_inc.from_expense_company,
        employee_action_detail_wrk_6_inc.from_expense_sub_account_num,
        employee_action_detail_wrk_6_inc.hr_code_desc,
        employee_action_detail_wrk_6_inc.last_update_date,
        employee_action_detail_wrk_6_inc.last_transfer_eff_date,
        employee_action_detail_wrk_6_inc.reason_desc,
        employee_action_detail_wrk_6_inc.to_auxiliary_status_code,
        employee_action_detail_wrk_6_inc.to_dept_code,
        employee_action_detail_wrk_6_inc.to_employee_pay_rate_amt,
        employee_action_detail_wrk_6_inc.to_employee_schedule_code,
        employee_action_detail_wrk_6_inc.to_job_code,
        employee_action_detail_wrk_6_inc.to_location_code,
        employee_action_detail_wrk_6_inc.to_overtime_plan_code,
        employee_action_detail_wrk_6_inc.to_position_code,
        employee_action_detail_wrk_6_inc.to_position_level_num,
        employee_action_detail_wrk_6_inc.to_process_level_code,
        employee_action_detail_wrk_6_inc.to_payment_frequency,
        employee_action_detail_wrk_6_inc.to_pay_grade_code,
        employee_action_detail_wrk_6_inc.to_pay_step_num,
        employee_action_detail_wrk_6_inc.to_salary_class_code,
        employee_action_detail_wrk_6_inc.to_schedule_code,
        employee_action_detail_wrk_6_inc.to_standard_hour,
        employee_action_detail_wrk_6_inc.to_supervisor_code,
        employee_action_detail_wrk_6_inc.to_union_code,
        employee_action_detail_wrk_6_inc.to_user_level,
        employee_action_detail_wrk_6_inc.to_exemption_flag,
        employee_action_detail_wrk_6_inc.to_expense_account_unit,
        employee_action_detail_wrk_6_inc.to_expense_sub_account_num,
        employee_action_detail_wrk_6_inc.transfer_termination_flag,
        employee_action_detail_wrk_6_inc.to_expense_company_num,
        -- EFF_TO_DATE,
        employee_action_detail_wrk_6_inc.active_dw_ind,
        employee_action_detail_wrk_6_inc.security_key_text,
        employee_action_detail_wrk_6_inc.delete_ind,
        employee_action_detail_wrk_6_inc.lawson_company_num,
        employee_action_detail_wrk_6_inc.process_level_code,
        employee_action_detail_wrk_6_inc.source_system_code,
        employee_action_detail_wrk_6_inc.dw_last_update_date_time,
        employee_action_detail_wrk_6_inc.valid_from_date,
        employee_action_detail_wrk_6_inc.valid_to_date
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_6_inc
      WHERE (
	   --employee_action_detail_wrk_6_inc.employee_num
	   coalesce(trim(cast(employee_action_detail_wrk_6_inc.employee_num as string)), '')
	   
	  ||employee_action_detail_wrk_6_inc.employee_action_sid
	  ||trim(employee_action_detail_wrk_6_inc.action_code )
	  ||employee_action_detail_wrk_6_inc.employee_sid 
	  ||employee_action_detail_wrk_6_inc.action_sequence_num
	  ||employee_action_detail_wrk_6_inc.action_from_date 
	  ||coalesce(cast(trim(employee_action_detail_wrk_6_inc.from_auxiliary_status_code) as STRING), '')
	  
	  --||employee_action_detail_wrk_6_inc.from_dept_code
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_dept_code as string)), '')
	  --||employee_action_detail_wrk_6_inc.from_employee_pay_rate_amount
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_employee_pay_rate_amount as string)), '')
	  
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_employee_work_schedule_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_job_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_location_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_overtime_plan_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_position_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_position_level_num as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_process_level_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_payment_frequency as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_payment_grade_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_payment_step_num as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_salary_class_code as string)), '')
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_schedule_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_standard_hour as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_supervisor_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_union_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_user_level as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_excemption_flag as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_expense_account_unit as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_expense_company as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.from_expense_sub_account_num as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.hr_code_desc as string)), '') 
	  ||employee_action_detail_wrk_6_inc.last_update_date
	  
	  --||employee_action_detail_wrk_6_inc.last_transfer_eff_date 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.last_transfer_eff_date as string)), '')
	  
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.reason_desc as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_payment_frequency as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_auxiliary_status_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_dept_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_employee_pay_rate_amt as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_employee_schedule_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_job_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_location_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_overtime_plan_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_position_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_position_level_num as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_process_level_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_pay_grade_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_pay_step_num as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_salary_class_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_schedule_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_standard_hour as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_supervisor_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_union_code as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_user_level as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_exemption_flag as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_expense_account_unit as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_expense_sub_account_num as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.transfer_termination_flag as string)), '') 
	  ||coalesce(trim(cast(employee_action_detail_wrk_6_inc.to_expense_company_num as string)), '') 
	  ||employee_action_detail_wrk_6_inc.lawson_company_num 
	  ||employee_action_detail_wrk_6_inc.process_level_code 
	  ||employee_action_detail_wrk_6_inc.source_system_code) 
	  
	  NOT IN(
        SELECT  
		--employee_action_detail.employee_num
		coalesce(trim(cast(employee_action_detail.employee_num as STRING )),'')
		
             ||employee_action_detail.employee_action_sid
             ||trim(employee_action_detail.action_code)
             ||employee_action_detail.employee_sid
             ||employee_action_detail.action_sequence_num
             ||employee_action_detail.action_from_date
              ||coalesce(trim(cast(employee_action_detail.from_auxiliary_status_code as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_dept_code as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_employee_pay_rate_amount as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_employee_work_schedule_code as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_job_code as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_location_code  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_overtime_plan_code as STRING)), '')
            ||coalesce(trim(cast(employee_action_detail.from_position_code  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_position_level_num as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_process_level_code  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_payment_frequency as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_payment_grade_code  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_payment_step_num as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_salary_class_code  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_schedule_code  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_standard_hour as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_supervisor_code  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_union_code  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_user_level  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_excemption_flag  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_expense_account_unit  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_expense_company as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.from_expense_sub_account_num as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.hr_code_desc  as STRING )),'')
            ||employee_action_detail.last_update_date
			
           -- ||employee_action_detail.last_transfer_eff_date
		   ||coalesce(trim(cast(employee_action_detail.last_transfer_eff_date as STRING )),'')
		   
            ||coalesce(trim(cast(employee_action_detail.reason_desc  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.to_payment_frequency as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.to_auxiliary_status_code as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.to_dept_code  as STRING) ),'')
            ||coalesce(trim(cast(employee_action_detail.to_employee_pay_rate_amt as STRING) ),'')
            ||coalesce(trim(cast(employee_action_detail.to_employee_schedule_code  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.to_job_code  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.to_location_code as STRING)), '')
            ||coalesce(trim(cast(employee_action_detail.to_overtime_plan_code  as STRING) ),'')
            ||coalesce(trim(cast(employee_action_detail.to_position_code  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.to_position_level_num as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.to_process_level_code  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.to_pay_grade_code  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.to_pay_step_num  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.to_salary_class_code  as STRING) ),'')
            ||coalesce(trim(cast(employee_action_detail.to_schedule_code  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.to_standard_hour  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.to_supervisor_code  as STRING) ),'')
            ||coalesce(trim(cast(employee_action_detail.to_union_code  as STRING)),'')
            ||coalesce(trim(cast(employee_action_detail.to_user_level  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.to_exemption_flag  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.to_expense_account_unit  as STRING )),'')
            ||coalesce(trim(cast(employee_action_detail.to_expense_sub_account_num as STRING) ),'')
            ||coalesce(trim(cast(employee_action_detail.transfer_termination_flag  as STRING) ),'')
            ||coalesce(trim(cast(employee_action_detail.to_expense_company_num as STRING) ),'')
             ||employee_action_detail.lawson_company_num
             ||employee_action_detail.process_level_code
             ||employee_action_detail.source_system_code
          FROM
            {{ params.param_hr_base_views_dataset_name }}.employee_action_detail
          WHERE DATE(employee_action_detail.valid_to_date) = DATE("9999-12-31"));

  UPDATE {{ params.param_hr_core_dataset_name }}.employee_action_detail AS tgt SET 
  valid_to_date = current_ts - INTERVAL 1 SECOND, active_dw_ind = 'N', dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) WHERE 
  (tgt.employee_action_sid||
  tgt.action_code|| tgt.employee_sid|| 
  tgt.action_sequence_num|| tgt.eff_from_date|| 
  tgt.lawson_company_num|| tgt.employee_num|| 
  tgt.source_system_code) NOT IN(
    SELECT 
        employee_action_detail_wrk_6_inc.employee_action_sid||
        employee_action_detail_wrk_6_inc.action_code||
        employee_action_detail_wrk_6_inc.employee_sid||
        employee_action_detail_wrk_6_inc.action_sequence_num||
        employee_action_detail_wrk_6_inc.eff_from_date||
        employee_action_detail_wrk_6_inc.lawson_company_num||
        employee_action_detail_wrk_6_inc.employee_num||
        employee_action_detail_wrk_6_inc.source_system_code
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_6_inc
  )
   AND DATE(tgt.valid_to_date) = DATE("9999-12-31");

/*  UPDATE  DELETE_IND  */

  UPDATE {{ params.param_hr_core_dataset_name }}.employee_action_detail AS empl SET delete_ind = 'D',
  valid_to_date = current_ts - INTERVAL 1 SECOND, active_dw_ind = 'N', dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) WHERE upper(empl.delete_ind) = 'A'
  -- AND empl.source_system_code = 'L'
 --  AND empl.lawson_company_num <> 300
   AND (empl.lawson_company_num||empl.employee_num) NOT IN(
    SELECT 
        employee_action_detail_wrk_6_inc.lawson_company_num||
        employee_action_detail_wrk_6_inc.employee_num
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_6_inc
      GROUP BY 1
  );

  UPDATE {{ params.param_hr_core_dataset_name }}.employee_action_detail AS empl 
  SET delete_ind = 'A', dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) WHERE upper(empl.delete_ind) = 'D'
 -- AND empl.source_system_code = 'L'
 --  AND empl.lawson_company_num <> 300
   AND (empl.lawson_company_num||empl.employee_num) IN(
    SELECT 
        employee_action_detail_wrk_6_inc.lawson_company_num||
        employee_action_detail_wrk_6_inc.employee_num
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk_6_inc
      GROUP BY 1
  );
END;