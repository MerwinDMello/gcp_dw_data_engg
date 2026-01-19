BEGIN
DECLARE DUP_COUNT INT64;
  DECLARE current_ts datetime;
  SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);


/*CALL {{ params.param_hr_core_dataset_name }}.sk_gen({{ params.param_hr_stage_dataset_name }},'reporting_xfer_termn',"CMPY||'-'||EMPL", 'Employee');*/
CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}','reporting_xfer_termn',"CMPY||'-'||EMPL", 'Employee');

/*  Truncate Work Table     */

TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk;

BEGIN TRANSACTION ;
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk (employee_num, employee_action_sid, eff_from_date, action_code, employee_sid, action_sequence_num, action_from_date, action_to_date, from_auxiliary_status_code, from_dept_code, from_employee_pay_rate_amount, from_employee_work_schedule_code, from_job_code, from_location_code, from_overtime_plan_code, from_position_code, from_position_level_num, from_process_level_code, from_payment_frequency, from_payment_grade_code, from_payment_step_num, from_salary_class_code, from_schedule_code, from_standard_hour, from_supervisor_code, from_union_code, from_user_level, from_excemption_flag, from_expense_account_unit, from_expense_company, from_expense_sub_account_num, hr_code_desc, last_update_date, last_transfer_eff_date, reason_desc, to_payment_frequency, to_auxiliary_status_code, to_dept_code, to_employee_pay_rate_amt, to_employee_schedule_code, to_job_code, to_location_code, to_overtime_plan_code, to_position_code, to_position_level_num, to_process_level_code, to_pay_grade_code, to_pay_step_num, to_salary_class_code, to_schedule_code, to_standard_hour, to_supervisor_code, to_union_code, to_user_level, to_exemption_flag, to_expense_account_unit, to_expense_sub_account_num, transfer_termination_flag, to_expense_company_num, active_dw_ind, lawson_company_num, process_level_code, security_key_text, source_system_code, dw_last_update_date_time, valid_from_date, valid_to_date)
    SELECT
        -- ,Eff_To_Date
        rxt.empl,
        empact.person_action_sid AS employee_action_sid,
        rxt.eff_dt AS eff_from_date,
        rxt.actn_cd AS action_code,
        cast(lkp_emp_sid.sk as INT64) AS employee_sid,
        rxt.actn_num AS action_sequence_num,
        rxt.eff_dt AS action_from_date,
        rxt.dw_lst_updt_dt AS action_to_date,
        rxt.from_aux_stts AS from_auxiliary_status_code,
        rxt.from_dept AS from_dept_code,
        rxt.from_empl_py_rt AS from_employee_pay_rate_amount,
        rxt.from_empl_schd AS from_employee_work_schedule_code,
        rxt.from_job_cd AS from_job_code,
        rxt.from_loc_cd AS from_location_code,
        rxt.from_ot_pln_cd AS from_overtime_plan_code,
        rxt.from_posn AS from_position_code,
        rxt.from_posn_lvl AS from_position_level_num,
        rxt.from_prcs_lvl AS from_process_level_code,
        rxt.from_py_freq AS from_payment_frequency,
        rxt.from_py_grd AS from_payment_grade_code,
        rxt.from_py_stp AS from_payment_step_num,
        rxt.from_sal_cl AS from_salary_class_code,
        rxt.from_sched AS from_schedule_code,
        rxt.from_std_hr AS from_standard_hour,
        rxt.from_supv AS from_supervisor_code,
        rxt.from_unon_cd AS from_union_code,
        rxt.from_user_lvl AS from_user_level,
        rxt.from_xmpt_flg AS from_excemption_flag,
        rxt.from_xpns_acct_unit AS from_expense_account_unit,
        rxt.from_xpns_cmpy AS from_expense_company,
        rxt.from_xpns_sub_acct AS from_expense_sub_account_num,
        rxt.hr_cd_desc AS hr_code_desc,
        rxt.lst_updt_dt AS last_update_date,
        rxt.lst_xfer_eff_dt AS last_transfer_eff_date,
        rxt.resn_1 AS reason_desc,
        rxt.t0_py_freq AS to_payment_frequency,
        rxt.to_aux_stts AS to_auxiliary_status_code,
        rxt.to_dept AS to_dept_code,
        rxt.to_empl_py_rt AS to_employee_pay_rate_amt,
        rxt.to_empl_schd AS to_employee_schedule_code,
        rxt.to_job_cd AS to_job_code,
        rxt.to_loc_cd AS to_location_code,
        rxt.to_ot_pln_cd AS to_overtime_plan_code,
        rxt.to_posn AS to_position_code,
        rxt.to_posn_lvl AS to_position_level_num,
        rxt.to_prcs_lvl AS to_process_level_code,
        rxt.to_py_grd AS to_pay_grade_code,
        rxt.to_py_stp AS to_pay_step_num,
        rxt.to_sal_cl AS to_salary_class_code,
        rxt.to_schd AS to_schedule_code,
        rxt.to_std_hr AS to_standard_hour,
        rxt.to_supv AS to_supervisor_code,
        rxt.to_unon_cd AS to_union_code,
        rxt.to_user_lvl AS to_user_level,
        rxt.to_xmpt_flg AS to_exemption_flag,
        rxt.to_xpns_acct_unit AS to_expense_account_unit,
        rxt.to_xpns_sub_acct AS to_expense_sub_account_num,
        rxt.xfer_termn_flg AS transfer_termination_flag,
        rxt.to_xpns_cmpy AS to_expense_company_num,
        -- ,DATE '9999-12-31' Eff_To_Date
        'Y' AS active_dw_ind,
        rxt.cmpy AS lawson_company_num,
        CASE
          WHEN trim(rxt.to_prcs_lvl) IS NULL THEN '00000'
          ELSE trim(rxt.to_prcs_lvl)
        END AS process_level_code,
        -- ,CASE WHEN TRIM(RXT.CMPY) IS NULL THEN '00000' ELSE TRIM(RXT.CMPY) END||'-'||CASE WHEN TRIM(RXT.To_Prcs_Lvl) IS NULL THEN '00000' ELSE TRIM(RXT.To_Prcs_Lvl) END||'-'||CASE WHEN TRIM(RXT.To_Dept) IS NULL THEN '00000' ELSE TRIM(RXT.To_Dept) END AS Security_Key_Text
        concat(CASE
          WHEN rxt.cmpy IS NULL THEN '00000'
          ELSE concat(substr('00000', 1, 5 - rxt.cmpy), rxt.cmpy)
        END, '-', CASE
          WHEN trim(rxt.to_prcs_lvl) IS NULL
           OR trim(rxt.to_prcs_lvl) = '' THEN '00000'
          ELSE concat(substr('00000', 1, 5 - length(trim(rxt.to_prcs_lvl))), trim(rxt.to_prcs_lvl))
        END, '-', CASE
          WHEN trim(rxt.to_dept) IS NULL
           OR trim(rxt.to_dept) = '' THEN '00000'
          ELSE concat(substr('00000', 1, 5 - length(trim(rxt.to_dept))), trim(rxt.to_dept))
        END) AS security_key_text,
		
        'L' AS source_system_code,
        current_ts AS dw_last_update_date_time,
        -- ,CURRENT_DATE AS Valid_from_date
        /*date_sub(date(current_ts) interval 1 DAY) AS valid_from_date,
        DATE '9999-12-31' AS valid_to_date*/
		current_ts AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date
		 FROM
        {{ params.param_hr_stage_dataset_name }}.reporting_xfer_termn AS rxt
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS lkp_emp_sid 
		ON substr(rxt.cmpy|| '-'||rxt.empl, 1, 255 )= lkp_emp_sid.sk_source_txt
         AND upper(lkp_emp_sid.sk_type) = 'EMPLOYEE'
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.person_action AS empact ON lkp_emp_sid.sk = empact.employee_sid
         AND upper(lkp_emp_sid.sk_type) = 'EMPLOYEE'
         AND rxt.actn_cd = empact.action_code
         AND rxt.actn_num = empact.action_sequence_num
         AND rxt.eff_dt = empact.action_from_date
         AND empact.valid_to_date = DATETIME("9999-12-31 23:59:59") 
      WHERE empact.person_action_sid IS NOT NULL
  ;



  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk (employee_num, employee_action_sid, eff_from_date, action_code, employee_sid, action_sequence_num, action_from_date, action_to_date, from_auxiliary_status_code, from_dept_code, from_employee_pay_rate_amount, from_employee_work_schedule_code, from_job_code, from_location_code, from_overtime_plan_code, from_position_code, from_position_level_num, from_process_level_code, from_payment_frequency, from_payment_grade_code, from_payment_step_num, from_salary_class_code, from_schedule_code, from_standard_hour, from_supervisor_code, from_union_code, from_user_level, from_excemption_flag, from_expense_account_unit, from_expense_company, from_expense_sub_account_num, hr_code_desc, last_update_date, last_transfer_eff_date, reason_desc, to_payment_frequency, to_auxiliary_status_code, to_dept_code, to_employee_pay_rate_amt, to_employee_schedule_code, to_job_code, to_location_code, to_overtime_plan_code, to_position_code, to_position_level_num, to_process_level_code, to_pay_grade_code, to_pay_step_num, to_salary_class_code, to_schedule_code, to_standard_hour, to_supervisor_code, to_union_code, to_user_level, to_exemption_flag, to_expense_account_unit, to_expense_sub_account_num, transfer_termination_flag, to_expense_company_num, active_dw_ind, lawson_company_num, process_level_code, security_key_text, source_system_code, dw_last_update_date_time, valid_from_date, valid_to_date)
    SELECT
        -- ,Eff_To_Date
        y.employee_num,
        y.employee_action_sid,
        y.eff_from_date,
        y.action_code,
        cast (y.employee_sid as INT64),
        y.action_sequence_num,
        y.action_from_date,
        y.action_to_date,
        y.from_auxiliary_status_code,
        y.from_dept_code,
        y.from_employee_pay_rate_amount,
        y.from_employee_work_schedule_code,
        y.from_job_code,
        y.from_location_code,
        y.from_overtime_plan_code,
        y.from_position_code,
        y.from_position_level_num,
        y.from_process_level_code,
        y.from_payment_frequency,
        y.from_payment_grade_code,
        y.from_payment_step_num,
        y.from_salary_class_code,
        y.from_schedule_code,
        y.from_standard_hour,
        y.from_supervisor_code,
        y.from_union_code,
        y.from_user_level,
        y.from_excemption_flag,
        y.from_expense_account_unit,
        y.from_expense_company,
        y.from_expense_sub_account_num,
        y.hr_code_desc,
        y.last_update_date,
        y.last_transfer_eff_date,
        y.reason_desc,
        y.to_payment_frequency,
        y.to_auxiliary_status_code,
        y.to_dept_code,
        y.to_employee_pay_rate_amt,
        y.to_employee_schedule_code,
        y.to_job_code,
        y.to_location_code,
        y.to_overtime_plan_code,
        y.to_position_code,
        y.to_position_level_num,
        y.to_process_level_code,
        y.to_pay_grade_code,
        y.to_pay_step_num,
        y.to_salary_class_code,
        y.to_schedule_code,
        y.to_standard_hour,
        y.to_supervisor_code,
        y.to_union_code,
        y.to_user_level,
        y.to_exemption_flag,
        y.to_expense_account_unit,
        y.to_expense_sub_account_num,
        y.transfer_termination_flag,
        y.to_expense_company_num,
        -- ,Eff_To_Date
        y.active_dw_ind,
        y.lawson_company_num,
        y.process_level_code,
        y.security_key_text,
        y.source_system_code,
        y.dw_last_update_date_time,
        current_ts AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date
      FROM
        (
          SELECT
              x.employee_num,
              -99999999 + row_number() OVER (ORDER BY x.employee_sid, x.action_code, x.action_sequence_num, x.action_from_date) AS employee_action_sid,
              x.eff_from_date,
              x.action_code,
              x.employee_sid,
              x.action_sequence_num,
              x.action_from_date,
              x.action_to_date,
              x.from_auxiliary_status_code,
              x.from_dept_code,
              x.from_employee_pay_rate_amount,
              x.from_employee_work_schedule_code,
              x.from_job_code,
              x.from_location_code,
              x.from_overtime_plan_code,
              x.from_position_code,
              x.from_position_level_num,
              x.from_process_level_code,
              x.from_payment_frequency,
              x.from_payment_grade_code,
              x.from_payment_step_num,
              x.from_salary_class_code,
              x.from_schedule_code,
              x.from_standard_hour,
              x.from_supervisor_code,
              x.from_union_code,
              x.from_user_level,
              x.from_excemption_flag,
              x.from_expense_account_unit,
              x.from_expense_company,
              x.from_expense_sub_account_num,
              x.hr_code_desc,
              x.last_update_date,
              x.last_transfer_eff_date,
              x.reason_desc,
              x.to_payment_frequency,
              x.to_auxiliary_status_code,
              x.to_dept_code,
              x.to_employee_pay_rate_amt,
              x.to_employee_schedule_code,
              x.to_job_code,
              x.to_location_code,
              x.to_overtime_plan_code,
              x.to_position_code,
              x.to_position_level_num,
              x.to_process_level_code,
              x.to_pay_grade_code,
              x.to_pay_step_num,
              x.to_salary_class_code,
              x.to_schedule_code,
              x.to_standard_hour,
              x.to_supervisor_code,
              x.to_union_code,
              x.to_user_level,
              x.to_exemption_flag,
              x.to_expense_account_unit,
              x.to_expense_sub_account_num,
              x.transfer_termination_flag,
              x.to_expense_company_num,
              -- ,Eff_To_Date
              x.active_dw_ind,
              x.lawson_company_num,
              x.process_level_code,
              x.security_key_text,
              x.source_system_code,
              x.dw_last_update_date_time,
              row_number() OVER (ORDER BY x.employee_sid, x.action_code, x.action_sequence_num, x.action_from_date) AS row_id,
              x.valid_from_date,
              x.valid_to_date
            FROM
              (
                SELECT
                    rxt.empl AS employee_num,
                    rxt.eff_dt AS eff_from_date,
                    rxt.actn_cd AS action_code,
                    lkp_emp_sid.sk AS employee_sid,
                    rxt.actn_num AS action_sequence_num,
                    rxt.eff_dt AS action_from_date,
                    rxt.dw_lst_updt_dt AS action_to_date,
                    rxt.from_aux_stts AS from_auxiliary_status_code,
                    rxt.from_dept AS from_dept_code,
                    rxt.from_empl_py_rt AS from_employee_pay_rate_amount,
                    rxt.from_empl_schd AS from_employee_work_schedule_code,
                    rxt.from_job_cd AS from_job_code,
                    rxt.from_loc_cd AS from_location_code,
                    rxt.from_ot_pln_cd AS from_overtime_plan_code,
                    rxt.from_posn AS from_position_code,
                    rxt.from_posn_lvl AS from_position_level_num,
                    rxt.from_prcs_lvl AS from_process_level_code,
                    rxt.from_py_freq AS from_payment_frequency,
                    rxt.from_py_grd AS from_payment_grade_code,
                    rxt.from_py_stp AS from_payment_step_num,
                    rxt.from_sal_cl AS from_salary_class_code,
                    rxt.from_sched AS from_schedule_code,
                    rxt.from_std_hr AS from_standard_hour,
                    rxt.from_supv AS from_supervisor_code,
                    rxt.from_unon_cd AS from_union_code,
                    rxt.from_user_lvl AS from_user_level,
                    rxt.from_xmpt_flg AS from_excemption_flag,
                    rxt.from_xpns_acct_unit AS from_expense_account_unit,
                    rxt.from_xpns_cmpy AS from_expense_company,
                    rxt.from_xpns_sub_acct AS from_expense_sub_account_num,
                    rxt.hr_cd_desc AS hr_code_desc,
                    rxt.lst_updt_dt AS last_update_date,
                    rxt.lst_xfer_eff_dt AS last_transfer_eff_date,
                    rxt.resn_1 AS reason_desc,
                    rxt.t0_py_freq AS to_payment_frequency,
                    rxt.to_aux_stts AS to_auxiliary_status_code,
                    rxt.to_dept AS to_dept_code,
                    rxt.to_empl_py_rt AS to_employee_pay_rate_amt,
                    rxt.to_empl_schd AS to_employee_schedule_code,
                    rxt.to_job_cd AS to_job_code,
                    rxt.to_loc_cd AS to_location_code,
                    rxt.to_ot_pln_cd AS to_overtime_plan_code,
                    rxt.to_posn AS to_position_code,
                    rxt.to_posn_lvl AS to_position_level_num,
                    rxt.to_prcs_lvl AS to_process_level_code,
                    rxt.to_py_grd AS to_pay_grade_code,
                    rxt.to_py_stp AS to_pay_step_num,
                    rxt.to_sal_cl AS to_salary_class_code,
                    rxt.to_schd AS to_schedule_code,
                    rxt.to_std_hr AS to_standard_hour,
                    rxt.to_supv AS to_supervisor_code,
                    rxt.to_unon_cd AS to_union_code,
                    rxt.to_user_lvl AS to_user_level,
                    rxt.to_xmpt_flg AS to_exemption_flag,
                    rxt.to_xpns_acct_unit AS to_expense_account_unit,
                    rxt.to_xpns_sub_acct AS to_expense_sub_account_num,
                    rxt.xfer_termn_flg AS transfer_termination_flag,
                    rxt.to_xpns_cmpy AS to_expense_company_num,
                    -- ,DATE '9999-12-31' Eff_To_Date
                    'Y' AS active_dw_ind,
                    rxt.cmpy AS lawson_company_num,
                    CASE
                      WHEN trim(rxt.to_prcs_lvl) IS NULL THEN '00000'
                      ELSE trim(rxt.to_prcs_lvl)
                    END AS process_level_code,
                    /*concat(CASE
                      WHEN rxt.cmpy IS NULL THEN '00000'
                      ELSE rxt.cmpy
                    END, '-', CASE
                      WHEN trim(rxt.to_prcs_lvl) IS NULL THEN '00000'
                      ELSE trim(rxt.to_prcs_lvl)
                    END, '-', CASE
                      WHEN trim(rxt.to_dept) IS NULL THEN '00000'
                      ELSE trim(rxt.to_dept)
                    END) AS security_key_text,*/
					
	case when rxt.cmpy is null then '00000' else cast (rxt.cmpy as string )end
	||'-'||case when trim(rxt.to_prcs_lvl) is null then '00000' else trim(rxt.to_prcs_lvl) end
	||'-'||case when trim(rxt.to_dept) is null then '00000' else trim(rxt.to_dept) end as security_key_text,
	
                    'L' AS source_system_code,
                    current_ts AS dw_last_update_date_time,
                    current_ts AS valid_from_date,
                    DATETIME("9999-12-31 23:59:59") AS valid_to_date
                  FROM
                    {{ params.param_hr_stage_dataset_name }}.reporting_xfer_termn AS rxt
                    INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS lkp_emp_sid ON
					substr(rxt.cmpy|| '-'||rxt.empl, 1, 255) = lkp_emp_sid.sk_source_txt
                     AND upper(lkp_emp_sid.sk_type) = 'EMPLOYEE'
                    LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.person_action AS empact ON lkp_emp_sid.sk = empact.employee_sid
                     AND upper(lkp_emp_sid.sk_type) = 'EMPLOYEE'
                     AND rxt.actn_cd = empact.action_code
                     AND rxt.actn_num = empact.action_sequence_num
                     AND rxt.eff_dt = empact.action_from_date
                     AND empact.valid_to_date = DATETIME("9999-12-31 23:59:59")
                  WHERE empact.person_action_sid IS NULL
              ) AS x
        ) AS y
  ;


  INSERT INTO {{ params.param_hr_core_dataset_name }}.employee_action_detail (employee_num, employee_action_sid, eff_from_date, action_code, employee_sid, action_sequence_num, action_from_date, action_to_date, from_auxiliary_status_code, from_dept_code, from_employee_pay_rate_amount, from_employee_work_schedule_code, from_job_code, from_location_code, from_overtime_plan_code, from_position_code, from_position_level_num, from_process_level_code, from_payment_frequency, from_payment_grade_code, from_payment_step_num, from_salary_class_code, from_schedule_code, from_standard_hour, from_supervisor_code, from_union_code, from_user_level, from_excemption_flag, from_expense_account_unit, from_expense_company, from_expense_sub_account_num, hr_code_desc, last_update_date, last_transfer_eff_date, reason_desc, to_payment_frequency, to_auxiliary_status_code, to_dept_code, to_employee_pay_rate_amt, to_employee_schedule_code, to_job_code, to_location_code, to_overtime_plan_code, to_position_code, to_position_level_num, to_process_level_code, to_pay_grade_code, to_pay_step_num, to_salary_class_code, to_schedule_code, to_standard_hour, to_supervisor_code, to_union_code, to_user_level, to_exemption_flag, to_expense_account_unit, to_expense_sub_account_num, transfer_termination_flag, to_expense_company_num, active_dw_ind, lawson_company_num, process_level_code, delete_ind, security_key_text, source_system_code, dw_last_update_date_time, valid_from_date, valid_to_date)
    SELECT
        -- ,Eff_To_Date
        employee_action_detail_wrk.employee_num,
        --  PERSON_ACTION_SID  as Employee_Action_SID
        employee_action_detail_wrk.employee_action_sid,
        employee_action_detail_wrk.eff_from_date,
        employee_action_detail_wrk.action_code,
        employee_action_detail_wrk.employee_sid,
        employee_action_detail_wrk.action_sequence_num,
        employee_action_detail_wrk.action_from_date,
        employee_action_detail_wrk.action_to_date,
        employee_action_detail_wrk.from_auxiliary_status_code,
        employee_action_detail_wrk.from_dept_code,
        employee_action_detail_wrk.from_employee_pay_rate_amount,
        employee_action_detail_wrk.from_employee_work_schedule_code,
        employee_action_detail_wrk.from_job_code,
        employee_action_detail_wrk.from_location_code,
        employee_action_detail_wrk.from_overtime_plan_code,
        employee_action_detail_wrk.from_position_code,
        employee_action_detail_wrk.from_position_level_num,
        employee_action_detail_wrk.from_process_level_code,
        employee_action_detail_wrk.from_payment_frequency,
        employee_action_detail_wrk.from_payment_grade_code,
        employee_action_detail_wrk.from_payment_step_num,
        employee_action_detail_wrk.from_salary_class_code,
        employee_action_detail_wrk.from_schedule_code,
        employee_action_detail_wrk.from_standard_hour,
        employee_action_detail_wrk.from_supervisor_code,
        employee_action_detail_wrk.from_union_code,
        employee_action_detail_wrk.from_user_level,
        employee_action_detail_wrk.from_excemption_flag,
        employee_action_detail_wrk.from_expense_account_unit,
        employee_action_detail_wrk.from_expense_company,
        employee_action_detail_wrk.from_expense_sub_account_num,
        employee_action_detail_wrk.hr_code_desc,
        employee_action_detail_wrk.last_update_date,
        employee_action_detail_wrk.last_transfer_eff_date,
        employee_action_detail_wrk.reason_desc,
        employee_action_detail_wrk.to_payment_frequency,
        employee_action_detail_wrk.to_auxiliary_status_code,
        employee_action_detail_wrk.to_dept_code,
        employee_action_detail_wrk.to_employee_pay_rate_amt,
        employee_action_detail_wrk.to_employee_schedule_code,
        employee_action_detail_wrk.to_job_code,
        employee_action_detail_wrk.to_location_code,
        employee_action_detail_wrk.to_overtime_plan_code,
        employee_action_detail_wrk.to_position_code,
        employee_action_detail_wrk.to_position_level_num,
        employee_action_detail_wrk.to_process_level_code,
        employee_action_detail_wrk.to_pay_grade_code,
        employee_action_detail_wrk.to_pay_step_num,
        employee_action_detail_wrk.to_salary_class_code,
        employee_action_detail_wrk.to_schedule_code,
        employee_action_detail_wrk.to_standard_hour,
        employee_action_detail_wrk.to_supervisor_code,
        employee_action_detail_wrk.to_union_code,
        employee_action_detail_wrk.to_user_level,
        employee_action_detail_wrk.to_exemption_flag,
        employee_action_detail_wrk.to_expense_account_unit,
        employee_action_detail_wrk.to_expense_sub_account_num,
        employee_action_detail_wrk.transfer_termination_flag,
        employee_action_detail_wrk.to_expense_company_num,
        -- ,Eff_To_Date
        employee_action_detail_wrk.active_dw_ind,
        employee_action_detail_wrk.lawson_company_num,
        employee_action_detail_wrk.process_level_code,
        'A' AS delete_ind,
        employee_action_detail_wrk.security_key_text,
        employee_action_detail_wrk.source_system_code,
        current_ts AS dw_last_update_date_time,
          current_ts AS valid_from_date,
          DATETIME("9999-12-31 23:59:59") AS valid_to_date
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_action_detail_wrk
  ;
/* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select Employee_Action_SID ,Eff_From_Date ,Valid_From_Date
        from {{ params.param_hr_core_dataset_name }}.employee_action_detail
        group by Employee_Action_SID ,Eff_From_Date ,Valid_From_Date		
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.employee_Action_Detail');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END ;


