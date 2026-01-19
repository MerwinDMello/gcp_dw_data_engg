BEGIN
DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;
/*  Truncate Work Table     */
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.emp_disciplinary_action_wrk;

/*  Insert the records into the Work Table  */
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.emp_disciplinary_action_wrk (employee_sid, disciplinary_ind, disciplinary_action_num, valid_from_date, valid_to_date, disciplinary_desc, creation_date, action_category_code, report_date, reported_by_employee_num, reported_by_name, action_status_code, action_outcome_desc, action_outcome_date, days_out_cnt, department_sid, location_code, job_code_sid, comment_desc, supervisor_employee_num, last_update_date, last_update_user_34_login_code, employee_num, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time)
    SELECT
        coalesce(emp.employee_sid, 0),
        CASE
          WHEN upper(pa.pagrdistep_type) = '' THEN '0'
          ELSE trim(pa.pagrdistep_type)
        END AS disciplinary_ind,
        coalesce(pa.griev_dis_nbr,0),
        current_ts,
        DATETIME("9999-12-31 23:59:59"),
        trim(pa.description),
        pa.r_date,
        trim(pa.griev_dis_cat),
        pa.report_date,
        pa.reports_by,
        trim(pa.reports_by_name),
        trim(pa.griev_dis_stat),
        trim(pa.outcome),
        pa.outcome_date,
        pa.days_out,
        coalesce(dept.dept_sid, 0),
        trim(pa.locat_code),
        coalesce(j_code.job_code_sid, 0),
        trim(pa.r_comment),
        pa.supervisor_emp,
        pa.date_stamp,
        substring(pa.user_id,1,7),
        pa.employee,
        pa.company,
        concat(substr('00000', 1, 5 - length(trim(pa.process_level))), trim(pa.process_level)),
        'L' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.pagrdi AS pa
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emp ON pa.company = emp.lawson_company_num
         AND pa.employee = emp.employee_num
         AND date(emp.valid_to_date) = "9999-12-31"
         AND upper(emp.source_system_code) = 'L'
         AND upper(EMP.active_dw_ind) = 'Y'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON trim(pa.department) = dept.dept_code
         AND pa.company = dept.lawson_company_num
         AND concat(substr('00000', 1, 5 - length(trim(pa.process_level))), trim(pa.process_level)) = dept.process_level_code
         AND date(dept.valid_to_date) = "9999-12-31"
         AND upper(dept.source_system_code) = 'L'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS j_code ON trim(pa.job_code) = j_code.job_code
         AND pa.company = j_code.lawson_company_num
         AND date(j_code.valid_to_date) = "9999-12-31"
         AND upper(j_code.source_system_code) = 'L';
    
/*  Close the previous records from Target table for same key for any Changes  */
/*  Insert the New Records/Chnages into the Target Table  */
/*UPDATE VLD_TO_DATE from records that are not presnt in Staging table*/
UPDATE {{ params.param_hr_core_dataset_name }}.employee_disciplinary_action AS tgt SET valid_to_date = current_ts - INTERVAL 1 SECOND WHERE date(tgt.valid_to_date) = "9999-12-31"
   AND (tgt.employee_sid, tgt.disciplinary_ind, tgt.disciplinary_action_num) NOT IN(
    SELECT AS STRUCT
        emp_disciplinary_action_wrk.employee_sid,
        coalesce(emp_disciplinary_action_wrk.disciplinary_ind, ''),
        coalesce(emp_disciplinary_action_wrk.disciplinary_action_num, 0)
      FROM
        {{ params.param_hr_stage_dataset_name }}.emp_disciplinary_action_wrk
  ) and tgt.source_system_code = 'L';

  UPDATE {{ params.param_hr_core_dataset_name }}.employee_disciplinary_action AS tgt SET valid_to_date = current_ts - INTERVAL 1 SECOND, dw_last_update_date_time = current_ts FROM {{ params.param_hr_stage_dataset_name }}.emp_disciplinary_action_wrk AS stg WHERE tgt.employee_sid = stg.employee_sid
   AND date(tgt.valid_to_date) = "9999-12-31"
   AND tgt.source_system_code = stg.source_system_code
   AND coalesce(trim(tgt.disciplinary_ind), '') = coalesce(trim(stg.disciplinary_ind), '')
   AND coalesce(tgt.disciplinary_action_num, 0) = coalesce(stg.disciplinary_action_num,0)
   AND ( upper(coalesce(stg.disciplinary_desc, '')) <> upper(coalesce(tgt.disciplinary_desc, ''))
   OR coalesce(stg.report_date, DATE '1900-01-01') <> coalesce(tgt.report_date, DATE '1900-01-01')
   OR coalesce(stg.reported_by_employee_num,0) <> coalesce(tgt.reported_by_employee_num,0)
   OR upper(coalesce(trim(stg.reported_by_name), '')) <> upper(coalesce(trim(tgt.reported_by_name), ''))
   OR upper(coalesce(trim(stg.action_status_code), '')) <> upper(coalesce(trim(tgt.action_status_code), ''))
   OR upper(coalesce(trim(stg.action_outcome_desc), '')) <> upper(coalesce(trim(tgt.action_outcome_desc), ''))
   OR coalesce(stg.action_outcome_date, DATE '1900-01-01') <> coalesce(tgt.action_outcome_date, DATE '1900-01-01')
   OR coalesce(stg.days_out_cnt, 0) <> coalesce(tgt.days_out_cnt,0)
   OR coalesce(stg.department_sid, 0) <> coalesce(tgt.department_sid, 0)
   OR upper(coalesce(trim(stg.location_code), '')) <> upper(coalesce(trim(tgt.location_code), ''))
   OR coalesce(stg.job_code_sid,0) <> coalesce(tgt.job_code_sid,0)
   OR upper(coalesce(trim(stg.comment_desc), '')) <> upper(coalesce(trim(tgt.comment_desc), ''))
   OR coalesce(stg.supervisor_employee_num,0) <> coalesce(tgt.supervisor_employee_num,0)
   OR coalesce(stg.last_update_date, DATE '1900-01-01') <> coalesce(tgt.last_update_date, DATE '1900-01-01')
   OR upper(coalesce(trim(stg.last_update_user_34_login_code), '')) <> upper(coalesce(trim(tgt.last_update_user_34_login_code), ''))
   OR coalesce(stg.employee_num,0) <> coalesce(tgt.employee_num,0)
   OR coalesce(stg.lawson_company_num,0) <> coalesce(tgt.lawson_company_num,0)
   OR upper(coalesce(trim(stg.process_level_code),'')) <> upper(coalesce(trim(tgt.process_level_code), '')))
   and tgt.source_system_code = 'L';

  INSERT INTO {{ params.param_hr_core_dataset_name }}.employee_disciplinary_action (employee_sid, disciplinary_ind, disciplinary_action_num, valid_from_date, valid_to_date, disciplinary_desc, creation_date, action_category_code, report_date, reported_by_employee_num, reported_by_name, action_status_code, action_outcome_desc, action_outcome_date, days_out_cnt, department_sid, location_code, job_code_sid, comment_desc, supervisor_employee_num, last_update_date, last_update_user_34_login_code, employee_num, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time)
    SELECT
        emp_disciplinary_action_wrk.employee_sid,
        emp_disciplinary_action_wrk.disciplinary_ind,
        emp_disciplinary_action_wrk.disciplinary_action_num,
        current_ts,
        emp_disciplinary_action_wrk.valid_to_date,
        emp_disciplinary_action_wrk.disciplinary_desc,
        emp_disciplinary_action_wrk.creation_date,
        emp_disciplinary_action_wrk.action_category_code,
        emp_disciplinary_action_wrk.report_date,
        emp_disciplinary_action_wrk.reported_by_employee_num,
        emp_disciplinary_action_wrk.reported_by_name,
        emp_disciplinary_action_wrk.action_status_code,
        emp_disciplinary_action_wrk.action_outcome_desc,
        emp_disciplinary_action_wrk.action_outcome_date,
        emp_disciplinary_action_wrk.days_out_cnt,
        emp_disciplinary_action_wrk.department_sid,
        emp_disciplinary_action_wrk.location_code,
        emp_disciplinary_action_wrk.job_code_sid,
        emp_disciplinary_action_wrk.comment_desc,
        emp_disciplinary_action_wrk.supervisor_employee_num,
        emp_disciplinary_action_wrk.last_update_date,
        emp_disciplinary_action_wrk.last_update_user_34_login_code,
        emp_disciplinary_action_wrk.employee_num,
        emp_disciplinary_action_wrk.lawson_company_num,
        emp_disciplinary_action_wrk.process_level_code,
        emp_disciplinary_action_wrk.source_system_code,
        current_ts
      FROM
        {{ params.param_hr_stage_dataset_name }}.emp_disciplinary_action_wrk
      WHERE 
      emp_disciplinary_action_wrk.source_system_code = 'L'
      AND
      (emp_disciplinary_action_wrk.employee_sid, emp_disciplinary_action_wrk.disciplinary_ind, emp_disciplinary_action_wrk.disciplinary_action_num) NOT IN(
        SELECT AS STRUCT
            employee_disciplinary_action.employee_sid,
            employee_disciplinary_action.disciplinary_ind,
            employee_disciplinary_action.disciplinary_action_num
          FROM
            {{ params.param_hr_base_views_dataset_name }}.employee_disciplinary_action
          WHERE date(employee_disciplinary_action.valid_to_date) = "9999-12-31"
          and employee_disciplinary_action.source_system_code = 'L'
      )
  ;
END;