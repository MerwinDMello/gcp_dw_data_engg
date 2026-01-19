BEGIN
DECLARE current_ts datetime;
  SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;
    /*  Generate the surrogate keys for Employee */
    CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'znreqhist', "Company||'-'||Employee", 'Employee');


    CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'znreqhist', "Company||'-'||Requisition", 'Requisition');  
    /*  Truncate Worktable Table     */
    TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_requisition_wrk;

    /*  Load Work Table with working Data */
    INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_requisition_wrk (employee_sid, requisition_sid, action_type_code, eff_from_date, action_code, user_id_text, work_unit_num, lawson_company_num, process_level_code, requisition_num, employee_num, delete_ind, valid_from_date, source_system_code, dw_last_update_date_time)
    SELECT
        coalesce(emp.employee_sid, cast(xwlk.sk as INT64)) AS employee_sid,
        coalesce(rec.requisition_sid, cast(lkp_req_sid.sk as INT64)) AS requisition_sid,
        stg.action_type AS action_type_code,
        stg.effective_date AS eff_from_date,
        trim(stg.action_code) AS action_code,
        trim(stg.user_id) AS user_id_text,
        stg.wu_nbr AS work_unit_num,
        stg.company AS lawson_company_num,
        '00000' AS process_level_code,
        stg.requisition AS requisition_num,
        stg.employee AS employee_num,
        'A' AS delete_ind,
        current_ts AS valid_from_date,
        'L' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.znreqhist AS stg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.employee AS emp 
         ON stg.employee = emp.employee_num
         AND upper(trim(cast(stg.company as string))) = upper(trim(cast(emp.lawson_company_num as string)))
         AND date(emp.valid_to_date) = "9999-12-31"
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.requisition AS rec 
        ON stg.requisition = rec.requisition_num
         AND upper(trim(cast(stg.company as string))) = upper(trim(cast(rec.lawson_company_num as string)))
         AND date(rec.valid_to_date) = "9999-12-31"
        LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk 
        ON upper(substr(concat(coalesce(stg.company, 0), '-', coalesce(stg.employee, 0)), 1, 60)) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'EMPLOYEE'
        LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS lkp_req_sid 
        ON upper(substr(concat(coalesce(stg.company, 0), '-', coalesce(stg.requisition, 0)), 1, 60)) = upper(lkp_req_sid.sk_source_txt)
         AND upper(lkp_req_sid.sk_type) = 'REQUISITION';
END;
