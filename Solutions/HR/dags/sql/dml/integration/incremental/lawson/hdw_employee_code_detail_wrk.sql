BEGIN
  DECLARE current_ts datetime;
  SET current_ts = datetime_trunc(current_datetime('US/Central'),second);

/*  Truncate Wrk Table      */
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_code_detail_wrk;

/*  Load Wrk Table with Today's records */
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_code_detail_wrk (employee_sid, employee_type_code, employee_sw, employee_code, employee_code_subject_code, employee_code_seq_num, valid_from_date, employee_num, acquired_date, renew_date, certification_renew_date, license_num_text, proficiency_level_text, verified_ind, employee_code_detail_text, company_sponsored_ind, skill_source_code, lawson_company_num, process_level_code, state_code, valid_to_date, active_dw_ind, delete_ind, source_system_code, dw_last_update_date_time)
    SELECT
        coalesce(ee.employee_sid, 0) AS employee_sid,
        emp.type AS employee_type_code,
        emp.emp_app AS employee_sw,
        emp.code AS employee_code,
        CASE
          WHEN trim(emp.subject) = '' THEN '-'
          ELSE trim(emp.subject)
        END AS employee_code_subject_code,
        emp.seq_nbr AS employee_code_seq_num,
        current_ts AS valid_from_date,
        emp.employee AS employee_num,
        emp.date_acquired AS acquired_date,
        emp.renew_date,
        emp.date_returned AS certification_renew_date,
        emp.lic_number AS license_num_text,
        emp.profic_level AS proficiency_level_text,
        emp.verified_flag AS verified_ind,
        emp.instructor AS employee_code_detail_text,
        emp.co_sponsored AS company_sponsored_ind,
        emp.skill_source AS skill_source_code,
        emp.company AS lawson_company_num,
        '00000' AS process_level_code,
        emp.state AS state_code,
        datetime("9999-12-31 23:59:59") AS valid_to_date,
        'Y' AS active_dw_ind,
        'A' AS delete_ind,
        'L' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.empcodes AS emp
        INNER JOIN (
          SELECT
              employee.employee_sid,
              employee.employee_num,
              employee.lawson_company_num
            FROM
              {{ params.param_hr_base_views_dataset_name }}.employee
            WHERE valid_to_date = datetime("9999-12-31 23:59:59")
            GROUP BY 1, 2, 3
        ) AS ee ON emp.employee = ee.employee_num
         AND emp.company = ee.lawson_company_num
      WHERE emp.emp_app <> 1
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24;

END;
    
 