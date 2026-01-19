BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_ts datetime;
  SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND);

/*  Truncate Wrk Table      */

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_position_wrk;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_position_wrk (employee_sid, position_sid, position_level_sequence_num, eff_from_date, eff_to_date, fte_percent, working_location_code, schedule_work_code, pay_rate_amt, last_update_date, dept_sid, active_dw_ind, delete_ind, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time, row_id, valid_from_date, valid_to_date, job_code, employee_num, account_unit_num, gl_company_num)
    SELECT
        -- XWLK.SK AS Employee_SID
        coalesce(emp.employee_sid, 0) AS employee_sid,
        -- ,LKP_POS_SID.SK AS Position_SID
        coalesce(jp.position_sid, 0) AS position_sid,
        pos.pos_level AS position_level_sequence_num,
        pos.effect_date AS eff_fom_date,
        CASE
          WHEN pos.end_date = '1800-01-01' THEN DATE '9999-12-31'
          ELSE pos.end_date
        END AS eff_to_date,
        pos.fte AS fte_percent,
        pos.locat_code AS working_location_code,
        pos.work_sched AS schedule_work_code,
        pos.pay_rate AS pay_rate_amt,
        -- ,CASE WHEN POS.END_Date >= CURRENT_DATE  OR POS.END_Date = '1800-01-01'  THEN 'Y'  ELSE 'N' END AS Active_DW_Ind
        pos.date_stamp AS last_update_date,
        -- ,DPT.Dept_SID AS Dept_SID
        coalesce(dpt.dept_sid, 0) AS dept_sid,
        'Y' AS active_dw_ind,
        'A' AS delete_ind,
        pos.company AS lawson_company_num,
        pos.process_level AS process_level_code,
        'L' AS source_system_code,
        current_ts AS dw_last_update_date_time,
        row_number() OVER (PARTITION BY pos.company, pos.employee, pos.position, pos.pos_level ORDER BY pos.date_stamp DESC, pos.time_stamp DESC, pos.effect_date DESC) AS row_id,
        current_ts AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        pos.job_code AS job_code,
        pos.employee AS employee_num,
        pos.exp_acct_unit AS account_unit_num,
        pos.exp_company AS gl_company_num
      FROM
        {{ params.param_hr_stage_dataset_name }}.paemppos AS pos
        INNER JOIN (
          SELECT
              employee.employee_sid,
              employee.employee_num,
              employee.lawson_company_num
            FROM
              {{ params.param_hr_base_views_dataset_name }}.employee
            WHERE employee.valid_to_date = DATETIME("9999-12-31 23:59:59")
            GROUP BY 1, 2, 3
        ) AS emp ON pos.employee = emp.employee_num
         AND pos.company = emp.lawson_company_num
        LEFT OUTER JOIN (
          SELECT
              job_position.position_sid,
              job_position.position_code,
              job_position.lawson_company_num
            FROM
              {{ params.param_hr_base_views_dataset_name }}.job_position
            WHERE job_position.valid_to_date = DATETIME("9999-12-31 23:59:59")
            GROUP BY 1, 2, 3
        ) AS jp ON trim(pos.position)= jp.position_code
         AND pos.company = jp.lawson_company_num
        LEFT OUTER JOIN (
          SELECT
              department.dept_sid,
              department.dept_code,
              department.process_level_code,
              department.lawson_company_num
            FROM
              {{ params.param_hr_base_views_dataset_name }}.department
            WHERE department.valid_to_date = DATETIME("9999-12-31 23:59:59")
            GROUP BY 1, 2, 3, 4
        ) AS dpt ON trim(pos.department) = dpt.dept_code
         AND pos.process_level = dpt.process_level_code
         AND pos.company = dpt.lawson_company_num ;
    
  UPDATE {{ params.param_hr_core_dataset_name }}.employee_position AS tgt SET active_dw_ind = 'N', dw_last_update_date_time = current_ts, valid_to_date = current_ts- interval 1 SECOND FROM (
    SELECT
        employee_position_wrk.employee_sid,
        employee_position_wrk.position_sid,
        employee_position_wrk.position_level_sequence_num,
        employee_position_wrk.eff_from_date,
        employee_position_wrk.eff_to_date,
        employee_position_wrk.fte_percent,
        employee_position_wrk.working_location_code,
        employee_position_wrk.last_update_date,
        employee_position_wrk.dept_sid,
        employee_position_wrk.account_unit_num,
        employee_position_wrk.gl_company_num,
        employee_position_wrk.schedule_work_code,
        employee_position_wrk.pay_rate_amt,
        employee_position_wrk.active_dw_ind,
        employee_position_wrk.lawson_company_num,
        employee_position_wrk.process_level_code,
        employee_position_wrk.valid_from_date,
        employee_position_wrk.valid_to_date,
        employee_position_wrk.job_code,
        employee_position_wrk.employee_num
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_position_wrk
  ) AS stg WHERE tgt.employee_sid = stg.employee_sid
   AND tgt.position_sid = stg.position_sid
   AND tgt.position_level_sequence_num = stg.position_level_sequence_num
   AND upper(tgt.active_dw_ind) = 'Y'
   AND upper(tgt.delete_ind) = 'A'
   AND tgt.eff_from_date = stg.eff_from_date
   AND 
   (tgt.fte_percent <> stg.fte_percent
   OR upper(trim(coalesce(tgt.working_location_code, ''))) <> upper(trim(coalesce(stg.working_location_code, '')))
   OR upper(trim(coalesce(tgt.schedule_work_code, ''))) <> upper(trim(coalesce(stg.schedule_work_code, '')))
   OR coalesce(tgt.pay_rate_amt, NUMERIC '0.0') <> coalesce(stg.pay_rate_amt, NUMERIC '0.0')
   OR tgt.dept_sid <> stg.dept_sid
   OR tgt.account_unit_num<>stg.account_unit_num
   OR tgt.gl_company_num <> stg.gl_company_num
   OR tgt.process_level_code <> stg.process_level_code
   OR trim(tgt.job_code) <> trim(stg.job_code)
   OR tgt.eff_to_date <> stg.eff_to_date
   OR tgt.last_update_date <> stg.last_update_date)
   AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
   AND UPPER(tgt.source_system_code) = 'L' ;
   
   BEGIN TRANSACTION ;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.employee_position (employee_sid, position_sid, position_level_sequence_num, eff_from_date, eff_to_date, fte_percent, working_location_code, schedule_work_code, pay_rate_amt, last_update_date, dept_sid, account_unit_num, gl_company_num, active_dw_ind, lawson_company_num, process_level_code, delete_ind, source_system_code, dw_last_update_date_time, valid_from_date, valid_to_date, job_code, employee_num)
    SELECT
        stg.employee_sid,
        stg.position_sid,
        stg.position_level_sequence_num,
        stg.eff_from_date,
        stg.eff_to_date,
        stg.fte_percent,
        stg.working_location_code,
        stg.schedule_work_code,
        stg.pay_rate_amt,
        stg.last_update_date,
        stg.dept_sid,
        stg.account_unit_num,
        stg.gl_company_num,
        stg.active_dw_ind,
        stg.lawson_company_num,
        stg.process_level_code,
        stg.delete_ind,
        stg.source_system_code,
        stg.dw_last_update_date_time,
        stg.valid_from_date,
        stg.valid_to_date,
        stg.job_code,
        stg.employee_num
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_position_wrk AS stg
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_position AS tgt ON tgt.employee_sid = stg.employee_sid
         AND tgt.position_sid = stg.position_sid
         AND tgt.position_level_sequence_num = stg.position_level_sequence_num
         AND tgt.eff_from_date = stg.eff_from_date
         AND upper(tgt.delete_ind) = 'A'
         AND upper(tgt.active_dw_ind) = 'N'
         AND tgt.valid_to_date = current_ts - interval 1 SECOND
         AND
         ( tgt.fte_percent <> stg.fte_percent
         OR upper(trim(coalesce(tgt.working_location_code, ''))) <> upper(trim(coalesce(stg.working_location_code, '')))
         OR upper(trim(coalesce(tgt.schedule_work_code, ''))) <> upper(trim(coalesce(stg.schedule_work_code, '')))
         OR coalesce(tgt.pay_rate_amt, NUMERIC '0.0') <> coalesce(stg.pay_rate_amt, NUMERIC '0.0')
         OR tgt.lawson_company_num <> stg.lawson_company_num
         OR tgt.process_level_code <> stg.process_level_code
         OR tgt.job_code <> stg.job_code
         OR tgt.eff_to_date <> stg.eff_to_date
         OR tgt.last_update_date <> stg.last_update_date
         OR tgt.dept_sid <> stg.dept_sid
         OR tgt.account_unit_num  <> stg.account_unit_num
         OR tgt.gl_company_num <> stg.gl_company_num)
  ;

/* NEW Record Inserted in Target*/

  INSERT INTO {{ params.param_hr_core_dataset_name }}.employee_position (employee_sid, position_sid, position_level_sequence_num, eff_from_date, eff_to_date, fte_percent, working_location_code, schedule_work_code, pay_rate_amt, last_update_date, dept_sid, account_unit_num, gl_company_num, active_dw_ind, lawson_company_num, process_level_code, delete_ind, source_system_code, dw_last_update_date_time, valid_from_date, valid_to_date, job_code, employee_num)
    SELECT
        stg.employee_sid,
        stg.position_sid,
        stg.position_level_sequence_num,
        stg.eff_from_date,
        stg.eff_to_date,
        stg.fte_percent,
        stg.working_location_code,
        stg.schedule_work_code,
        stg.pay_rate_amt,
        stg.last_update_date,
        stg.dept_sid,
        stg.account_unit_num,
        stg.gl_company_num,
        stg.active_dw_ind,
        stg.lawson_company_num,
        stg.process_level_code,
        stg.delete_ind,
        stg.source_system_code,
        current_ts AS dw_last_update_date_time,
        stg.valid_from_date,
        stg.valid_to_date,
        stg.job_code,
        stg.employee_num
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_position_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_position AS tgt ON tgt.employee_sid = stg.employee_sid
         AND tgt.position_sid = stg.position_sid
         AND tgt.position_level_sequence_num = stg.position_level_sequence_num
         AND tgt.eff_from_date = stg.eff_from_date
         AND tgt.valid_to_date =DATETIME("9999-12-31 23:59:59")
      WHERE tgt.employee_sid IS NULL
  ;

  UPDATE {{ params.param_hr_core_dataset_name }}.employee_position AS emp SET 
  valid_to_date = current_ts - INTERVAL 1 SECOND, dw_last_update_date_time = current_ts WHERE 
  emp.valid_to_date = DATETIME("9999-12-31 23:59:59")
   AND (emp.employee_sid||emp.position_sid||emp.position_level_sequence_num||emp.eff_from_date) NOT IN(
    SELECT 
        employee_position_wrk.employee_sid
        ||employee_position_wrk.position_sid
        ||employee_position_wrk.position_level_sequence_num
        ||employee_position_wrk.eff_from_date
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_position_wrk
      GROUP BY 1
  ) AND UPPER(emp.source_system_code) = 'L';

/*  UPDATE  DELETE_IND */

  UPDATE {{ params.param_hr_core_dataset_name }}.employee_position AS empl SET delete_ind = 'D', 
  valid_to_date = current_ts -interval 1 SECOND, active_dw_ind = 'N' WHERE upper(empl.delete_ind) = 'A'
   AND (empl.lawson_company_num||empl.employee_num) NOT IN(
    SELECT DISTINCT
        paemppos.company
        ||paemppos.employee
      FROM
        {{ params.param_hr_stage_dataset_name }}.paemppos
    /*UNION DISTINCT
    SELECT DISTINCT AS STRUCT
        msh_paemppos_stg.company,
        msh_paemppos_stg.employee
      FROM
        {{ params.param_hr_stage_dataset_name }}.msh_paemppos_stg*/
  ) AND UPPER(empl.source_system_code) = 'L';

  UPDATE {{ params.param_hr_core_dataset_name }}.employee_position AS empl SET delete_ind = 'A' WHERE upper(empl.delete_ind) = 'D'
   AND (empl.lawson_company_num||empl.employee_num) IN(
    SELECT DISTINCT 
        paemppos.company
        ||paemppos.employee
      FROM
        {{ params.param_hr_stage_dataset_name }}.paemppos
   /* UNION DISTINCT
    SELECT DISTINCT AS STRUCT
        msh_paemppos_stg.company,
        msh_paemppos_stg.employee
      FROM
        {{ params.param_hr_stage_dataset_name }}.msh_paemppos_stg*/
  ) AND UPPER(empl.source_system_code) = 'L';

/* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select employee_sid ,position_sid ,position_level_sequence_num ,eff_from_date ,valid_from_date
        from {{ params.param_hr_core_dataset_name }}.employee_position
        group by employee_sid ,position_sid ,position_level_sequence_num ,eff_from_date ,valid_from_date		
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.employee_Action_Detail');
    ELSE
      COMMIT TRANSACTION;
    END IF;
	

END;
