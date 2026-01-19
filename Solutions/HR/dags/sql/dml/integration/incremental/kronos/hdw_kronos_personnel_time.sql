BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_ts datetime;
  SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND);

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.personnel_time_wrk;

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.krono_combined_edw_employee_reject;

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.krono_combined_edw_employee_reject 
      SELECT
          *
        FROM
          {{ params.param_hr_stage_dataset_name }}.krono_combined_edw_employee
        WHERE regexp_contains(hr_company, "^[0-9 ]+$") = false
        AND krono_combined_edw_employee.hr_company IS NOT NULL;
    
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.personnel_time_wrk (employee_num, process_level_code, clock_library_code, personnel_name, hire_date_time, lawson_company_num, job_code, dept_code, pay_type_code, termination_date, employee_34_login_code, source_system_code, dw_last_update_date_time)
      SELECT
          cast(krono_combined_edw_employee.employee_num as int64) AS employee_num,
          lpad(krono_combined_edw_employee.process_level_code, 5, '0'),
          krono_combined_edw_employee.clock_library_code,
          krono_combined_edw_employee.personnel_name,
          cast(krono_combined_edw_employee.hire_date as datetime) AS hire_date,
          coalesce(cast(krono_combined_edw_employee.hr_company as int64), 0),
          krono_combined_edw_employee.job_code,
          substring(krono_combined_edw_employee.dept_code,1,5),
          krono_combined_edw_employee.pay_type_code,
          krono_combined_edw_employee.termination_date AS termination_date,
          krono_combined_edw_employee.employee_34_login_code,
          'K',
          timestamp_trunc(current_datetime('US/Central'), SECOND)
        FROM
          {{ params.param_hr_stage_dataset_name }}.krono_combined_edw_employee
        WHERE Regexp_contains(hr_company, "^[0-9 ]+$") = true
        OR krono_combined_edw_employee.hr_company IS NULL
        QUALIFY row_number() OVER (PARTITION BY krono_combined_edw_employee.employee_num, krono_combined_edw_employee.process_level_code, krono_combined_edw_employee.clock_library_code ORDER BY krono_combined_edw_employee.employee_num) = 1
    ;
  BEGIN TRANSACTION;
      UPDATE {{ params.param_hr_core_dataset_name }}.personnel_time AS tgt SET valid_to_date = current_ts - INTERVAL 1 SECOND, dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND) FROM {{ params.param_hr_stage_dataset_name }}.personnel_time_wrk AS wrk WHERE tgt.valid_to_date = datetime('9999-12-31 23:59:59')
         AND trim(cast(tgt.employee_num as string)) = trim(cast(wrk.employee_num as string))
        AND lpad(tgt.process_level_code, 5, '0') = wrk.process_level_code
        AND coalesce(tgt.clock_library_code, '') = coalesce(wrk.clock_library_code,'')
        AND (
        upper(coalesce(wrk.personnel_name,'')) <> upper(coalesce(tgt.personnel_name,''))
        OR coalesce(wrk.hire_date_time, '1900-01-01 00:00:00') <> coalesce(tgt.hire_date_time,'1900-01-01 00:00:00')
        OR coalesce(wrk.lawson_company_num,0) <> coalesce(tgt.lawson_company_num, 0)
        OR upper(coalesce(wrk.job_code, ' ')) <> upper(coalesce(tgt.job_code, ' '))
        OR upper(coalesce(wrk.dept_code, ' ')) <> upper(coalesce(tgt.dept_code, ' '))
        OR upper(coalesce(wrk.pay_type_code, ' ')) <> upper(coalesce(tgt.pay_type_code, ' '))
        OR wrk.termination_date <> tgt.termination_date
        OR upper(coalesce(nullif(ltrim(wrk.employee_34_login_code),''), ' ')) <> upper(coalesce(nullif(ltrim(tgt.employee_34_login_code),''), ' ')));
    
      INSERT INTO {{ params.param_hr_core_dataset_name }}.personnel_time (employee_num, process_level_code, clock_library_code, valid_from_date, valid_to_date, personnel_name, hire_date_time, lawson_company_num, job_code, dept_code, pay_type_code, termination_date, employee_34_login_code, source_system_code, dw_last_update_date_time)
        SELECT
            wrk.employee_num,
            wrk.process_level_code,
            wrk.clock_library_code,
            current_ts,
            datetime("9999-12-31 23:59:59"),
            wrk.personnel_name,
            wrk.hire_date_time,
            wrk.lawson_company_num,
            wrk.job_code,
            wrk.dept_code,
            wrk.pay_type_code,
            wrk.termination_date,
            wrk.employee_34_login_code,
            wrk.source_system_code,
            wrk.dw_last_update_date_time
          FROM
            {{ params.param_hr_stage_dataset_name }}.personnel_time_wrk AS wrk
          WHERE (wrk.employee_num, wrk.process_level_code, wrk.clock_library_code) NOT IN(
            SELECT AS STRUCT
                tgt.employee_num,
                lpad(tgt.process_level_code, 5, '0'),
                tgt.clock_library_code
              FROM
                {{ params.param_hr_core_dataset_name }}.personnel_time AS tgt
              WHERE tgt.valid_to_date = datetime('9999-12-31 23:59:59')
          )
          QUALIFY row_number() OVER (PARTITION BY wrk.employee_num, wrk.process_level_code, wrk.clock_library_code ORDER BY wrk.employee_num) = 1
      ;
      /* Test Unique Primary Index constraint set in Terdata */
      SET DUP_COUNT = ( 
          select count(*)
          from (
          select
              Employee_Num, Process_Level_Code, Clock_Library_Code, Valid_From_Date
          from {{ params.param_hr_core_dataset_name }}.personnel_time 
          group by Employee_Num, Process_Level_Code, Clock_Library_Code, Valid_From_Date
          having count(*) > 1
          )
      );
      IF DUP_COUNT <> 0 THEN
        ROLLBACK TRANSACTION;
        RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: edwhr_copy.personnel_time');
      ELSE
        COMMIT TRANSACTION;
      END IF;
END;
