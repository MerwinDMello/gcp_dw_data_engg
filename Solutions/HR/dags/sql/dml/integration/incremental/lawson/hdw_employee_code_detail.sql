BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;

BEGIN TRANSACTION;

  UPDATE {{ params.param_hr_core_dataset_name }}.employee_code_detail AS tgt 
  SET valid_to_date = current_ts - INTERVAL 1 SECOND , active_dw_ind = 'N', dw_last_update_date_time = current_ts FROM {{ params.param_hr_stage_dataset_name }}.employee_code_detail_wrk AS stg WHERE tgt.employee_sid = stg.employee_sid
   AND tgt.employee_type_code = stg.employee_type_code
   AND tgt.employee_sw = stg.employee_sw
   AND tgt.employee_code = stg.employee_code
   AND tgt.employee_code_subject_code = stg.employee_code_subject_code
   AND tgt.employee_code_seq_num = stg.employee_code_seq_num
   AND (coalesce(tgt.acquired_date, DATE '1799-01-01') <> coalesce(stg.acquired_date, DATE '1799-01-01')
   OR coalesce(tgt.renew_date, DATE '1799-01-01') <> coalesce(stg.renew_date, DATE '1799-01-01')
   OR coalesce(tgt.certification_renew_date, DATE '1799-01-01') <> coalesce(stg.certification_renew_date, DATE '1799-01-01')
   OR upper(trim(coalesce(tgt.license_num_text, ''))) <> upper(trim(coalesce(stg.license_num_text, '')))
   OR upper(trim(coalesce(tgt.proficiency_level_text, ''))) <> upper(trim(coalesce(stg.proficiency_level_text, '')))
   OR upper(trim(coalesce(tgt.verified_ind, ''))) <> upper(trim(coalesce(stg.verified_ind, '')))
   OR upper(trim(coalesce(tgt.employee_code_detail_text, ''))) <> upper(trim(coalesce(stg.employee_code_detail_text, '')))
   OR upper(trim(coalesce(tgt.company_sponsored_ind, ''))) <> upper(trim(coalesce(stg.company_sponsored_ind, '')))
   OR upper(trim(coalesce(tgt.skill_source_code, ''))) <> upper(trim(coalesce(stg.skill_source_code, '')))
   OR upper(trim(coalesce(tgt.process_level_code, ''))) <> upper(trim(coalesce(stg.process_level_code, '')))
   OR upper(trim(coalesce(tgt.state_code, ''))) <> upper(trim(coalesce(stg.state_code, ''))))
   AND tgt.valid_to_date =  DATETIME("9999-12-31 23:59:59")
   and tgt.source_system_code = 'L';

  INSERT INTO {{ params.param_hr_core_dataset_name }}.employee_code_detail (employee_sid, employee_type_code, employee_sw, employee_code, employee_code_subject_code, employee_code_seq_num, valid_from_date, employee_num, acquired_date, renew_date, certification_renew_date, license_num_text, proficiency_level_text, verified_ind, employee_code_detail_text, company_sponsored_ind, skill_source_code, lawson_company_num, process_level_code, state_code, valid_to_date, active_dw_ind, delete_ind, source_system_code, dw_last_update_date_time)
    SELECT
        stg.employee_sid,
        stg.employee_type_code,
        stg.employee_sw,
        stg.employee_code,
        stg.employee_code_subject_code,
        stg.employee_code_seq_num,
        current_ts,
        stg.employee_num,
        stg.acquired_date,
        stg.renew_date,
        stg.certification_renew_date,
        stg.license_num_text,
        stg.proficiency_level_text,
        stg.verified_ind,
        stg.employee_code_detail_text,
        stg.company_sponsored_ind,
        stg.skill_source_code,
        stg.lawson_company_num,
        stg.process_level_code,
        stg.state_code,
        stg.valid_to_date,
        stg.active_dw_ind,
        stg.delete_ind,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_code_detail_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.employee_code_detail AS tgt ON tgt.employee_sid = stg.employee_sid
         AND tgt.employee_type_code = stg.employee_type_code
         AND tgt.employee_sw = stg.employee_sw
         AND tgt.employee_code = stg.employee_code
         AND tgt.employee_code_subject_code = stg.employee_code_subject_code
         AND tgt.employee_code_seq_num = stg.employee_code_seq_num
         AND coalesce(tgt.acquired_date, DATE '1799-01-01') = coalesce(stg.acquired_date, DATE '1799-01-01')
         AND coalesce(tgt.renew_date, DATE '1799-01-01') = coalesce(stg.renew_date, DATE '1799-01-01')
         AND coalesce(tgt.certification_renew_date, DATE '1799-01-01') = coalesce(stg.certification_renew_date, DATE '1799-01-01')
         AND upper(trim(coalesce(tgt.license_num_text, ''))) = upper(trim(coalesce(stg.license_num_text, '')))
         AND upper(trim(coalesce(tgt.proficiency_level_text, ''))) = upper(trim(coalesce(stg.proficiency_level_text, '')))
         AND upper(trim(coalesce(tgt.verified_ind, ''))) = upper(trim(coalesce(stg.verified_ind, '')))
         AND upper(trim(coalesce(tgt.employee_code_detail_text, ''))) = upper(trim(coalesce(stg.employee_code_detail_text, '')))
         AND upper(trim(coalesce(tgt.company_sponsored_ind, ''))) = upper(trim(coalesce(stg.company_sponsored_ind, '')))
         AND upper(trim(coalesce(tgt.skill_source_code, ''))) = upper(trim(coalesce(stg.skill_source_code, '')))
         AND upper(trim(coalesce(tgt.process_level_code, ''))) = upper(trim(coalesce(stg.process_level_code, '')))
         AND upper(trim(coalesce(tgt.state_code, ''))) = upper(trim(coalesce(stg.state_code, '')))
         AND tgt.valid_to_date =  DATETIME("9999-12-31 23:59:59")
      WHERE tgt.employee_sid IS NULL
  ;
/*  UPDATE  DELETE_IND   */
  UPDATE {{ params.param_hr_core_dataset_name }}.employee_code_detail AS tgt SET delete_ind = emp.delete_ind, valid_to_date = current_ts - INTERVAL 1 SECOND, active_dw_ind = 'N', dw_last_update_date_time = current_ts FROM (
    SELECT
        employee.employee_sid,
        employee.delete_ind
      FROM
        {{ params.param_hr_core_dataset_name }}.employee
      WHERE upper(employee.delete_ind) = 'D'
       AND employee.valid_to_date = DATETIME("9999-12-31 23:59:59")
  ) AS emp WHERE tgt.employee_sid = emp.employee_sid
   AND upper(tgt.delete_ind) = 'A'
   and tgt.source_system_code = 'L';

  UPDATE {{ params.param_hr_core_dataset_name }}.employee_code_detail AS tgt SET delete_ind = emp.delete_ind, dw_last_update_date_time = current_ts FROM (
    SELECT
        employee.employee_sid,
        employee.delete_ind
      FROM
        {{ params.param_hr_core_dataset_name }}.employee
      WHERE upper(employee.delete_ind) = 'A'
       AND employee.valid_to_date = DATETIME("9999-12-31 23:59:59")
  ) AS emp WHERE tgt.employee_sid = emp.employee_sid
   AND upper(tgt.delete_ind) = 'D'
   and tgt.source_system_code = 'L';

/* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select Employee_SID, Employee_Type_Code ,Employee_Sw ,Employee_Code ,Employee_Code_Subject_Code ,Employee_Code_Seq_Num ,Valid_From_Date
        from {{ params.param_hr_core_dataset_name }}.employee_code_detail
        group by Employee_SID, Employee_Type_Code ,Employee_Sw ,Employee_Code ,Employee_Code_Subject_Code , Employee_Code_Seq_Num ,Valid_From_Date		
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.employee_code_detail');
    ELSE
      COMMIT TRANSACTION;
    END IF;
/*  RETIRE RECORD ON 2ND RETIRE LOGIC */
  UPDATE {{ params.param_hr_core_dataset_name }}.employee_code_detail AS tgt SET active_dw_ind = 'N', valid_to_date = current_ts - INTERVAL 1 SECOND, dw_last_update_date_time = current_ts WHERE tgt.valid_to_date =  DATETIME("9999-12-31 23:59:59")
   AND (tgt.employee_sid, tgt.employee_type_code, tgt.employee_sw, tgt.employee_code, tgt.employee_code_subject_code, tgt.employee_code_seq_num) NOT IN(
    SELECT AS STRUCT
        employee_code_detail_wrk.employee_sid,
        employee_code_detail_wrk.employee_type_code,
        employee_code_detail_wrk.employee_sw,
        employee_code_detail_wrk.employee_code,
        employee_code_detail_wrk.employee_code_subject_code,
        employee_code_detail_wrk.employee_code_seq_num
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_code_detail_wrk
      GROUP BY 1, 2, 3, 4, 5, 6
  )and tgt.source_system_code = 'L';
END;

