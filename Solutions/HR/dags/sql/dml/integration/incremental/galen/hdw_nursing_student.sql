BEGIN
DECLARE
  DUP_COUNT INT64;
/*  CLOSE THE PREVIOUS RECORDS FROM TARGET TABLE FOR SAME KEY FOR ANY CHANGES  */
/*  INSERT THE NEW RECORDS/CHNAGES INTO THE TARGET TABLE  */
/* BEGIN TRANSACTION BLOCK STARTS HERE */

  BEGIN TRANSACTION;
  UPDATE {{ params.param_hr_core_dataset_name }}.nursing_student AS tgt SET valid_to_date = DATETIME(stg.valid_from_date - INTERVAL 1 DAY), dw_last_update_date_time = datetime_trunc(current_datetime(), SECOND) FROM {{ params.param_hr_stage_dataset_name }}.nursing_student_wrk AS stg WHERE tgt.student_sid = stg.student_sid
   AND (trim(CAST(coalesce(tgt.student_num, -999) as STRING)) <> trim(CAST(coalesce(stg.student_num, -999) as STRING))
   OR upper(trim(coalesce(tgt.student_ssn, 'XXX'))) <> upper(trim(coalesce(stg.student_ssn, 'XXX')))
   OR upper(trim(coalesce(tgt.student_first_name, 'XXX'))) <> upper(trim(coalesce(stg.student_first_name, 'XXX')))
   OR upper(trim(coalesce(tgt.student_last_name, 'XXX'))) <> upper(trim(coalesce(stg.student_last_name, 'XXX')))
   OR upper(trim(coalesce(tgt.student_middle_name, 'XXX'))) <> upper(trim(coalesce(stg.student_middle_name, 'XXX')))
   OR trim(CAST(coalesce(tgt.birth_date, DATE '1900-01-01') as STRING)) <> trim(CAST(coalesce(stg.birth_date, DATE '1900-01-01') as STRING))
   OR upper(trim(coalesce(tgt.gender_code, 'XXX'))) <> upper(trim(coalesce(stg.gender_code, 'XXX')))
   OR upper(trim(coalesce(tgt.ethnic_origin_desc, 'XXX'))) <> upper(trim(coalesce(stg.ethnic_origin_desc, 'XXX')))
   OR trim(CAST(coalesce(tgt.addr_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.addr_sid, -999) as STRING))
   OR upper(trim(coalesce(tgt.pell_grant_eligibility_ind, 'XXX'))) <> upper(trim(coalesce(stg.pell_grant_eligibility_ind, 'XXX')))
   OR upper(trim(coalesce(tgt.first_gen_college_grad_ind, 'XXX'))) <> upper(trim(coalesce(stg.first_gen_college_grad_ind, 'XXX')))
   OR trim(CAST(coalesce(tgt.employee_num, -999) as STRING)) <> trim(CAST(coalesce(stg.employee_num, -999) as STRING))
   OR coalesce(trim(tgt.source_system_code), 'XX') <> coalesce(trim(stg.source_system_code), 'XX'))
   AND tgt.valid_to_date = DATETIME('9999-12-31 23:59:59');

  INSERT INTO {{ params.param_hr_core_dataset_name }}.nursing_student (student_sid, valid_from_date, valid_to_date, student_num, student_ssn, student_first_name, student_last_name, student_middle_name, birth_date, gender_code, ethnic_origin_desc, addr_sid, pell_grant_eligibility_ind, first_gen_college_grad_ind, employee_num, source_system_code, dw_last_update_date_time)
    SELECT
        stg.student_sid,
        stg.valid_from_date,
        stg.valid_to_date,
        stg.student_num,
        stg.student_ssn,
        stg.student_first_name,
        stg.student_last_name,
        stg.student_middle_name,
        stg.birth_date,
        stg.gender_code,
        stg.ethnic_origin_desc,
        stg.addr_sid,
        stg.pell_grant_eligibility_ind,
        stg.first_gen_college_grad_ind,
        stg.employee_num,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.nursing_student_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.nursing_student AS tgt ON stg.student_sid = tgt.student_sid
         AND trim(CAST(coalesce(tgt.student_num, -999) as STRING)) = trim(CAST(coalesce(stg.student_num, -999) as STRING))
         AND upper(trim(coalesce(tgt.student_ssn, 'XXX'))) = upper(trim(coalesce(stg.student_ssn, 'XXX')))
         AND upper(trim(coalesce(tgt.student_first_name, 'XXX'))) = upper(trim(coalesce(stg.student_first_name, 'XXX')))
         AND upper(trim(coalesce(tgt.student_last_name, 'XXX'))) = upper(trim(coalesce(stg.student_last_name, 'XXX')))
         AND upper(trim(coalesce(tgt.student_middle_name, 'XXX'))) = upper(trim(coalesce(stg.student_middle_name, 'XXX')))
         AND trim(CAST(coalesce(tgt.birth_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.birth_date, DATE '1900-01-01') as STRING))
         AND upper(trim(coalesce(tgt.gender_code, 'XXX'))) = upper(trim(coalesce(stg.gender_code, 'XXX')))
         AND upper(trim(coalesce(tgt.ethnic_origin_desc, 'XXX'))) = upper(trim(coalesce(stg.ethnic_origin_desc, 'XXX')))
         AND trim(CAST(coalesce(tgt.addr_sid, -999) as STRING)) = trim(CAST(coalesce(stg.addr_sid, -999) as STRING))
         AND upper(trim(coalesce(tgt.pell_grant_eligibility_ind, 'XXX'))) = upper(trim(coalesce(stg.pell_grant_eligibility_ind, 'XXX')))
         AND upper(trim(coalesce(tgt.first_gen_college_grad_ind, 'XXX'))) = upper(trim(coalesce(stg.first_gen_college_grad_ind, 'XXX')))
         AND trim(CAST(coalesce(tgt.employee_num, -999) as STRING)) = trim(CAST(coalesce(stg.employee_num, -999) as STRING))
         AND coalesce(trim(tgt.source_system_code), 'XX') = coalesce(trim(stg.source_system_code), 'XX')
         AND tgt.valid_to_date = DATETIME('9999-12-31 23:59:59')
      WHERE tgt.student_sid IS NULL
  ;


        SET DUP_COUNT = ( 
        select count(*)
        from (
        select
           student_sid, valid_from_date
        from {{ params.param_hr_core_dataset_name }}.nursing_student
        group by student_sid, valid_from_date
        having count(*) > 1
        )
    );
        IF
         DUP_COUNT <> 0 THEN
          ROLLBACK TRANSACTION; RAISE
            USING
            MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.nursing_student');
        ELSE
        COMMIT TRANSACTION;
        END IF
  ;
END;
/* END TRANSACTION BLOCK COMMENT */
