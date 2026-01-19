
/*  GENERATE THE SURROGATE KEYS FOR NURSING STUDENT */
CALL `{{ params.param_hr_core_dataset_name }}`.sk_gen('{{ params.param_hr_stage_dataset_name }}', 'galen_stg', 'TRIM(Student_ID)||\'-C\'', 'NURSING_STUDENT');

  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.nursing_student_wrk;

/*  Load Work Table */

  INSERT INTO {{ params.param_hr_stage_dataset_name }}.nursing_student_wrk (student_sid, valid_from_date, valid_to_date, student_num, student_ssn, student_first_name, student_last_name, student_middle_name, birth_date, gender_code, ethnic_origin_desc, addr_sid, pell_grant_eligibility_ind, first_gen_college_grad_ind, employee_num, source_system_code, dw_last_update_date_time)
    SELECT DISTINCT
        cast(xwlk.sk as int64) AS student_sid,
        datetime_trunc(current_datetime('US/Central'),second) AS valid_from_date,
        DATETIME('9999-12-31 23:59:59') AS valid_to_date,
        CASE
           trim(stg.student_id)
          WHEN '' THEN 0
          ELSE CAST(trim(stg.student_id) as INT64)
        END AS student_num,
        replace(stg.ssn, '-', '') AS student_ssn,
        trim(stg.first_name) AS student_first_name,
        trim(stg.last_name) AS student_last_name,
        trim(stg.middle_initial) AS student_middle_name,
        stg.birthdate AS birth_date,
        -- CASE STG.GENDER WHEN 'Male' THEN 'M' WHEN 'Female' THEN 'F' ELSE 'U' END AS Gender_Code,
        stg.gender AS gender_code,
        trim(stg.ethnicity_5_options) AS ethnic_origin_desc,
        ad.addr_sid AS addr_sid,
        CASE
          WHEN upper(stg.pell_grant_eligibility) = 'YES' THEN 'Y'
          WHEN upper(stg.pell_grant_eligibility) = 'NO' THEN 'N'
          ELSE ''
        END AS pell_grant_eligibility_ind,
        CASE
          WHEN upper(stg.first_generation_college_grad) = 'YES' THEN 'Y'
          WHEN upper(stg.first_generation_college_grad) = 'NO' THEN 'N'
          ELSE ''
        END AS first_gen_college_grad_ind,
        ep.employee_num AS employee_num,
        'C' AS source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.galen_stg AS stg
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.ref_sk_xwlk AS xwlk ON concat(trim(student_id), '-C') = trim(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'NURSING_STUDENT'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.address AS ad ON stg.home_address_street = ad.addr_line_1_text
         AND stg.home_address_city = ad.city_name
         AND stg.home_address_state = ad.state_code
         AND stg.home_address_zip = ad.zip_code
         AND upper(ad.addr_type_code) = 'SHA'
         AND upper(ad.source_system_code) = 'C'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS ep ON ep.employee_ssn = replace(stg.ssn, '-', '')
         AND ep.valid_to_date = '9999-12-31 23:59:59'
      QUALIFY row_number() OVER (PARTITION BY student_sid ORDER BY employee_num DESC, grad_date DESC) = 1
  ;
