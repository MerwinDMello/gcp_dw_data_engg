BEGIN
  DECLARE DUP_COUNT INT64;

  BEGIN TRANSACTION;
    INSERT INTO {{ params.param_hr_core_dataset_name }}.bi_executive_current (employee_sid, reporting_period, employee_num, lawson_company_num, employee_34_login_code, employee_full_name, ethnic_origin_code, ethnic_class_desc, gender_code, pay_grade_code, benefit_salary_amt, anniversary_date, termination_date, hire_date, birth_date, facility_date, job_experience_date, executive_development_program_code, executive_development_program_ind, position_code, position_code_desc, position_num, job_code, dept_code, dept_name, process_level_name, process_level_code, group_name, division_name, market_name, coid_name, coid, company_code, lob_code, hospital_category_code, facility_street_address_1_text, facility_street_address_2_text, facility_city_name, facility_state_code, facility_zip_code, facility_phone_num, facility_union_code, source_system_code, dw_last_update_date_time)
    SELECT
        e.employee_sid,
        substr(CAST(lud.pe_date_prior_month AS STRING), 1, 7) AS reporting_period,
        -- Current_Date AS Load_Date,
        e.employee_num,
        e.lawson_company_num,
        e.employee_34_login_code,
        trim(concat(trim(ep.employee_last_name), ', ', trim(ep.employee_first_name), ' ', trim(ep.employee_middle_name))) AS employee_full_name,
        ep.ethnic_origin_code,
        hrd.demographic_desc AS ethnic_class_desc,
        ep.gender_code,
        e.pay_grade_code,
        ep.benefit_salary_amt,
        e.anniversary_date,
        CASE
          WHEN e.termination_date = '1800-01-01' THEN NULL
          ELSE e.termination_date
        END AS termination_date,
        e.hire_date,
        ep.birth_date,
        ed.detail_value_date AS facility_date,
        ed2.detail_value_date AS job_experience_date,
        ed3.detail_value_alphanumeric_text AS executive_development_program_code,
        max(CASE
          WHEN ed3.detail_value_alphanumeric_text IS NULL THEN 'N'
          ELSE 'Y'
        END) AS executive_development_program_ind,
        substr(jp.position_code_desc, 1, 3) AS position_code,
        jp.position_code_desc,
        CAST(jp.position_code AS INT64) AS position_num,
        epp.job_code,
        d.dept_code,
        d.dept_name,
        pl.process_level_name,
        pl.process_level_code,
        ff.group_name,
        ff.division_name,
        ff.market_name,
        ff.coid_name,
        ff.coid,
        -- ff.LOB_Code,
        ff.company_code,
        ff.lob_code,
        llhc.hospital_category_code,
        fa.street_address_1_text AS facility_street_address_1_text,
        fa.street_address_2_text AS facility_street_address_2_text,
        fa.city_name AS facility_city_name,
        fa.state_code AS facility_state_code,
        fa.postal_code AS facility_zip_code,
        facnum.facility_phone_num,
        CASE
          WHEN upper(trim(rplu.facility_union_code)) = 'Y' THEN 'Yes'
          ELSE 'No'
        END AS facility_union_code,
        'L' AS source_system_code,
        DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        {{ params.param_hr_base_views_dataset_name }}.employee AS e
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.junc_employee_status AS jes ON e.employee_sid = jes.employee_sid
         AND jes.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND upper(trim(jes.status_type_code)) = 'EMP'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_detail AS ed ON e.employee_sid = ed.employee_sid
         AND ed.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND upper(trim(ed.employee_detail_code)) = '93'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_detail AS ed2 ON e.employee_sid = ed2.employee_sid
         AND ed2.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND upper(trim(ed2.employee_detail_code)) = '92'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_detail AS ed3 ON e.employee_sid = ed3.employee_sid
         AND ed3.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND upper(trim(ed3.employee_detail_code)) = '75'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS ep ON e.employee_sid = ep.employee_sid
         AND ep.valid_to_date = DATETIME("9999-12-31 23:59:59")
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_position AS epp ON e.employee_sid = epp.employee_sid
         AND epp.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND epp.eff_to_date = "9999-12-31"
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON epp.position_sid = jp.position_sid
         AND jp.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND jp.eff_to_date = "9999-12-31"
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS pl ON e.process_level_sid = pl.process_level_sid
         AND pl.valid_to_date = DATETIME("9999-12-31 23:59:59")
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS d ON e.dept_sid = d.dept_sid
         AND d.valid_to_date = DATETIME("9999-12-31 23:59:59")
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS gldc ON e.account_unit_num = gldc.account_unit_num
         AND e.gl_company_num = gldc.gl_company_num
         AND gldc.valid_to_date = DATETIME("9999-12-31 23:59:59")
        LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON gldc.coid = ff.coid
         AND gldc.company_code = ff.company_code
        LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.facility_address AS fa ON gldc.coid = fa.coid
         AND gldc.company_code = fa.company_code
         AND upper(trim(fa.address_type_code)) = 'PH'
        LEFT OUTER JOIN (
          SELECT
              fcd.coid,
              fcd.company_code,
              fcd.facility_phone_num
            FROM
              {{ params.param_pub_views_dataset_name }}.facility_comm_device AS fcd
            WHERE upper(trim(fcd.facility_phone_type_code)) = 'F'
            QUALIFY row_number() OVER (PARTITION BY fcd.coid, fcd.company_code ORDER BY fcd.facility_phone_type_code DESC, fcd.eff_from_date DESC) = 1
        ) AS facnum ON gldc.coid = facnum.coid
         AND gldc.company_code = facnum.company_code
        LEFT OUTER JOIN (
          SELECT DISTINCT
              ref_process_level_union.process_level_code,
              'Y' AS facility_union_code
            FROM
              {{ params.param_hr_base_views_dataset_name }}.ref_process_level_union
        ) AS rplu ON d.dept_code = rplu.process_level_code
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_demographic AS hrd ON ep.ethnic_origin_code = hrd.demographic_code
         AND upper(trim(hrd.demographic_type_code)) = 'ET'
        LEFT OUTER JOIN (
          SELECT
              ref_lawson_location_hospital_category.lawson_location_code,
              ref_lawson_location_hospital_category.hospital_category_code,
              ref_lawson_location_hospital_category.hospital_category_code_year
            FROM
              {{ params.param_hr_base_views_dataset_name }}.ref_lawson_location_hospital_category
            QUALIFY row_number() OVER (PARTITION BY ref_lawson_location_hospital_category.lawson_location_code ORDER BY ref_lawson_location_hospital_category.hospital_category_code_year DESC) = 1
        ) AS llhc ON e.location_code = llhc.lawson_location_code
        LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS lud ON lud.date_id = current_date('US/Central')
      WHERE e.valid_to_date = DATETIME("9999-12-31 23:59:59")
       AND upper(trim(e.process_level_code)) = '10202'
       AND upper(trim(jes.status_code)) IN(
        '01', '02', '03', '04', '05'
      )
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, upper(CASE
        WHEN ed3.detail_value_alphanumeric_text IS NULL THEN 'N'
        ELSE 'Y'
      END), 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43
      QUALIFY row_number() OVER (PARTITION BY e.employee_sid, reporting_period ORDER BY position_code) = 1
  ;

    /* Test Unique Primary Index constraint set in Teradata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            employee_sid, reporting_period
        from {{ params.param_hr_core_dataset_name }}.bi_executive_current
        group by employee_sid, reporting_period
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: ' , '{{ params.param_hr_core_dataset_name }}' , '.bi_executive_current');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;