BEGIN
  DECLARE DUP_COUNT INT64;

  BEGIN TRANSACTION;
    INSERT INTO {{ params.param_hr_core_dataset_name }}.bi_executive_vacancy (requisition_sid, reporting_period, lawson_company_num, requisition_num, group_name, division_name, market_name, coid_name, coid, company_code, lob_code, hospital_category_code, facility_street_address_1_text, facility_street_address_2_text, facility_city_name, facility_state_code, facility_zip_code, requisition_desc, job_code, job_code_desc, status_code, requisition_open_date, requisition_closed_date, approval_date_1, approval_date_2, approval_date_3, approval_date_4, approval_date_5, last_approval_date, days_open_num, location_code, process_level_code, dept_code, source_system_code, dw_last_update_date_time)
      SELECT
          r.requisition_sid,
          substr(CAST(lud.pe_date_prior_month AS STRING), 1, 7) AS reporting_period,
          -- Current_Date AS Load_Date,
          r.lawson_company_num,
          r.requisition_num,
          ff.group_name,
          ff.division_name,
          ff.market_name,
          ff.coid_name,
          ff.coid,
          ff.company_code,
          ff.lob_code,
          /*Coalesce(ff.Group_Name,fff.Group_Name) AS Group_Name,
          Coalesce(ff.Division_Name,fff.Division_Name) AS Division_Name,
          Coalesce(ff.Market_Name,fff.Market_Name) AS Market_Name,
          Coalesce(ff.Coid_Name, fff.Coid_Name) AS Coid_Name,
          Coalesce(ff.Coid, fff.Coid) AS Coid,
          Coalesce(ff.Company_Code,fff.Company_Code) AS Company_Code,
          Coalesce(ff.LOB_Code, fff.LOB_Code) AS LOB_Code,*/
          llhc.hospital_category_code,
          fa.street_address_1_text AS street_address_1_text,
          fa.street_address_2_text AS street_address_2_text,
          fa.city_name AS city_name,
          fa.state_code AS state_code,
          fa.postal_code AS zip_code,
          /*Coalesce(fa.Street_Address_1_Text,faa.Street_Address_1_Text) AS Street_Address_1_Text,
          Coalesce(fa.Street_Address_2_Text,faa.Street_Address_2_Text) AS Street_Address_2_Text,
          Coalesce(fa.City_Name,faa.City_Name) AS City_Name,
          Coalesce(fa.State_Code,faa.State_Code) AS State_Code,
          Coalesce(fa.Postal_Code,faa.Postal_Code) AS Zip_Code,*/
          r.requisition_desc,
          jc.job_code,
          jc.job_code_desc,
          rs.status_code,
          r.requisition_open_date,
          r.requisition_closed_date,
          ras1.approval_start_date AS approval_date_1,
          ras2.approval_start_date AS approval_date_2,
          ras3.approval_start_date AS approval_date_3,
          ras4.approval_start_date AS approval_date_4,
          ras5.approval_start_date AS approval_date_5,
          coalesce(ras5.approval_start_date, ras4.approval_start_date, ras3.approval_start_date, ras2.approval_start_date, ras1.approval_start_date) AS last_approval_date,
          date_diff(current_date('US/Central'), coalesce(ras5.approval_start_date, ras4.approval_start_date, ras3.approval_start_date, ras2.approval_start_date, ras1.approval_start_date), DAY) AS days_open_num,
          r.location_code,
          gldc.process_level_code,
          -- Coalesce(gldc.Process_Level_Code,gl.Process_Level_Code,'00000') AS Process_Level_Code,
          rd.dept_code,
          'L' AS source_system_code,
          DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          {{ params.param_hr_base_views_dataset_name }}.requisition AS r
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_department AS rd ON r.requisition_sid = rd.requisition_sid
          AND rd.valid_to_date = DATETIME("9999-12-31 23:59:59")
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_position AS rp ON r.requisition_sid = rp.requisition_sid
          AND rp.valid_to_date = DATETIME("9999-12-31 23:59:59")
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON rp.position_sid = jp.position_sid
          AND jp.valid_to_date = DATETIME("9999-12-31 23:59:59")
          AND jp.eff_to_date = '9999-12-31'
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jc ON jp.job_code_sid = jc.job_code_sid
          AND jc.valid_to_date = DATETIME("9999-12-31 23:59:59")
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_status AS rs ON r.requisition_sid = rs.requisition_sid
          AND rs.valid_to_date = DATETIME("9999-12-31 23:59:59")
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_approval_stage AS ras1 ON r.requisition_sid = ras1.requisition_sid
          AND upper(trim(ras1.approval_desc)) = 'FIRST APPROVAL'
          AND ras1.valid_to_date = DATETIME("9999-12-31 23:59:59")
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_approval_stage AS ras2 ON r.requisition_sid = ras2.requisition_sid
          AND upper(trim(ras2.approval_desc)) = 'SECOND APPROVAL'
          AND ras2.valid_to_date = DATETIME("9999-12-31 23:59:59")
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_approval_stage AS ras3 ON r.requisition_sid = ras3.requisition_sid
          AND upper(trim(ras3.approval_desc)) = 'THIRD APPROVAL'
          AND ras3.valid_to_date = DATETIME("9999-12-31 23:59:59")
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_approval_stage AS ras4 ON r.requisition_sid = ras4.requisition_sid
          AND upper(trim(ras4.approval_desc)) = 'FOURTH APPROVAL'
          AND ras4.valid_to_date = DATETIME("9999-12-31 23:59:59")
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_approval_stage AS ras5 ON r.requisition_sid = ras5.requisition_sid
          AND upper(trim(ras5.approval_desc)) = 'FIFTH APPROVAL'
          AND ras5.valid_to_date = DATETIME("9999-12-31 23:59:59")
          LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.gl_lawson_dept_crosswalk AS gldc ON jp.account_unit_num = gldc.account_unit_num
          AND jp.gl_company_num = gldc.gl_company_num
          AND gldc.valid_to_date = DATETIME("9999-12-31 23:59:59")
          LEFT OUTER JOIN /*LEFT JOIN (
          SELECT	DISTINCT
          Process_Level_Code,
          Coid,
          Company_Code
          FROM	EDWHR_BASE_VIEWS.GL_Lawson_Dept_Crosswalk
          )gldc
            ON rd.Dept_Code = gldc.Coid
          LEFT JOIN EDWHR_BASE_VIEWS.GL_Lawson_Dept_Crosswalk gl
            ON rd.Process_Level_Code = gl.Process_Level_Code
            AND rd.Dept_Code = gl.Account_Unit_Num
            AND rd.Process_Level_Code NOT= '10202'*/
          {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON gldc.coid = ff.coid
          AND gldc.company_code = ff.company_code
          LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.facility_address AS fa ON ff.coid = fa.coid
          AND ff.company_code = fa.company_code
          AND upper(trim(fa.address_type_code)) = 'PH'
          LEFT OUTER JOIN /*
          LEFT JOIN EDW_PUB_VIEWS.Fact_Facility fff
            ON gl.Coid = fff.Coid
            AND gl.Company_Code = fff.Company_Code
          LEFT JOIN EDW_PUB_VIEWS.Facility_Address faa
            ON fff.Coid = faa.Coid
            AND fff.Company_Code = faa.Company_Code
            AND faa.Address_Type_Code = 'PH'*/
          (
            SELECT
                ref_lawson_location_hospital_category.lawson_location_code,
                ref_lawson_location_hospital_category.hospital_category_code,
                ref_lawson_location_hospital_category.hospital_category_code_year
              FROM
                {{ params.param_hr_base_views_dataset_name }}.ref_lawson_location_hospital_category
              QUALIFY row_number() OVER (PARTITION BY ref_lawson_location_hospital_category.lawson_location_code ORDER BY ref_lawson_location_hospital_category.hospital_category_code_year DESC) = 1
          ) AS llhc ON r.location_code = llhc.lawson_location_code
          LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.lu_date AS lud ON lud.date_id = current_date('US/Central')
        WHERE r.valid_to_date = DATETIME("9999-12-31 23:59:59")
        AND r.lawson_company_num = 5950
        AND upper(trim(jc.job_code)) IN(
          '01003', '01011', '01013', '01016', '01015', '01020', '01499', '01303', '01423', '01170', '01046'
        )
        AND upper(trim(rs.status_code)) = 'WFAPPROVE'
  ;

    /* Test Unique Primary Index constraint set in Teradata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            requisition_sid, reporting_period
        from {{ params.param_hr_core_dataset_name }}.bi_executive_vacancy
        group by requisition_sid, reporting_period
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: ' , '{{ params.param_hr_core_dataset_name }}' , '.bi_executive_vacancy');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;