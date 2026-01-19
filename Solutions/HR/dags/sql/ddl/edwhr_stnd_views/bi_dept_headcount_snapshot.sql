CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.bi_dept_headcount_snapshot AS SELECT
    -- -added 7/11/22
    -- -added 7/11/22
    -- --added 7/11/22
    -- added 7/11/22
    -- added 7/11/22
    -- added 7/11/22
    -- added 7/11/22
    -- added 7/11/22
    hrmet.date_id AS pe_date,
    facfac.sector_name,
    facfac.group_name,
    facfac.division_name,
    facfac.market_name,
    facfac.lob_code,
    facfac.sub_lob_code,
    -- -added 7/11/22
    facfac.cons_facility_name,
    hrmet.coid,
    facfac.coid_name,
    hrmet.process_level_code,
    -- -added 7/11/22
    hrmet.location_code,
    -- --added 7/11/22
    xwalk.dept_num,
    dept.dept_code,
    -- added 7/11/22
    dept.dept_name,
    fd.functional_dept_desc,
    -- -added 7/11/22
    fd.sub_functional_dept_desc,
    -- -added 7/11/22
    stas.status_code AS aux_status_code,
    stast.status_code AS emp_status_code,
    ep.fte_percent AS fte_percent,
    -- added 7/11/22
    hrmet.employee_sid,
    hrmet.employee_num,
    emp.hire_date,
    emp.anniversary_date,
    anni_cte.hr_employee_value_date,
    CASE
      WHEN upper(jobcl.job_class_code) = '103' THEN 'RN'
      WHEN jobco.job_code IN(
        ndir.job_code
      ) THEN 'Leadership'
      WHEN jobpos.position_code_desc = pct.job_title_text THEN 'PCT'
      ELSE 'Non-RN'
    END AS rn_grouping_text,
    mat1.category_desc AS ilob_category,
    mat1.sub_category_desc AS ilob_sub_category,
    hrops.business_unit_name AS hrops_business_unit,
    hrops.business_unit_segment_name AS hrops_segment,
    refkt.key_talent_group_text AS key_talent,
    jobcl.job_class_code,
    jobcl.job_class_desc,
    jobco.job_code,
    -- -added 7/11/22
    jobco.job_code_desc,
    jobpos.position_code_desc,
    --   ,dept.dept_sid   REPLACED BY COST_CENTER
    --        ,jobpos.position_code_desc
    --   ,case when rn_grouping = 'Leadership' then ndir.director_grouping_desc
    --    else fundept.Functional_Dept_Desc
    --    end as specialty
    --                 ,subdept.sub_functional_dept_desc as sub_specialty
    -- ,Months_Between(hrmet.Date_Id, emp.anniversary_date) AS Tenure_Months_Num
    -- ,Months_Between(hrmet.Date_Id, Emp.Hire_Date) AS Tenure_Months_Num3
    -- ,CASE WHEN empdet.Adjusted_Hire_Date > hrmet.Date_ID THEN
    --  Months_Between(hrmet.Date_Id, Anni_CTE.HR_Employee_Value_Date)
    --  ELSE Months_Between(hrmet.Date_Id, emp.Anniversary_Date) END AS Tenure_Months_Num --V1 Change
    CASE
      WHEN (hrmet.lawson_company_num) = 300 THEN date_diff(hrmet.date_id , empdet.adjusted_hire_date ,month)
      ELSE date_diff(hrmet.date_id , emp.anniversary_date,month )
    END AS tenure_months_num,
    round(date_diff(hrmet.date_id , empdet.acute_experience_start_date,day) / NUMERIC '365.25', CAST(CAST(2 as BIGNUMERIC) as INT64)) AS rn_experience_years,
    CASE
      WHEN date_diff(hrmet.date_id , empdet.acute_experience_start_date ,day)<= 150 THEN 'New Grad'
      WHEN date_diff(hrmet.date_id , empdet.acute_experience_start_date ,day) <= 365 THEN 'Less than 1 Year'
      WHEN date_diff(hrmet.date_id , empdet.acute_experience_start_date ,day) <= 731 THEN '1-2 Years'
      WHEN date_diff(hrmet.date_id , empdet.acute_experience_start_date ,day) > 731 THEN '2+ Years'
      ELSE 'Unknown'
    END AS rn_experience_bucket_years,
    ndir.director_grouping_desc,
    -- ,hrmet.functional_dept_num
    -- ,facfac.COID_Name
    -- ,facfac.LOB_Code
    -- ,facfac.LOB_Name
    -- ,facfac.Group_Name
    -- ,facfac.Division_Name
    -- ,SUM(case when hrmet.analytics_msr_sid = '80100' and hrmet.Date_Id = last_day(hrmet.Date_Id) then Metric_Numerator_Qty else 0 end) AS Headcount
    hrmet.analytics_msr_sid
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot AS hrmet
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facfac ON hrmet.coid = facfac.coid
     AND hrmet.company_code = facfac.company_code
    LEFT OUTER JOIN (
      SELECT
          hr_employee_history.employee_sid,
          hr_employee_history.hr_employee_element_date,
          hr_employee_history.hr_employee_value_date,
          hr_employee_history.employee_num,
          coalesce(min(hr_employee_history.hr_employee_element_date) OVER (PARTITION BY hr_employee_history.employee_sid, hr_employee_history.lawson_element_num ORDER BY hr_employee_history.hr_employee_element_date, hr_employee_history.sequence_num, hr_employee_history.last_update_date ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), DATE '9999-12-31') AS element_date_end
        FROM
          {{ params.param_hr_base_views_dataset_name }}.hr_employee_history
        WHERE date(hr_employee_history.valid_to_date) = '9999-12-31'
         AND hr_employee_history.lawson_element_num = 25
    ) AS anni_cte ON hrmet.employee_sid = anni_cte.employee_sid
     AND hrmet.date_id BETWEEN anni_cte.hr_employee_element_date AND anni_cte.element_date_end
    LEFT OUTER JOIN -- V1 Change
    {{ params.param_hr_base_views_dataset_name }}.department AS dept ON hrmet.dept_sid = dept.dept_sid
     AND date(dept.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.hr_dept_rollup AS hrdr ON hrmet.process_level_code = hrdr.process_level_code
     AND hrmet.coid = hrdr.coid
     AND dept.dept_code = hrdr.dept_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk ON hrmet.coid = xwalk.coid
     AND dept.dept_code = xwalk.account_unit_num
     AND hrmet.process_level_code = xwalk.process_level_code
     AND date(xwalk.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_department AS fundept ON hrmet.functional_dept_num = fundept.functional_dept_num
    LEFT OUTER JOIN -- -- Added Function/Subfunction back to view in 7/11/22
    {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department AS fd ON xwalk.dept_num = fd.dept_num
     AND xwalk.coid = fd.coid
     AND xwalk.company_code = fd.company_code
    LEFT OUTER JOIN -- -- End  Function/Subfunction
    -- Functional and Sub Functional Departments now identified by COID & Cost Center relationship within PBI Data Model
    /*LEFT JOIN EDW_PUB_VIEWS.Functional_Sub_Functional_Department subdept
    ON hrmet.Sub_Functional_Dept_Num = subdept.Sub_Functional_Dept_Num
    AND fundept.Functional_Dept_Num = subdept.Functional_Dept_Num
    AND Hrmet.COID = subdept.COID*/
    {{ params.param_hr_base_views_dataset_name }}.job_class AS jobcl ON hrmet.job_class_sid = jobcl.job_class_sid
     AND date(jobcl.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_operations AS hrops ON hrmet.process_level_code = hrops.process_level_code
    LEFT OUTER JOIN -- V2 Change
    {{ params.param_hr_base_views_dataset_name }}.job_code AS jobco ON hrmet.job_code_sid = jobco.job_code_sid
     AND date(jobco.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stas ON hrmet.auxiliary_status_sid = stas.status_sid
     AND date(stas.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stast ON hrmet.employee_status_sid = stast.status_sid
     AND date(stast.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobpos ON hrmet.position_sid = jobpos.position_sid
     AND date(jobpos.valid_to_date) = '9999-12-31'
     AND hrmet.date_id BETWEEN jobpos.eff_from_date AND jobpos.eff_to_date
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS refkt ON hrmet.key_talent_id = refkt.key_talent_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_director AS ndir ON jobco.job_code = ndir.job_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_care_position AS pct ON jobpos.position_code_desc = pct.job_title_text
    LEFT OUTER JOIN -- ----Intergrated LOB Mapping Start----------------------------------------------------------------------------------------------------------------------------------------------------   --V2 Change
    {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat1 ON hrmet.integrated_lob_id = mat1.integrated_lob_id
    LEFT OUTER JOIN -- V2 Change
    {{ params.param_hr_base_views_dataset_name }}.employee AS emp ON hrmet.employee_sid = emp.employee_sid
     AND date(emp.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.bi_employee_detail AS empdet ON hrmet.employee_sid = empdet.employee_sid
    LEFT OUTER JOIN -- -- Add to bring in the FTE_Percent  7/11/22
    (
      SELECT
          er.employee_sid,
          er.position_sid,
          er.fte_percent,
          er.date_id
        FROM
          {{ params.param_hr_base_views_dataset_name }}.employee_roster AS er
    ) AS ep ON hrmet.employee_sid = ep.employee_sid
     AND hrmet.position_sid = ep.position_sid
     AND hrmet.date_id = ep.date_id
    INNER JOIN -- -- END FTE_Percent join
    {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON hrmet.process_level_code = c.process_level_code
     AND hrmet.lawson_company_num = c.lawson_company_num
     AND c.user_id = session_user()
    INNER JOIN -- -- Added snapshot date join to only pull the most recent snapshots
    (
      SELECT
          max(fact_hr_metric_snapshot.snapshot_date) AS maxsnap
        FROM
          {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot
    ) AS snapdt ON hrmet.snapshot_date = snapdt.maxsnap
  WHERE hrmet.date_id BETWEEN '2016-01-01' AND date_sub(current_date(), interval extract(DAY from current_date()) DAY)
   AND (hrmet.analytics_msr_sid) = 80100
;
