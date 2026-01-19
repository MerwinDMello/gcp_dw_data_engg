create or replace view {{ params.param_hr_bi_views_dataset_name }}.factheadcount_weekly as SELECT
    last_day(hrmet.date_id, WEEK) AS pe_date,
    hrmet.coid,
    concat(coalesce(hrdr.coid, '00000'), hrdr.process_level_code, coalesce(hrdr.dept_code, '00000'), hrdr.lawson_company_num) AS pl_uid,
    stas.status_code AS aux_status,
    stast.status_code AS emp_status,
    hrmet.employee_num,
    hrmet.employee_sid,
    hrmet.key_talent_id,
    hrmet.position_sid,
    ep.fte_percent AS fte_percent,
    concat(trim(cast(hrmet.position_sid as string)), jobpos.eff_to_date) AS position_key,
    coalesce(anniv_hist.anniversary_date, emp.anniversary_date) AS anniversary_date,
    ep.work_schedule_code AS schedule_work_code,
    hrc.hr_code_desc AS schedule_work_code_desc,
    CAST(date_diff(hrmet.date_id, emppers.birth_date, DAY) / NUMERIC '365.25' as FLOAT64) AS age,
    CASE
      WHEN upper(jobcl.job_class_code) = '103' THEN 'RN'
      WHEN jobco.job_code IN(
        ndir.job_code
      ) THEN 'Leadership'
      WHEN upper(trim(jobpos.position_code_desc)) = upper(trim(pct.job_title_text)) THEN 'PCT'
      ELSE 'Non-RN'
    END AS rn_grouping,
    hrdr.cost_center,
    hrmet.process_level_code,
    hrmet.integrated_lob_id,
    CASE
      WHEN hrmet.lawson_company_num = 300 THEN date_diff(CAST(hrmet.date_id as DATE), CAST(emp.adjusted_hire_date as DATE),month)
      ELSE date_diff(CAST(hrmet.date_id as DATE), CAST(coalesce(anniv_hist.anniversary_date, emp.anniversary_date) as DATE),month)
    END AS tenure,
    CAST(date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) / NUMERIC '365.25' as FLOAT64) AS rn_xp_date,
    CASE
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 150 THEN 'New Grad'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 365 THEN 'Less than 1 Year'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 731 THEN '1-2 Years'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) > 731 THEN '2+ Years'
      ELSE 'Unknown'
    END AS rn_xp_bucket,
    CASE
      WHEN upper(trim(jobpos.position_code_desc)) = upper(trim(norm.job_title_text))
       AND upper(stas.status_code) = 'PRN'
       AND upper(facfac.division_name) = 'NORTH TEXAS DIVISION' THEN 'Normalized'
      WHEN jobpos.position_code_desc = norm.job_title_text
       AND upper(stas.status_code) = 'TEMP'
       AND upper(facfac.division_name) = 'NORTH TEXAS DIVISION' THEN 'Normalized'
      ELSE 'Standard'
    END AS normalization,
    CASE
      WHEN (upper(jobpos.position_code_desc) LIKE '%PRN I'
       OR upper(jobpos.position_code_desc) LIKE '%PRN')
       AND upper(jobcl.job_class_code) = '103' THEN 'PRN Tier 1'
      WHEN upper(jobpos.position_code_desc) LIKE '%PRN II'
       AND upper(jobcl.job_class_code) = '103' THEN 'PRN Tier 2'
      WHEN upper(jobpos.position_code_desc) LIKE '%PRN III'
       AND upper(jobcl.job_class_code) = '103' THEN 'PRN Tier 3'
      ELSE CAST(NULL as STRING)
    END AS prn_tier,
    CASE
      WHEN stas.status_code IN(
        'FT', 'PT'
      )
       OR (upper(jobpos.position_code_desc) LIKE '%PRN II'
       OR upper(jobpos.position_code_desc) LIKE '%PRN III') THEN 'Core'
      ELSE 'Flex'
    END AS workforce_category,
    hrmet.source_system_code
  FROM
   {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS hrmet 
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facfac ON hrmet.coid = facfac.coid
     AND hrmet.company_code = facfac.company_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON hrmet.dept_sid = dept.dept_sid
     AND DATE(dept.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_bi_views_dataset_name }}.hr_dept_rollup AS hrdr ON hrmet.process_level_code = hrdr.process_level_code
     AND hrmet.coid = hrdr.coid
     AND hrmet.dept_sid = hrdr.dept_sid
     AND hrmet.lawson_company_num = hrdr.lawson_company_num
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_department AS fundept ON hrmet.functional_dept_num = fundept.functional_dept_num
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jobcl ON hrmet.job_class_sid = jobcl.job_class_sid
     AND DATE(jobcl.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jobco ON hrmet.job_code_sid = jobco.job_code_sid
     AND DATE(jobco.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_location AS reflo ON hrmet.location_code = reflo.location_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stas ON hrmet.auxiliary_status_sid = stas.status_sid
     AND DATE(stas.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stast ON hrmet.employee_status_sid = stast.status_sid
     AND DATE(stast.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobpos ON hrmet.position_sid = jobpos.position_sid
     AND DATE(jobpos.valid_to_date) = DATE('9999-12-31')
     AND hrmet.date_id BETWEEN jobpos.eff_from_date AND jobpos.eff_to_date
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emp ON hrmet.employee_sid = emp.employee_sid
     AND DATE(emp.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS refkt ON hrmet.key_talent_id = refkt.key_talent_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_director AS ndir ON jobco.job_code = ndir.job_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_care_position AS pct ON upper(trim(jobpos.position_code_desc)) = upper(trim(pct.job_title_text))
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_rn_normalization AS norm ON upper(trim(jobpos.position_code_desc)) = upper(trim(norm.job_title_text))
     AND upper(trim(jobcl.job_class_desc)) = upper(trim(norm.skill_mix_desc))
     AND upper(CASE
      WHEN stas.status_code IN(
        --  10/22/2020 Added by Thomas to correct duplication
        'FT', 'PT'
      ) THEN 'FT/PT'
      WHEN stas.status_code IN(
        'PRN', 'TEMP'
      ) THEN 'PRN/TEMP'
    END) = upper(norm.auxiliary_status_code)
    LEFT OUTER JOIN --  10/22/2020 Added by Thomas to correct duplication
    {{ params.param_pub_views_dataset_name }}.sub_functional_department AS subf ON hrmet.sub_functional_dept_num = subf.sub_functional_dept_num
     AND hrmet.functional_dept_num = fundept.functional_dept_num
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS emppers ON hrmet.employee_sid = emppers.employee_sid
     AND DATE(emppers.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.bi_employee_detail AS empdet ON hrmet.employee_sid = empdet.employee_sid
    LEFT OUTER JOIN --  Removed join due to causing duplicate records
    /*LEFT OUTER JOIN EDWHR_BASE_VIEWS.Employee_Position ep -- 7/6/2021 Added by Stephen for Schedule_Work_Code (Shift Code)
    				ON hrmet.employee_sid = ep.Employee_SID
    				AND hrmet.position_sid = ep.position_sid
    				AND ep.Valid_To_Date = DATE('9999-12-31')
    				AND hrmet.date_id BETWEEN ep.Eff_From_Date AND ep.Eff_To_Date*/
    --  5/20/2022 Added by Stephen to correct duplicate records
    (
      SELECT
          er.employee_sid,
          er.position_sid,
          er.work_schedule_code,
          er.fte_percent,
          er.date_id
        FROM
         {{ params.param_hr_base_views_dataset_name }}.employee_roster AS er
    ) AS ep ON hrmet.employee_sid = ep.employee_sid
     AND hrmet.position_sid = ep.position_sid
     AND hrmet.date_id = ep.date_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS hrc ON ep.work_schedule_code = hrc.hr_code
     AND upper(hrc.hr_type_code) = 'WS'
     AND upper(hrc.active_ind) = 'A'
    LEFT OUTER JOIN --  7/6/2021 Added by Stephen for Schedule_Work_Code_Desc (Shift Description)
    -- 5/12/2022 Added by Stephen to correct historical Anniversary Date for employees who previously termed with a rehire action
    (
      SELECT
          hreh.employee_sid,
          hreh.employee_num,
          hreh.hr_employee_element_date AS eff_from,
          CASE
            WHEN lead(hreh.hr_employee_element_date, 1) OVER (PARTITION BY hreh.employee_num, hreh.lawson_element_num ORDER BY hreh.lawson_element_num, hreh.hr_employee_element_date) - 1 IS NULL THEN parse_date('%Y-%m-%d', '9999-12-31')
            ELSE lead(hreh.hr_employee_element_date, 1) OVER (PARTITION BY hreh.employee_num, hreh.lawson_element_num ORDER BY hreh.lawson_element_num, hreh.hr_employee_element_date) - 1
          END AS eff_to,
          hreh.lawson_element_num,
          rle.lawson_element_desc,
          hreh.hr_employee_value_date AS anniversary_date
        FROM
          {{ params.param_hr_views_dataset_name }}.hr_employee_history AS hreh
          LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.ref_lawson_element AS rle ON hreh.lawson_element_num = rle.lawson_element_num
        WHERE DATE(hreh.valid_to_date) = DATE('9999-12-31')
         AND hreh.lawson_element_num = 25
        QUALIFY row_number() OVER (PARTITION BY hreh.employee_num, hreh.lawson_element_num, eff_from ORDER BY eff_from, hreh.sequence_num DESC) = 1
    ) AS anniv_hist ON hrmet.employee_sid = anniv_hist.employee_sid
     AND hrmet.date_id BETWEEN anniv_hist.eff_from AND anniv_hist.eff_to
  WHERE hrmet.analytics_msr_sid = 80100
   AND hrmet.date_id > date_add(current_date('US/Central'), interval -25 MONTH)
   AND hrmet.date_id = last_day(hrmet.date_id, WEEK)