

CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.bi_dept_termination_snapshot AS SELECT
    -- -added 9/23/22
    -- -added 9/23/22
    -- -added 9/23/22
    -- added 9/23/22
    -- added 9/23/22
    --  removing the last_day and just showing the actual date ID (AC)
    last_day(DATE(hrmet.date_id)) AS pe_date,
    hrmet.date_id AS term_date,
    facfac.sector_name,
    facfac.group_name,
    facfac.division_name,
    facfac.market_name,
    facfac.lob_code,
    facfac.sub_lob_code,
    -- -added by Cheryl 9/23/2022
    facfac.cons_facility_name,
    hrmet.coid,
    facfac.coid_name,
    hrmet.process_level_code,
    -- -added by Cheryl 9/23/2022
    hrmet.location_code,
    -- -added by Cheryl 9/23/2022
    dept.dept_code,
    -- -added by Cheryl 9/23/2022
    xwalk.dept_num,
    dept.dept_name,
    fd.functional_dept_desc,
    -- -added by Cheryl 9/23/2022
    fd.sub_functional_dept_desc,
    -- -added by Cheryl 9/23/2022
    stas.status_code AS aux_status_code,
    stast.status_code AS emp_status_code,
    CASE
      WHEN upper(hrmet.action_code) = '1TERMPEND'
       AND hrmet.action_reason_text NOT IN(
        'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR'
      ) THEN 'External'
      WHEN upper(hrmet.action_code) LIKE '%1XFER%' THEN 'Internal'
      WHEN upper(hrmet.action_code) = '1TERMPEND'
       AND hrmet.action_reason_text IN(
        'TV-FCLTRNS', 'TV-RELOHCA'
      ) THEN 'Internal'
      ELSE 'Other'
    END AS term_type_text,
    CASE
      WHEN hrmet.action_reason_text IN(
        'TV-NVRSTRT', 'TI-OUTSRCE', 'TV-CHAP', 'TV-NOSHOW', 'TV-OUTSRCE', 'TV-RESIDEN', 'TI-DIVEST', 'TV-ENDASSG', 'TI-ENDASSG', 'TV-NOSTART'
      ) THEN 'Other'
      ELSE 'Standard'
    END AS term_governance_group_desc,
    hrmet.action_reason_text,
    hrmet.employee_sid,
    hrmet.employee_num,
    CASE
      WHEN upper(jobcl.job_class_code) = '103' THEN 'RN'
      WHEN jobco.job_code IN(
        ndir.job_code
      ) THEN 'Leadership'
      WHEN jobpos.position_code_desc = pct.job_title_text THEN 'PCT'
      ELSE 'Non-RN'
    END AS rn_grouping_text,
    ilob.category_desc AS ilob_category_desc,
    -- V1 Change
    ilob.sub_category_desc AS ilob_subcategory_desc,
    -- V1 Change
    hrops.business_unit_name AS hrops_business_unit_name,
    -- V1 Change
    hrops.business_unit_segment_name AS hrops_segment_name,
    -- V1 Change
    refkt.key_talent_group_text,
    jobcl.job_class_code,
    jobcl.job_class_desc,
    jobco.job_code_desc,
    jobpos.position_code_desc,
    round(date_diff(hrmet.date_id , empdet.acute_experience_start_date,day) / NUMERIC '365.25', CAST(CAST(2 as BIGNUMERIC) as INT64)) AS rn_experience_years,
    CASE
      WHEN date_diff(hrmet.date_id , empdet.acute_experience_start_date,day) <= 150 THEN 'New Grad'
      WHEN date_diff(hrmet.date_id , empdet.acute_experience_start_date,day) <= 365 THEN 'Less than 1 Year'
      WHEN date_diff(hrmet.date_id , empdet.acute_experience_start_date,day) <= 731 THEN '1-2 Years'
      WHEN date_diff(hrmet.date_id , empdet.acute_experience_start_date,day) > 731 THEN '2+ Years'
      ELSE 'Unknown'
    END AS rn_experience_bucket_at_term_years,
    CASE
      WHEN empdet.adjusted_hire_date > hrmet.date_id THEN date_diff(hrmet.date_id, anni_cte.hr_employee_value_date ,month)
      ELSE date_diff(hrmet.date_id , empdet.adjusted_hire_date ,month)
    END AS tenure_months_num,
    -- V1 Change
    hrmet.analytics_msr_sid,
    ndir.director_grouping_desc,
    ep.fte_percent AS fte_percent
  FROM
    -- added 9/23/22
    {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot AS hrmet
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON hrmet.dept_sid = dept.dept_sid
     AND date(dept.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.hr_dept_rollup AS hrdr ON hrmet.coid = hrdr.coid
     AND hrmet.process_level_code = hrdr.process_level_code
     AND dept.dept_code = hrdr.dept_code
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
    {{ params.param_pub_views_dataset_name }}.functional_department AS fundept ON hrmet.functional_dept_num = fundept.functional_dept_num
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk ON hrmet.coid = xwalk.coid
     AND hrmet.process_level_code = xwalk.process_level_code
     AND dept.dept_code = xwalk.account_unit_num
     AND date(xwalk.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN -- -- Added Function/Subfunction back to view in 9/23/22
    {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department AS fd ON xwalk.dept_num = fd.dept_num
     AND xwalk.coid = fd.coid
     AND xwalk.company_code = fd.company_code
    LEFT OUTER JOIN -- -- End  Function/Subfunction
    {{ params.param_hr_base_views_dataset_name }}.job_class AS jobcl ON hrmet.job_class_sid = jobcl.job_class_sid
     AND date(jobcl.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_operations AS hrops ON hrmet.process_level_code = hrops.process_level_code
    LEFT OUTER JOIN -- V1 Change
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
    LEFT OUTER JOIN -- ----Integrated LOB Mapping Start----------------------------------------------------------------------------------------------------------------------------------------------------   --V1 Change
    {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS ilob ON hrmet.integrated_lob_id = ilob.integrated_lob_id
    LEFT OUTER JOIN -- V1 Change
    {{ params.param_hr_base_views_dataset_name }}.bi_employee_detail AS empdet ON hrmet.employee_sid = empdet.employee_sid
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
  WHERE hrmet.date_id BETWEEN '2016-01-01' AND current_date()
   AND (hrmet.analytics_msr_sid) = 80300
;
