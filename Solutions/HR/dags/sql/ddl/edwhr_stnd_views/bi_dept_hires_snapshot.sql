-- #####################################################################################
-- #                                                                                   #
-- # View Name : EDWHR_STND_VIEWS.BI_Dept_Hires                #
-- # Purpose : This will enable PSG and others to report on hiring metrics in their reporting #
-- #                                                                                   #
-- # CHANGE CONTROL:                        #
-- #                                           #
-- #                          #
-- # DATE          Developer     Change Comment                                        #
-- # 09/09/2020    Carpenter Alex   Initial Select Query                               #
-- # 09/11/2020    Shachindra Pandey View creation      #
-- #                                                               #
-- #####################################################################################
CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.bi_dept_hires_snapshot AS SELECT DISTINCT
    -- -added 9/23/22
    -- -added 9/23/22
    -- -added by Cheryl 9/23/2022
    -- -added by Cheryl 9/23/2022
    -- -added by Cheryl 9/23/2022
    -- -added by Cheryl 9/23/2022
    last_day(DATE(fhrm.date_id)) AS pe_date,
    fhrm.date_id AS hire_date,
    ff.sector_name,
    ff.group_name,
    ff.division_name,
    ff.market_name,
    ff.lob_code,
    -- Added by Stephen on 10/27/2021
    ff.sub_lob_code,
    -- Added by Stephen on 10/27/2021
    ff.cons_facility_name,
    fhrm.coid,
    ff.coid_name,
    fhrm.process_level_code,
    -- -added by Cheryl 9/23/2022
    fhrm.location_code,
    -- -added by Cheryl 9/23/2022
    xwalk.dept_num,
    dept.dept_code,
    dept.dept_name,
    fd.functional_dept_desc,
    -- -added by Cheryl 9/23/2022
    fd.sub_functional_dept_desc,
    -- -added by Cheryl 9/23/2022
    auxstat.status_code AS aux_status_code,
    empstat.status_code AS emp_status_code,
    CASE
      WHEN upper(dept_starn.dept_code) = '63110'
       AND upper(dept_starn.process_level_code) = '26624' THEN 'External'
      WHEN upper(fhrm.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition'
      WHEN lb30.employee_num IS NULL
       AND upper(fhrm.action_code) NOT LIKE '%1XFER%' THEN 'External'
      ELSE 'Internal'
    END AS hire_type,
    fhrm.action_reason_text,
    fhrm.employee_sid,
    fhrm.employee_num,
    round(date_diff(fhrm.date_id , epers.birth_date,day) / NUMERIC '365.25', CAST(CAST(2 as BIGNUMERIC) as INT64)) AS age_at_hire,
    CASE
      WHEN upper(jcl.job_class_code) = '103' THEN 'RN'
      WHEN jcd.job_code IN(
        nd.job_code
      ) THEN 'Leadership'
      WHEN jp.position_code_desc = pct.job_title_text THEN 'PCT'
      ELSE 'Non-RN'
    END AS rn_grouping_text,
    ilob.category_desc AS ilob_category_desc,
    ilob.sub_category_desc AS ilob_subcategory_desc,
    hrops.business_unit_name AS hrops_business_unit_name,
    hrops.business_unit_segment_name AS hrops_segment_name,
    kt.key_talent_group_text,
    jcl.job_class_desc,
    jcl.job_class_code,
    jcd.job_code,
    jcd.job_code_desc,
    jp.position_code_desc,
    CASE
      WHEN round(date_diff(fhrm.date_id , bied.acute_experience_start_date,day) / NUMERIC '365.25', CAST(CAST(2 as BIGNUMERIC) as INT64)) < 0 THEN CAST(0 as BIGNUMERIC)
      ELSE round(date_diff(fhrm.date_id , bied.acute_experience_start_date, day) / NUMERIC '365.25', CAST(CAST(2 as BIGNUMERIC) as INT64))
    END AS rn_experience_years,
    CASE
      WHEN date_diff(fhrm.date_id , bied.acute_experience_start_date,day) <= 150 THEN 'New Grad'
      WHEN date_diff(fhrm.date_id , bied.acute_experience_start_date,day) <= 365 THEN 'Less Than 1 Year'
      WHEN date_diff(fhrm.date_id , bied.acute_experience_start_date,day) <= 731 THEN '1-2 Years'
      WHEN date_diff(fhrm.date_id , bied.acute_experience_start_date,day) > 731 THEN '2+ Years'
      ELSE 'Unknown'
    END AS rn_experience_bucket_at_hire_years,
    CASE
      WHEN jp.position_code_desc = rnn.job_title_text
       AND upper(auxstat.status_code) = 'PRN'
       AND upper(ff.division_code) = '00017' THEN 'Normalized'
      WHEN jp.position_code_desc = rnn.job_title_text
       AND upper(auxstat.status_code) = 'TEMP'
       AND upper(ff.division_code) = '00017' THEN 'Normalized'
      ELSE 'Standard'
    END AS normalization,
    fhrm.analytics_msr_sid,
    nd.director_grouping_desc,
    ep.fte_percent AS fte_percent
  FROM
    -- added 9/23/22
    {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot AS fhrm
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON fhrm.coid = ff.coid
     AND fhrm.company_code = ff.company_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON fhrm.position_sid = jp.position_sid
     AND date(jp.valid_to_date) = '9999-12-31'
     AND fhrm.date_id BETWEEN jp.eff_from_date AND jp.eff_to_date
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk ON jp.account_unit_num = xwalk.account_unit_num
     AND jp.gl_company_num = xwalk.gl_company_num
     AND date(xwalk.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON jp.dept_sid = dept.dept_sid
     AND date(dept.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS empstat ON fhrm.employee_status_sid = empstat.status_sid
     AND date(empstat.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS auxstat ON fhrm.auxiliary_status_sid = auxstat.status_sid
     AND date(auxstat.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jcd ON fhrm.job_code_sid = jcd.job_code_sid
     AND date(jcd.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jcl ON jcd.job_class_sid = jcl.job_class_sid
     AND date(jcl.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.bi_employee_detail AS bied ON fhrm.employee_sid = bied.employee_sid
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS lb30 ON fhrm.employee_num = lb30.employee_num
     AND DATE(fhrm.date_id - INTERVAL 30 DAY) = lb30.date_id
     AND (lb30.analytics_msr_sid) = 80100
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept_starn ON lb30.dept_sid = dept_starn.dept_sid
     AND date(dept_starn.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS epers ON fhrm.employee_sid = epers.employee_sid
     AND date(epers.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_director AS nd ON jcd.job_code = nd.job_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_care_position AS pct ON jp.position_code_desc = pct.job_title_text
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_rn_normalization AS rnn ON jp.position_code_desc = rnn.job_title_text
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS ilob ON fhrm.integrated_lob_id = ilob.integrated_lob_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS kt ON fhrm.key_talent_id = kt.key_talent_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_operations AS hrops ON fhrm.process_level_code = hrops.process_level_code
    LEFT OUTER JOIN -- - Added Function/Subfunction back to view in 9/23/22
    {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department AS fd ON xwalk.dept_num = fd.dept_num
     AND xwalk.coid = fd.coid
     AND xwalk.company_code = fd.company_code
    LEFT OUTER JOIN -- -- End  Function/Subfunction
    -- -- Add to bring in the FTE_Percent 9/23/22
    (
      SELECT
          er.employee_sid,
          er.position_sid,
          er.fte_percent,
          er.date_id
        FROM
          {{ params.param_hr_base_views_dataset_name }}.employee_roster AS er
    ) AS ep ON fhrm.employee_sid = ep.employee_sid
     AND fhrm.position_sid = ep.position_sid
     AND fhrm.date_id = ep.date_id
    INNER JOIN -- -- END FTE_Percent join
    {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON fhrm.process_level_code = c.process_level_code
     AND fhrm.lawson_company_num = c.lawson_company_num
     AND c.user_id = session_user()
    INNER JOIN -- -- Added snapshot date join to only pull the most recent snapshots
    (
      SELECT
          max(fact_hr_metric_snapshot.snapshot_date) AS maxsnap
        FROM
          {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot
    ) AS snapdt ON fhrm.snapshot_date = snapdt.maxsnap
  WHERE (fhrm.analytics_msr_sid) = 80200
   AND fhrm.date_id BETWEEN '2016-01-01' AND date_sub(current_date(), interval extract(DAY from current_date()) DAY)
   AND fhrm.action_code IN(
    -- --default value 2016-01-01;
    '1HIREAPPL', '1REHIRE', '1XFERPOS', '1XFEREIN-S', '1XFERINT'
  )
;
