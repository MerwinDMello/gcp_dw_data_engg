-- Translation time: 2023-04-12T01:17:05.938700Z
-- Translation job ID: 62432306-4f81-40cb-aaa3-b1e8779e3176
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/EDWHR_STND_VIEWS/bi_dept_vacancies.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.bi_dept_vacancies AS SELECT
    -- -9/23/2022
    -- -9/23/2022
    -- -9/23/2022
    -- -9/23/2022
    -- -9/23/2022
    -- -9/23/2022
    -- -9/23/2022
    -- -added by Cheryl 9/23/2022
    -- -added by Cheryl 9/23/2022
    -- -added by Cheryl 9/23/2022
    CASE
      WHEN hrmet.date_id = date_sub(current_date(), interval 1 DAY) THEN hrmet.date_id
      ELSE last_day(DATE(hrmet.date_id))
    END AS pe_date,
    hrmet.date_id,
    hrmet.process_level_code,
    hrmet.lawson_company_num,
    facfac.sector_name,
    -- Added by Cheryl on 9/23/2022
    facfac.group_name,
    -- Added by Cheryl on 9/23/2022
    facfac.division_name,
    -- Added by Cheryl on 9/23/2022
    facfac.market_name,
    -- Added by Cheryl on 9/23/2022
    facfac.lob_code,
    -- Added by Cheryl on 9/23/2022
    facfac.sub_lob_code,
    -- Added by Cheryl on 9/23/2022
    facfac.cons_facility_name,
    hrmet.coid,
    facfac.coid_name,
    dept.dept_code,
    -- Added by Cheryl on 9/23/2022
    xwalk.dept_num,
    dept.dept_name,
    fd.functional_dept_desc,
    -- -added by Cheryl 9/23/2022
    fd.sub_functional_dept_desc,
    -- -added by Cheryl 9/23/2022
    jobcl.job_class_code,
    jobcl.job_class_desc,
    jobco.job_code,
    jobco.job_code_desc,
    jobpos.position_code_desc,
    max(CASE
      WHEN upper(jobcl.job_class_code) = '103' THEN 'RN'
      WHEN jobco.job_code IN(
        ndir.job_code
      ) THEN 'Leadership'
      WHEN jobpos.position_code_desc = pct.job_title_text THEN 'PCT'
      ELSE 'Non-RN'
    END) AS rn_grouping_text,
    max(CASE
      WHEN trim(stas.status_code) = '01' THEN 'FT'
      WHEN trim(stas.status_code) = '02' THEN 'PT'
      WHEN trim(stas.status_code) = '03' THEN 'PRN'
      WHEN trim(stas.status_code) = '04' THEN 'TEMP'
      ELSE CAST(NULL as STRING)
    END) AS emp_status,
    ilob.category_desc AS ilob_category_desc,
    ilob.sub_category_desc AS ilob_subcategory_desc,
    hrops.business_unit_name AS hrops_business_unit_name,
    hrops.business_unit_segment_name AS hrops_segment_name,
    ndir.director_grouping_desc,
    ep.fte_percent AS fte_percent,
    -- added 9/23/22
    sum(CASE
      WHEN (analytics_msr_sid) = 80400 THEN hrmet.metric_numerator_qty
      ELSE 0
    END) AS vacancies
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS hrmet
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON hrmet.dept_sid = dept.dept_sid
     AND date(dept.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.hr_dept_rollup AS hrdr ON hrmet.coid = hrdr.coid
     AND hrmet.process_level_code = hrdr.process_level_code
     AND dept.dept_code = hrdr.dept_code
     AND hrmet.lawson_company_num = hrdr.lawson_company_num
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jobco ON hrmet.job_code_sid = jobco.job_code_sid
     AND date(jobco.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facfac ON hrmet.coid = facfac.coid
     AND hrmet.company_code = facfac.company_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk ON hrmet.coid = xwalk.coid
     AND hrmet.process_level_code = xwalk.process_level_code
     AND dept.dept_code = xwalk.account_unit_num
     AND date(xwalk.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_department AS fundept ON hrmet.functional_dept_num = fundept.functional_dept_num
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jobcl ON hrmet.job_class_sid = jobcl.job_class_sid
     AND date(jobcl.valid_to_date) = '9999-12-31'
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS req ON hrmet.requisition_sid = req.requisition_sid
     AND date(req.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_director AS ndir ON jobco.job_code = ndir.job_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_position AS reqpos ON req.requisition_sid = reqpos.requisition_sid
     AND date(reqpos.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobpos ON reqpos.position_sid = jobpos.position_sid
     AND date(jobpos.valid_to_date) = '9999-12-31'
     AND hrmet.date_id BETWEEN jobpos.eff_from_date AND jobpos.eff_to_date
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_care_position AS pct ON jobpos.position_code_desc = pct.job_title_text
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS ilob ON hrmet.integrated_lob_id = ilob.integrated_lob_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS kt ON hrmet.key_talent_id = kt.key_talent_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_operations AS hrops ON hrmet.process_level_code = hrops.process_level_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stas ON req.application_status_sid = stas.status_sid
     AND date(stas.valid_to_date) = '9999-12-31'
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
    LEFT OUTER JOIN -- -- END FTE_Percent join
    -- - Added Function/Subfunction back to view in 9/23/22
    {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department AS fd ON xwalk.dept_num = fd.dept_num
     AND xwalk.coid = fd.coid
     AND xwalk.company_code = fd.company_code
  WHERE hrmet.date_id BETWEEN date_add(last_day(date_add(current_date(), interval -37 MONTH)), interval 1 DAY) AND current_date()
   AND (hrmet.date_id = last_day(DATE(hrmet.date_id))
   OR hrmet.date_id = date_sub(current_date(), interval 1 DAY))
   AND (hrmet.analytics_msr_sid) = 80400
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, upper(CASE
    WHEN upper(jobcl.job_class_code) = '103' THEN 'RN'
    WHEN jobco.job_code IN(
      ndir.job_code
    ) THEN 'Leadership'
    WHEN jobpos.position_code_desc = pct.job_title_text THEN 'PCT'
    ELSE 'Non-RN'
  END), upper(CASE
    WHEN trim(stas.status_code) = '01' THEN 'FT'
    WHEN trim(stas.status_code) = '02' THEN 'PT'
    WHEN trim(stas.status_code) = '03' THEN 'PRN'
    WHEN trim(stas.status_code) = '04' THEN 'TEMP'
    ELSE CAST(NULL as STRING)
  END), 26, 27, 28, 29, 30, 31
;
-- -- End  Function/Subfunction
--  Rolling 36 months
