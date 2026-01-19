-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/EDWHR_BI_Views/facttransfers_daily.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.facttransfers_daily AS SELECT
    hrmet.coid,
    max(concat(coalesce(hrdr.coid, '00000'), hrdr.process_level_code, coalesce(hrdr.dept_code, '00000'), hrdr.lawson_company_num)) AS pl_uid,
    hrdr.cost_center,
    hrmet.date_id AS pe_date,
    stas.status_code,
    jobco.job_code_desc,
    hrmet.employee_num,
    hrmet.employee_sid,
    CAST((date_diff(hrmet.date_id,emppers.birth_date,day)) /365.25 as FLOAT64) AS age_at_transfer,
    hrmet.key_talent_id,
    hrmet.action_code,
    hrmet.action_reason_text,
    hrmet.position_sid,
    concat(hrmet.position_sid, jobpos.eff_to_date) AS position_key,
    ep.schedule_work_code,
    hrc.hr_code_desc AS schedule_work_code_desc,
    ep.fte_percent AS fte_percent,
    max(CASE
      WHEN upper(jobcl.job_class_code) = '103' THEN 'RN'
      WHEN jobco.job_code IN(
        ndir.job_code
      ) THEN 'Leadership'
      WHEN jobpos.position_code_desc = pct.job_title_text THEN 'PCT'
      ELSE 'Non-RN'
    END) AS rn_grouping,
     CAST(date_diff(hrmet.date_id,empdet.acute_experience_start_date,day) / 365.25 as FLOAT64) AS rn_xp_date,
    max(CASE
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date,day) <= 150 THEN 'New Grad'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date,day) <= 365 THEN 'Less than 1 Year'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date,day) <= 731 THEN '1-2 Years'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date,day) > 731 THEN '2+ Years'
      ELSE 'Unknown'
    END) AS rn_xp_bucket_at_transfer,
    hrmet.process_level_code,
    hrmet.integrated_lob_id,
    max(CASE
      WHEN (upper(jobpos.position_code_desc) LIKE '%PRN I'
       OR upper(jobpos.position_code_desc) LIKE '%PRN')
       AND upper(jobcl.job_class_code) = '103' THEN 'PRN Tier 1'
      WHEN upper(jobpos.position_code_desc) LIKE '%PRN II'
       AND upper(jobcl.job_class_code) = '103' THEN 'PRN Tier 2'
      WHEN upper(jobpos.position_code_desc) LIKE '%PRN III'
       AND upper(jobcl.job_class_code) = '103' THEN 'PRN Tier 3'
      ELSE CAST(NULL as STRING)
    END) AS prn_tier,
    max(CASE
      WHEN stas.status_code IN(
        'FT', 'PT'
      )
       OR (upper(jobpos.position_code_desc) LIKE '%PRN II'
       OR upper(jobpos.position_code_desc) LIKE '%PRN III') THEN 'Core'
      ELSE 'Flex'
    END) AS workforce_category,
    hrmet.source_system_code,
    CAST(sum(CASE
      WHEN hrmet.analytics_msr_sid = 80700 THEN hrmet.metric_numerator_qty
      ELSE 0
    END) as FLOAT64) AS transfers
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS hrmet
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facfac ON hrmet.coid = facfac.coid
     AND hrmet.company_code = facfac.company_code
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_department AS fundept ON hrmet.functional_dept_num = fundept.functional_dept_num
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON hrmet.dept_sid = dept.dept_sid
     AND date(dept.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_bi_views_dataset_name }}.hr_dept_rollup AS hrdr ON hrmet.coid = hrdr.coid
     AND hrmet.process_level_code = hrdr.process_level_code
     AND hrmet.dept_sid = hrdr.dept_sid
     AND hrmet.lawson_company_num = hrdr.lawson_company_num
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk ON hrmet.coid = xwalk.coid
     AND hrmet.process_level_code = xwalk.process_level_code
     AND dept.dept_code = xwalk.account_unit_num
     AND date(xwalk.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jobcl ON hrmet.job_class_sid = jobcl.job_class_sid
     AND date(jobcl.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jobco ON hrmet.job_code_sid = jobco.job_code_sid
     AND date(jobco.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_location AS reflo ON hrmet.location_code = reflo.location_code
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
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.bi_employee_detail AS empdet ON empdet.employee_sid = hrmet.employee_sid
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_rn_normalization AS norm ON jobpos.position_code_desc = norm.job_title_text
     AND jobcl.job_class_desc = norm.skill_mix_desc
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
    {{ params.param_hr_base_views_dataset_name }}.employee_position AS ep ON hrmet.employee_sid = ep.employee_sid
     AND hrmet.position_sid = ep.position_sid
     AND date(ep.valid_to_date) = '9999-12-31'
     AND hrmet.date_id BETWEEN ep.eff_from_date AND ep.eff_to_date
    LEFT OUTER JOIN --  7/21/2021 Added by Stephen for Schedule_Work_Code (Shift Code)
    {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS hrc ON ep.schedule_work_code = hrc.hr_code
     AND upper(hrc.hr_type_code) = 'WS'
     AND upper(hrc.active_ind) = 'A'
    LEFT OUTER JOIN --  7/21/2021 Added by Stephen for Schedule_Work_Code_Desc (Shift Description)
    {{ params.param_pub_views_dataset_name }}.sub_functional_department AS subf ON hrmet.sub_functional_dept_num = subf.sub_functional_dept_num
     AND hrmet.functional_dept_num = fundept.functional_dept_num
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS emppers ON hrmet.employee_sid = emppers.employee_sid
     AND date(emppers.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot AS lb30 ON hrmet.employee_num = lb30.employee_num
     AND DATE(hrmet.date_id - INTERVAL 30 DAY) = lb30.date_id
     AND lb30.analytics_msr_sid = 80100
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept_starn ON lb30.dept_sid = dept_starn.dept_sid
     AND date(dept_starn.valid_to_date) = '9999-12-31'
  WHERE hrmet.date_id BETWEEN '2016-01-01' AND current_date('US/Central')
   AND hrmet.analytics_msr_sid = 80700
  GROUP BY 1, upper(concat(coalesce(hrdr.coid, '00000'), hrdr.process_level_code, coalesce(hrdr.dept_code, '00000'), hrdr.lawson_company_num)), 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, upper(CASE
    WHEN upper(jobcl.job_class_code) = '103' THEN 'RN'
    WHEN jobco.job_code IN(
      ndir.job_code
    ) THEN 'Leadership'
    WHEN jobpos.position_code_desc = pct.job_title_text THEN 'PCT'
    ELSE 'Non-RN'
  END), 19, upper(CASE
    WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date,day) <= 150 THEN 'New Grad'
    WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date,day) <= 365 THEN 'Less than 1 Year'
    WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date,day) <= 731 THEN '1-2 Years'
    WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date,day) > 731 THEN '2+ Years'
    ELSE 'Unknown'
  END), 21, 22, upper(CASE
    WHEN (upper(jobpos.position_code_desc) LIKE '%PRN I'
     OR upper(jobpos.position_code_desc) LIKE '%PRN')
     AND upper(jobcl.job_class_code) = '103' THEN 'PRN Tier 1'
    WHEN upper(jobpos.position_code_desc) LIKE '%PRN II'
     AND upper(jobcl.job_class_code) = '103' THEN 'PRN Tier 2'
    WHEN upper(jobpos.position_code_desc) LIKE '%PRN III'
     AND upper(jobcl.job_class_code) = '103' THEN 'PRN Tier 3'
    ELSE CAST(NULL as STRING)
  END), upper(CASE
    WHEN stas.status_code IN(
      'FT', 'PT'
    )
     OR (upper(jobpos.position_code_desc) LIKE '%PRN II'
     OR upper(jobpos.position_code_desc) LIKE '%PRN III') THEN 'Core'
    ELSE 'Flex'
  END), 25
;
--  transfers
