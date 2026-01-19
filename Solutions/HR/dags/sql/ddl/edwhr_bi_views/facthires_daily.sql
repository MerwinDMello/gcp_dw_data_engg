create or replace view {{ params.param_hr_bi_views_dataset_name }}.facthires_daily as SELECT
    hrmet.coid,
    max(concat(coalesce(hrdr.coid, '00000'), hrdr.process_level_code, coalesce(hrdr.dept_code, '00000'), hrdr.lawson_company_num)) AS pl_uid,
    concat(coalesce(trim(cast(hrmet.requisition_sid as string)), '00000'), coalesce(trim(cast(offer.candidate_profile_sid as string)), '00000')) AS req_app_uid,
    hrdr.cost_center,
    hrmet.date_id AS pe_date,
    stas.status_code,
    ep.fte_percent AS fte_percent,
    jobco.job_code_desc,
    ep.schedule_work_code,
    hrc.hr_code_desc AS schedule_work_code_desc,
    max(CASE
      WHEN upper(dept_starn.dept_code) = '63110'
       AND upper(dept_starn.process_level_code) = '26624' THEN 'External'
      WHEN upper(hrmet.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition'
      WHEN lb30.employee_num IS NULL
       AND upper(hrmet.action_code) NOT LIKE '%1XFER%' THEN 'External'
      ELSE 'Internal'
    END) AS hire_type,
    hrmet.employee_num,
    hrmet.employee_sid,
    hrmet.requisition_sid,
    offer.recruitment_requisition_sid,
    CAST(date_diff(hrmet.date_id, emppers.birth_date, DAY) / NUMERIC '365.25' as FLOAT64) AS age_at_hire,
    hrmet.key_talent_id,
    hrmet.action_code,
    hrmet.action_reason_text,
    hrmet.position_sid,
    concat(trim(cast(hrmet.position_sid as string)), jobpos.eff_to_date) AS position_key,
    max(CASE
      WHEN upper(jobcl.job_class_code) = '103' THEN 'RN'
      WHEN jobco.job_code IN(
        ndir.job_code
      ) THEN 'Leadership'
      WHEN jobpos.position_code_desc = pct.job_title_text THEN 'PCT'
      ELSE 'Non-RN'
    END) AS rn_grouping,
    CAST(date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) / NUMERIC '365.25' as FLOAT64) AS rn_xp_date,
    max(CASE
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 150 THEN 'New Grad'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 365 THEN 'Less than 1 Year'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 731 THEN '1-2 Years'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) > 731 THEN '2+ Years'
      ELSE 'Unknown'
    END) AS rn_xp_bucket_at_hire,
    hrmet.process_level_code,
    hrmet.integrated_lob_id,
    r.requisition_open_date,
    offer.extend_date,
    offer.accept_date,
    offer.start_date,
    max(CASE
      WHEN jobpos.position_code_desc = norm.job_title_text
       AND upper(stas.status_code) = 'PRN'
       AND upper(facfac.division_name) = 'NORTH TEXAS DIVISION' THEN 'Normalized'
      WHEN jobpos.position_code_desc = norm.job_title_text
       AND upper(stas.status_code) = 'TEMP'
       AND upper(facfac.division_name) = 'NORTH TEXAS DIVISION' THEN 'Normalized'
      ELSE 'Standard'
    END) AS normalization,
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
    CAST(date_diff(hrmet.date_id, r.requisition_open_date, DAY) as FLOAT64) AS tts,
    hrmet.source_system_code,
    max(concat(recuser.last_name, ', ', recuser.first_name)) AS recruiter_name,
    recuser.employee_34_login_code AS recruiter_34,
    CAST(sum(CASE
      WHEN hrmet.analytics_msr_sid = 80200 THEN hrmet.metric_numerator_qty
      ELSE 0
    END) as FLOAT64) AS hires
  FROM
   {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS hrmet 
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facfac ON hrmet.coid = facfac.coid
     AND hrmet.company_code = facfac.company_code
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_department AS fundept ON hrmet.functional_dept_num = fundept.functional_dept_num
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON hrmet.dept_sid = dept.dept_sid
     AND DATE(dept.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdr ON hrmet.coid = hrdr.coid
     AND hrmet.process_level_code = hrdr.process_level_code
     AND hrmet.dept_sid = hrdr.dept_sid
     AND hrmet.lawson_company_num = hrdr.lawson_company_num
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
    {{ params.param_pub_views_dataset_name }}.sub_functional_department AS subf ON hrmet.sub_functional_dept_num = subf.sub_functional_dept_num
     AND hrmet.functional_dept_num = fundept.functional_dept_num
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS emppers ON hrmet.employee_sid = emppers.employee_sid
     AND DATE(emppers.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN (
      SELECT
          lb.employee_num,
          lb.dept_sid,
          lb.date_id,
          lb.analytics_msr_sid
        FROM
         {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS lb
        WHERE lb.analytics_msr_sid = 80100
    ) AS lb30 ON hrmet.employee_num = lb30.employee_num
     AND DATE(hrmet.date_id - INTERVAL 30 DAY) = lb30.date_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept_starn ON lb30.dept_sid = dept_starn.dept_sid
     AND DATE(dept_starn.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_position AS ep ON hrmet.employee_sid = ep.employee_sid
     AND hrmet.position_sid = ep.position_sid
     AND DATE(ep.valid_to_date) = DATE('9999-12-31')
     AND hrmet.date_id BETWEEN ep.eff_from_date AND ep.eff_to_date
    LEFT OUTER JOIN --  7/19/2021 Added by Stephen for Schedule_Work_Code (Shift Code)
   {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS hrc ON ep.schedule_work_code = hrc.hr_code
     AND upper(hrc.hr_type_code) = 'WS'
     AND upper(hrc.active_ind) = 'A'
    LEFT OUTER JOIN --  7/19/2021 Added by Stephen for Schedule_Work_Code_Desc (Shift Description)
    (
      SELECT
          recr.recruitment_requisition_sid,
          recr.lawson_requisition_sid,
          s.candidate_profile_sid,
          o.accept_date,
          o.extend_date,
          o.start_date,
          recr.recruitment_requisition_num_text,
          recr.lawson_requisition_num,
          recr.recruitment_job_sid,
          recr.hiring_manager_user_sid
        FROM
         {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS recr
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s ON recr.recruitment_requisition_sid = s.recruitment_requisition_sid
           AND DATE(s.valid_to_date) = DATE('9999-12-31')
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer AS o ON s.submission_sid = o.submission_sid
           AND DATE(o.valid_to_date) = DATE('9999-12-31')
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status AS os ON o.offer_sid = os.offer_sid
           AND DATE(os.valid_to_date) = DATE('9999-12-31')
        WHERE os.offer_status_id IN(
          10, 1010
        )
         AND DATE(recr.valid_to_date) = DATE('9999-12-31')
        QUALIFY row_number() OVER (PARTITION BY recr.lawson_requisition_sid ORDER BY o.capture_date DESC, recr.approved_sw DESC, recr.recruitment_requisition_sid DESC, o.sequence_num DESC) = 1
    ) AS offer ON hrmet.requisition_sid = offer.lawson_requisition_sid
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS r ON hrmet.requisition_sid = r.requisition_sid
     AND DATE(r.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj ON offer.recruitment_job_sid = rj.recruitment_job_sid
     AND DATE(rj.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS recuser ON rj.recruiter_user_sid = recuser.recruitment_user_sid
     AND DATE(recuser.valid_to_date) = DATE('9999-12-31')
  WHERE hrmet.date_id BETWEEN '2016-01-01' AND current_date('US/Central')
   AND hrmet.analytics_msr_sid = 80200
   AND hrmet.action_code IN(
    -- --------------Join to get Recruiter information
    --  hires
    '1HIREAPPL', '1REHIRE', '1XFERPOS', '1XFEREIN-S', '1XFERINT'
  )
  GROUP BY 1, upper(concat(coalesce(hrdr.coid, '00000'), hrdr.process_level_code, coalesce(hrdr.dept_code, '00000'), hrdr.lawson_company_num)), 3, 4, 5, 6, 7, 8, 9, 10, upper(CASE
    WHEN upper(dept_starn.dept_code) = '63110'
     AND upper(dept_starn.process_level_code) = '26624' THEN 'External'
    WHEN upper(hrmet.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition'
    WHEN lb30.employee_num IS NULL
     AND upper(hrmet.action_code) NOT LIKE '%1XFER%' THEN 'External'
    ELSE 'Internal'
  END), 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, upper(CASE
    WHEN upper(jobcl.job_class_code) = '103' THEN 'RN'
    WHEN jobco.job_code IN(
      ndir.job_code
    ) THEN 'Leadership'
    WHEN upper(trim(jobpos.position_code_desc)) = upper(trim(pct.job_title_text)) THEN 'PCT'
    ELSE 'Non-RN'
  END), 23, upper(CASE
    WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 150 THEN 'New Grad'
    WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 365 THEN 'Less than 1 Year'
    WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 731 THEN '1-2 Years'
    WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) > 731 THEN '2+ Years'
    ELSE 'Unknown'
  END), 25, 26, 27, 28, 29, 30, upper(CASE
    WHEN jobpos.position_code_desc = norm.job_title_text
     AND upper(stas.status_code) = 'PRN'
     AND upper(facfac.division_name) = 'NORTH TEXAS DIVISION' THEN 'Normalized'
    WHEN upper(trim(jobpos.position_code_desc)) = upper(trim(norm.job_title_text))
     AND upper(stas.status_code) = 'TEMP'
     AND upper(facfac.division_name) = 'NORTH TEXAS DIVISION' THEN 'Normalized'
    ELSE 'Standard'
  END), upper(CASE
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
  END), 34, 35, upper(concat(recuser.last_name, ', ', recuser.first_name)), 37
