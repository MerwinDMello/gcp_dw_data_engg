create or replace view {{ params.param_hr_bi_views_dataset_name }}.facthires as SELECT
    last_day(DATE(hrmet.date_id)) AS pe_date,
    upper(hrmet.coid) AS coid,
    upper(concat(coalesce(hrdr.coid, '00000'), hrdr.process_level_code, coalesce(hrdr.dept_code, '00000'), hrdr.lawson_company_num)) AS pl_uid,
    concat(coalesce(trim(cast(hrmet.requisition_sid as string)), '00000'), coalesce(trim(cast(offer.candidate_profile_sid as string)), '00000')) AS req_app_uid,
    hrdr.cost_center,
    hrmet.date_id,
    upper(stas.status_code) AS status_code,
    CAST(ep.fte_percent as FLOAT64) AS fte_percent,
    upper(jobco.job_code_desc) AS job_code_desc,
    upper(ep.schedule_work_code) AS schedule_work_code,
    upper(hrc.hr_code_desc) AS schedule_work_code_desc,
    CASE
      WHEN upper(dept_starn.dept_code) = '63110'
       AND upper(dept_starn.process_level_code) = '26624' THEN 'External'
      WHEN upper(hrmet.action_reason_text) LIKE '%ACQ%' THEN 'Acquisition'
      WHEN lb30.employee_num IS NULL
       AND upper(hrmet.action_code) NOT LIKE '%1XFER%' THEN 'External'
      ELSE 'Internal'
    END AS hire_type,
    hrmet.employee_num,
    hrmet.employee_sid,
    hrmet.requisition_sid,
    offer.recruitment_requisition_sid,
    CAST(date_diff(hrmet.date_id, emppers.birth_date, DAY) / NUMERIC '365.25' as FLOAT64) AS age_at_hire,
    hrmet.key_talent_id,
    upper(hrmet.action_code) AS action_code,
    upper(hrmet.action_reason_text) AS action_reason_text,
    hrmet.position_sid,
    concat(trim(cast(hrmet.position_sid as string)), cast(jobpos.eff_to_date as string)) AS position_key,
    CASE
      WHEN upper(jobcl.job_class_code) = '103' THEN 'RN'
      WHEN upper(jobco.job_code) IN(
        upper(ndir.job_code)
      ) THEN 'Leadership'
      WHEN upper(jobpos.position_code_desc) = upper(pct.job_title_text) THEN 'PCT'
      ELSE 'Non-RN'
    END AS rn_grouping,
    CAST(date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) / NUMERIC '365.25' as FLOAT64) AS rn_xp_date,
    CASE
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 150 THEN 'New Grad'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 365 THEN 'Less than 1 Year'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 731 THEN '1-2 Years'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) > 731 THEN '2+ Years'
      ELSE 'Unknown'
    END AS rn_xp_bucket_at_hire,
    upper(hrmet.process_level_code) AS process_level_code,
    hrmet.integrated_lob_id,
    r.requisition_open_date,
    offer.extend_date,
    offer.accept_date,
    offer.start_date,
    CASE
      WHEN upper(jobpos.position_code_desc) = upper(norm.job_title_text)
       AND upper(stas.status_code) = 'PRN'
       AND upper(facfac.division_name) = 'NORTH TEXAS DIVISION' THEN 'Normalized'
      WHEN upper(jobpos.position_code_desc) = upper(norm.job_title_text)
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
    upper(CASE
      WHEN upper(stas.status_code) IN(
        'FT', 'PT'
      )
       OR (upper(jobpos.position_code_desc) LIKE '%PRN II'
       OR upper(jobpos.position_code_desc) LIKE '%PRN III') THEN 'Core'
      ELSE 'Flex'
    END) AS workforce_category,
    CAST(date_diff(hrmet.date_id, r.requisition_open_date, DAY) as FLOAT64) AS tts,
    hrmet.source_system_code,
    concat(recuser.last_name, ', ', recuser.first_name) AS recruiter_name,
    recuser.employee_34_login_code AS recruiter_34,
    sum(CASE
      WHEN hrmet.analytics_msr_sid = 80200.0 THEN hrmet.metric_numerator_qty
      ELSE CAST(0 as NUMERIC)
    END) AS hires
  FROM
   {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot AS hrmet 
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facfac ON upper(hrmet.coid) = upper(facfac.coid)
     AND upper(hrmet.company_code) = upper(facfac.company_code)
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_department AS fundept ON upper(hrmet.functional_dept_num) = upper(fundept.functional_dept_num)
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON hrmet.dept_sid = dept.dept_sid
     AND DATE(dept.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdr ON upper(hrmet.coid) = upper(hrdr.coid)
     AND upper(hrmet.process_level_code) = upper(hrdr.process_level_code)
     AND hrmet.dept_sid = hrdr.dept_sid
     AND hrmet.lawson_company_num = hrdr.lawson_company_num
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jobcl ON hrmet.job_class_sid = jobcl.job_class_sid
     AND DATE(jobcl.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jobco ON hrmet.job_code_sid = jobco.job_code_sid
     AND DATE(jobco.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_location AS reflo ON upper(hrmet.location_code) = upper(reflo.location_code)
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stas ON hrmet.auxiliary_status_sid = stas.status_sid
     AND DATE(stas.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stast ON hrmet.employee_status_sid = stast.status_sid
     AND DATE(stast.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobpos ON hrmet.position_sid = jobpos.position_sid
     AND DATE(jobpos.valid_to_date) = DATE('9999-12-31')
     AND hrmet.date_id BETWEEN jobpos.eff_from_date AND jobpos.eff_to_date
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS refkt ON hrmet.key_talent_id = refkt.key_talent_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_director AS ndir ON upper(jobco.job_code) = upper(ndir.job_code)
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_care_position AS pct ON upper(jobpos.position_code_desc) = upper(pct.job_title_text)
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.bi_employee_detail AS empdet ON empdet.employee_sid = hrmet.employee_sid
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_rn_normalization AS norm ON upper(jobpos.position_code_desc) = upper(norm.job_title_text)
     AND upper(jobcl.job_class_desc) = upper(norm.skill_mix_desc)
     AND CASE
      WHEN upper(stas.status_code) IN(
        --  10/22/2020 Added by Thomas to correct duplication
        'FT', 'PT'
      ) THEN 'FT/PT'
      WHEN upper(stas.status_code) IN(
        'PRN', 'TEMP'
      ) THEN 'PRN/TEMP'
    END = upper(norm.auxiliary_status_code)
    LEFT OUTER JOIN --  10/22/2020 Added by Thomas to correct duplication
    {{ params.param_pub_views_dataset_name }}.sub_functional_department AS subf ON hrmet.sub_functional_dept_num = subf.sub_functional_dept_num
     AND hrmet.functional_dept_num = fundept.functional_dept_num
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS emppers ON hrmet.employee_sid = emppers.employee_sid
     AND DATE(emppers.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_position AS ep ON hrmet.employee_sid = ep.employee_sid
     AND hrmet.position_sid = ep.position_sid
     AND DATE(ep.valid_to_date) = DATE('9999-12-31')
     AND hrmet.date_id BETWEEN ep.eff_from_date AND ep.eff_to_date
    LEFT OUTER JOIN --  7/19/2021 Added by Stephen for Schedule_Work_Code (Shift Code)
   {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS hrc ON ep.schedule_work_code = hrc.hr_code
     AND upper(hrc.hr_type_code) = 'WS'
     AND upper(hrc.active_ind) = 'A'
    INNER JOIN --  7/19/2021 Added by Stephen for Schedule_Work_Code_Desc (Shift Description)
    -- -- Added snapshot date join to only pull the most recent snapshots
    (
      SELECT
          max(fact_hr_metric_snapshot.snapshot_date) AS maxsnap
        FROM
         {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot
    ) AS snapdt ON hrmet.snapshot_date = snapdt.maxsnap
    LEFT OUTER JOIN (
      SELECT
          lb.employee_num,
          lb.dept_sid,
          lb.date_id,
          lb.analytics_msr_sid,
          lb.snapshot_date
        FROM
         {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot AS lb
        WHERE lb.analytics_msr_sid = 80100.0
    ) AS lb30 ON hrmet.employee_num = lb30.employee_num
     AND date_sub(hrmet.date_id, interval 30 DAY) = lb30.date_id
    LEFT OUTER JOIN -- ---QUALIFY Row_Number()Over(PARTITION BY lb.Employee_num ORDER BY lb.Date_ID DESC)=1)  --- removed when found to cause internal external mismatch.
   {{ params.param_hr_base_views_dataset_name }}.department AS dept_starn ON lb30.dept_sid = dept_starn.dept_sid
     AND DATE(dept_starn.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN -- -- Add Candidate Profile info for join
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
          LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.offer_status AS os ON o.offer_sid = os.offer_sid
           AND DATE(os.valid_to_date) = DATE('9999-12-31')
        WHERE os.offer_status_id IN (
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
  WHERE hrmet.date_id BETWEEN '2016-01-01' AND date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
   AND hrmet.analytics_msr_sid = 80200
   AND upper(hrmet.action_code) IN(
    -- --------------Join to get Recruiter information
    --  hires
    '1HIREAPPL', '1REHIRE', '1XFERPOS', '1XFEREIN-S', '1XFERINT'
  )
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38

