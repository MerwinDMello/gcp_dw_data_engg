create or replace view {{ params.param_hr_bi_views_dataset_name }}.factterms as  SELECT
    last_day(DATE(hrmet.date_id)) AS pe_date,
    coalesce(ev.effective_date, hrmet.date_id) as eff_date,      ---- modified 5/17/23 to include the ghr value where available or the lawson value if not.
    coalesce(ev.as_of_date, pa.action_last_update_date) as create_date,  ---- modified 5/17/23 to include the GHR value where available or the Lawson value if not.
    concat(coalesce(hrdr.coid, '00000'), hrdr.process_level_code, coalesce(hrdr.dept_code, '00000'), hrdr.lawson_company_num) AS pl_uid,
    hrmet.coid,
    hrdr.cost_center,
    stas.status_code,
    ep.fte_percent,
    CASE
      WHEN upper(trim(jobpos.position_code_desc)) = upper(trim(norm.job_title_text))
       AND upper(stas.status_code) = 'PRN'
       AND upper(facfac.division_name) = 'NORTH TEXAS DIVISION' THEN 'Normalized'
      WHEN upper(trim(jobpos.position_code_desc)) = upper(trim(norm.job_title_text))
       AND upper(stas.status_code) = 'TEMP'
       AND upper(facfac.division_name) = 'NORTH TEXAS DIVISION' THEN 'Normalized'
      ELSE 'Standard'
    END AS normalization,
    CASE
      WHEN upper(dept.dept_code) = '63110'
       AND upper(dept.process_level_code) = '26624' THEN 'External'
      WHEN upper(hrmet.action_code) LIKE '%1XFER%' THEN 'Internal'
      WHEN upper(hrmet.action_code) = '1TERMPEND'
       AND hrmet.action_reason_text IN(
        'TV-FCLTRNS', 'TV-RELOHCA'
      ) THEN 'Internal'
      ELSE ard.action_reason_type_desc
    END AS term_type,
    hrmet.action_code,
    -- 6/2/2022 Added by Stephen
    hrmet.action_reason_text,
    hrmet.employee_num,
    hrmet.employee_sid,
    concat(hrmet.employee_num, hrmet.date_id) AS term_uid,
    round(date_diff(hrmet.date_id, emppers.birth_date, DAY) / NUMERIC '365.25', CAST(CAST(2 as BIGNUMERIC) as INT64)) AS age_at_term,
    hrmet.key_talent_id,
    adjust_hire.hr_employee_value_date AS adjusted_hire_date,
    hrmet.position_sid,
    concat(trim(cast(hrmet.position_sid as string)), jobpos.eff_to_date) AS position_key,
    ep.schedule_work_code,
    hrc.hr_code_desc AS schedule_work_code_desc,
    CASE
      WHEN upper(jobcl.job_class_code) = '103' THEN 'RN'
      WHEN jobco.job_code IN(
        ndir.job_code
      ) THEN 'Leadership'
      WHEN upper(trim(jobpos.position_code_desc)) = upper(trim(pct.job_title_text)) THEN 'PCT'
      ELSE 'Non-RN'
    END AS rn_grouping,
    hrmet.process_level_code,
    round(date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) / NUMERIC '365.25', CAST(CAST(2 as BIGNUMERIC) as INT64)) AS rn_xp_date,
    CASE
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 150 THEN 'New Grad'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 365 THEN 'Less than 1 Year'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 731 THEN '1-2 Years'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) > 731 THEN '2+ Years'
      ELSE 'Unknown'
    END AS rn_xp_bucket_at_term,
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
      WHEN (stas.status_code IN(
        '01', '02'
      )
       OR (upper(jobpos.position_code_desc) LIKE '%PRN II'
       OR upper(jobpos.position_code_desc) LIKE '%PRN III'))
       AND upper(jobcl.job_class_code) = '103' THEN 'Core'
      WHEN (upper(jobpos.position_code_desc) LIKE '%PRN I'
       OR upper(jobpos.position_code_desc) LIKE '%PRN'
       OR stas.status_code IN(
        '04'
      ))
       AND upper(jobcl.job_class_code) = '103' THEN 'Flex'
      ELSE CAST(NULL as STRING)
    END AS workforce_category,
    CASE
      WHEN hrmet.lawson_company_num = 300 THEN date_diff(CAST(hrmet.date_id as DATE), CAST(adjust_hire.hr_employee_value_date as DATE),month)
      ELSE date_diff(CAST(hrmet.date_id as DATE), CAST(emp.anniversary_date as DATE), month)
    END AS tenure,
    hrmet.integrated_lob_id,
    ard.mapped_action_reason_text AS term_action_reason_mapped,
    ard.action_reason_group_text AS term_group,
    ard.action_reason_sub_group_text AS term_subgroup,
    hrmet.source_system_code
  FROM
   {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot AS hrmet 
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON hrmet.dept_sid = dept.dept_sid
     AND DATE(dept.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdr ON hrmet.coid = hrdr.coid
     AND hrmet.process_level_code = hrdr.process_level_code
     AND hrmet.dept_sid = hrdr.dept_sid
     AND hrmet.lawson_company_num = hrdr.lawson_company_num
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facfac ON hrmet.coid = facfac.coid
     AND hrmet.company_code = facfac.company_code
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
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS refkt ON hrmet.key_talent_id = refkt.key_talent_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emp ON hrmet.employee_sid = emp.employee_sid
     AND DATE(emp.valid_to_date) = DATE('9999-12-31')
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
    LEFT OUTER JOIN -- Added Qualify to sub-query in order to removed duplicate records in employee_position
    (
      SELECT
          ep_0.employee_sid,
          ep_0.position_sid,
          ep_0.fte_percent,
          ep_0.schedule_work_code,
          ep_0.eff_from_date,
          ep_0.eff_to_date
        FROM
         {{ params.param_hr_base_views_dataset_name }}.employee_position AS ep_0
        WHERE DATE(ep_0.valid_to_date) = DATE('9999-12-31')
        QUALIFY row_number() OVER (PARTITION BY ep_0.employee_sid, ep_0.position_sid ORDER BY ep_0.eff_from_date DESC) = 1
    ) AS ep ON hrmet.employee_sid = ep.employee_sid
     AND hrmet.position_sid = ep.position_sid
     AND hrmet.date_id BETWEEN ep.eff_from_date AND ep.eff_to_date
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS hrc ON ep.schedule_work_code = hrc.hr_code
     AND upper(hrc.hr_type_code) = 'WS'
     AND upper(hrc.active_ind) = 'A'
    LEFT OUTER JOIN --  7/19/2021 Added by Stephen for Schedule_Work_Code_Desc (Shift Description)
   {{ params.param_hr_base_views_dataset_name }}.ref_action_reason_detail AS ard ON hrmet.action_reason_text = ard.action_reason_text
    LEFT OUTER JOIN -- 9/22/2021 Added by Stephen
    -- 9/21/2022 Added by Stephen to correct historical Adjusted Hire Date for employees that termed and were rehired after 180 days
    (
      SELECT
          hreh.employee_sid,
          hreh.employee_num,
          hreh.hr_employee_element_date AS eff_from,
          CASE
            WHEN lead(hreh.hr_employee_element_date, 1) OVER (PARTITION BY hreh.employee_num, hreh.lawson_element_num ORDER BY hreh.lawson_element_num, hreh.hr_employee_element_date, hreh.sequence_num) - 1 IS NULL THEN parse_date('%Y-%m-%d', '9999-12-31')
            ELSE lead(hreh.hr_employee_element_date, 1) OVER (PARTITION BY hreh.employee_num, hreh.lawson_element_num ORDER BY hreh.lawson_element_num, hreh.hr_employee_element_date, hreh.sequence_num) - 1
          END AS eff_to,
          hreh.hr_employee_value_date
        FROM
          {{ params.param_hr_views_dataset_name }}.hr_employee_history AS hreh
        WHERE DATE(hreh.valid_to_date) = DATE('9999-12-31')
         AND hreh.lawson_element_num IN(
          51
        )
        QUALIFY row_number() OVER (PARTITION BY hreh.employee_num, hreh.lawson_element_num, eff_from ORDER BY eff_from, hreh.sequence_num DESC) = 1
    ) AS adjust_hire ON hrmet.employee_sid = adjust_hire.employee_sid
     AND hrmet.date_id BETWEEN adjust_hire.eff_from AND adjust_hire.eff_to

    ----- Add join to person action to pick up the Term Creation Date (Action_Last_Update_Date) which is when the term action was first keyed in to Lawson.  4/26/23 
    LEFT JOIN (
      SELECT 
        employee_sid, 
        action_last_update_date, 
        eff_from_date, 
        action_code
      FROM {{ params.param_hr_base_views_dataset_name }}.person_action 
      WHERE DATE(valid_to_date) = '9999-12-31'
      QUALIFY ROW_NUMBER() Over(PARTITION BY employee_sid, eff_from_date, action_code ORDER BY action_sequence_num DESC) = 1
    ) pa
      ON hrmet.employee_sid = pa.employee_sid
      AND hrmet.date_id = pa.eff_from_date
      AND hrmet.action_code = pa.action_code

      ---Add Join for the Term entry date from GHR   5/19/23
    LEFT JOIN (
      SELECT DISTINCT
        v.variable_name, 
        CAST(v.variable_value_text AS INT64) as employee_num,
        max(eff_date.effective_date) as effective_date,
        max(as_of.entry_date) as as_of_date

      FROM {{ params.param_hr_base_views_dataset_name }}.hr_workunit_variable v

      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit hwu
      ON v.workunit_sid = hwu.workunit_sid
      AND  UPPER(hwu.service_definition_text) IN ('REMOVEWORKASSIGNMENT', 'TERMINATE', 'HCA TERM FINAL', 'TRANSFER')
      AND UPPER(hwu.active_dw_ind) = 'Y'
      AND UPPER(hwu.source_system_code) = 'B'

      INNER JOIN (
        SELECT 
          v.workunit_sid,
          v.variable_name,
          Cast(v.variable_value_text AS DATE Format 'yyyymmdd') AS effective_date

        FROM {{ params.param_hr_base_views_dataset_name }}.hr_workunit_variable v

        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit hwu
          ON v.workunit_sid = hwu.workunit_sid
          AND  UPPER(hwu.service_definition_text) IN ('REMOVEWORKASSIGNMENT', 'TERMINATE', 'HCA TERM FINAL', 'TRANSFER')
          AND UPPER(hwu.active_dw_ind) = 'Y'
          AND UPPER(hwu.source_system_code) = 'B'

        WHERE UPPER(v.variable_name) = 'EFFECTIVEDATE'
          AND UPPER(v.variable_value_text) not in ('0', '00000000', '', 'UNDEFINED') --added '' and 'undefined' due to those strings causing the cast to fail
          AND v.variable_value_text IS NOT NULL 
          AND DATE(v.valid_to_date) = '9999-12-31'
          AND UPPER(v.source_system_code) = 'B'
      ) eff_date
      ON eff_date.workunit_sid = v.workunit_sid 

      INNER JOIN (
        SELECT 
          v.workunit_sid,
          v.variable_name,
          Cast(v.variable_value_text AS DATE Format 'yyyymmdd')  AS as_of_date,
          hwu.close_date_time AS entry_date

        FROM {{ params.param_hr_base_views_dataset_name }}.hr_workunit_variable v

        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit hwu
        ON v.workunit_sid = hwu.workunit_sid
          AND  UPPER(hwu.service_definition_text) IN ('REMOVEWORKASSIGNMENT', 'TERMINATE', 'HCA TERM FINAL', 'TRANSFER')
          AND UPPER(hwu.active_dw_ind) = 'Y'
          AND UPPER(hwu.source_system_code) = 'B'

        WHERE UPPER(v.variable_name) = 'ASOFDATE'
          AND UPPER(v.variable_value_text) not in ('0', '00000000', '', 'UNDEFINED') 
          AND v.variable_value_text IS NOT NULL 
          AND DATE(v.valid_to_date) = '9999-12-31'
          AND UPPER(v.source_system_code) = 'B' 
      ) as_of
      ON as_of.workunit_sid = v.workunit_sid
      WHERE UPPER(v.variable_name) = 'EMPLOYEE'
        AND DATE(v.valid_to_date) = '9999-12-31'
        AND UPPER(v.source_system_code) = 'B'
      GROUP BY 1,2
    ) ev
    ON hrmet.employee_num = ev.employee_num
    AND (Cast(hrmet.date_id AS DATE) = Cast(ev.effective_date AS DATE) OR Cast(hrmet.date_id AS DATE) = Cast(ev.effective_date AS DATE)-1)

    INNER JOIN -- -- Added snapshot date join to only pull the most recent snapshots
    (
      SELECT
          max(fact_hr_metric_snapshot.snapshot_date) AS maxsnap
        FROM
         {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot
    ) AS snapdt ON hrmet.snapshot_date = snapdt.maxsnap
  WHERE hrmet.date_id BETWEEN '2016-01-01' AND date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
   AND hrmet.analytics_msr_sid = 80300;

create or replace view {{ params.param_hr_bi_views_dataset_name }}.factterms_daily as SELECT
    hrmet.date_id AS pe_date,
    COALESCE(ev.as_of_date, pa.action_last_update_date) AS create_date,
    concat(coalesce(hrdr.coid, '00000'), hrdr.process_level_code, coalesce(hrdr.dept_code, '00000'), hrdr.lawson_company_num) AS pl_uid,
    hrmet.coid,
    hrdr.cost_center,
    stas.status_code,
    ep.fte_percent AS fte_percent,
    CASE
      WHEN upper(trim(jobpos.position_code_desc)) = upper(trim(norm.job_title_text))
       AND upper(stas.status_code) = 'PRN'
       AND upper(facfac.division_name) = 'NORTH TEXAS DIVISION' THEN 'Normalized'
      WHEN upper(trim(jobpos.position_code_desc)) = upper(trim(norm.job_title_text))
       AND upper(stas.status_code) = 'TEMP'
       AND upper(facfac.division_name) = 'NORTH TEXAS DIVISION' THEN 'Normalized'
      ELSE 'Standard'
    END AS normalization,
    CASE
      WHEN upper(dept.dept_code) = '63110'
       AND upper(dept.process_level_code) = '26624' THEN 'External'
      WHEN upper(hrmet.action_code) LIKE '%1XFER%' THEN 'Internal'
      WHEN upper(hrmet.action_code) = '1TERMPEND'
       AND hrmet.action_reason_text IN(
        'TV-FCLTRNS', 'TV-RELOHCA'
      ) THEN 'Internal'
      ELSE ard.action_reason_type_desc
    END AS term_type,
    hrmet.action_code,
    -- 6/2/2022 Added by Stephen
    hrmet.action_reason_text,
    hrmet.employee_num,
    hrmet.employee_sid,
    concat(hrmet.employee_num, hrmet.date_id) AS term_uid,
    CAST(date_diff(hrmet.date_id, emppers.birth_date, DAY) / NUMERIC '365.25' as FLOAT64) AS age_at_term,
    hrmet.key_talent_id,
    adjust_hire.hr_employee_value_date AS adjusted_hire_date,
    hrmet.position_sid,
    concat(trim(cast(hrmet.position_sid as string)), jobpos.eff_to_date) AS position_key,
    ep.schedule_work_code,
    hrc.hr_code_desc AS schedule_work_code_desc,
    CASE
      WHEN upper(jobcl.job_class_code) = '103' THEN 'RN'
      WHEN jobco.job_code IN(
        ndir.job_code
      ) THEN 'Leadership'
      WHEN upper(trim(jobpos.position_code_desc)) = upper(trim(pct.job_title_text)) THEN 'PCT'
      ELSE 'Non-RN'
    END AS rn_grouping,
    hrmet.process_level_code,
    CAST(date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) / NUMERIC '365.25' as FLOAT64) AS rn_xp_date,
    CASE
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 150 THEN 'New Grad'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 365 THEN 'Less than 1 Year'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) <= 731 THEN '1-2 Years'
      WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, DAY) > 731 THEN '2+ Years'
      ELSE 'Unknown'
    END AS rn_xp_bucket_at_term,
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
    CASE
      WHEN hrmet.lawson_company_num = 300 THEN date_diff(hrmet.date_id , adjust_hire.hr_employee_value_date,month )
      ELSE date_diff(hrmet.date_id , emp.anniversary_date ,month)
    END AS tenure,
    hrmet.integrated_lob_id,
    ard.mapped_action_reason_text AS term_action_reason_mapped,
    ard.action_reason_group_text AS term_group,
    ard.action_reason_sub_group_text AS term_subgroup,
    hrmet.source_system_code
  FROM
   {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS hrmet
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON hrmet.dept_sid = dept.dept_sid
     AND DATE(dept.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdr ON hrmet.coid = hrdr.coid
     AND hrmet.process_level_code = hrdr.process_level_code
     AND hrmet.dept_sid = hrdr.dept_sid
     AND hrmet.lawson_company_num = hrdr.lawson_company_num
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facfac ON hrmet.coid = facfac.coid
     AND hrmet.company_code = facfac.company_code
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
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS refkt ON hrmet.key_talent_id = refkt.key_talent_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emp ON hrmet.employee_sid = emp.employee_sid
     AND DATE(emp.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_director AS ndir ON jobco.job_code = ndir.job_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_care_position AS pct ON jobpos.position_code_desc = pct.job_title_text
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
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.bi_employee_detail AS empdet ON hrmet.employee_sid = empdet.employee_sid
    LEFT OUTER JOIN -- Added Qualify to sub-query in order to removed duplicate records in employee_position
    (
      SELECT
          ep_0.employee_sid,
          ep_0.position_sid,
          ep_0.fte_percent,
          ep_0.schedule_work_code,
          ep_0.eff_from_date,
          ep_0.eff_to_date
        FROM
         {{ params.param_hr_base_views_dataset_name }}.employee_position AS ep_0
        WHERE DATE(ep_0.valid_to_date) = DATE('9999-12-31')
        QUALIFY row_number() OVER (PARTITION BY ep_0.employee_sid, ep_0.position_sid ORDER BY ep_0.eff_from_date DESC) = 1
    ) AS ep ON hrmet.employee_sid = ep.employee_sid
     AND hrmet.position_sid = ep.position_sid
     AND hrmet.date_id BETWEEN ep.eff_from_date AND ep.eff_to_date
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS hrc ON ep.schedule_work_code = hrc.hr_code
     AND upper(hrc.hr_type_code) = 'WS'
     AND upper(hrc.active_ind) = 'A'
    LEFT OUTER JOIN --  7/19/2021 Added by Stephen for Schedule_Work_Code_Desc (Shift Description)
   {{ params.param_hr_base_views_dataset_name }}.ref_action_reason_detail AS ard ON hrmet.action_reason_text = ard.action_reason_text
    LEFT OUTER JOIN -- 9/23/21 Added by Stephen
    -- 9/21/2022 Added by Stephen to correct historical Adjusted Hire Date for employees that termed and were rehired after 180 days
    (
      SELECT
          hreh.employee_sid,
          hreh.employee_num,
          hreh.hr_employee_element_date AS eff_from,
          CASE
            WHEN lead(hreh.hr_employee_element_date, 1) OVER (PARTITION BY hreh.employee_num, hreh.lawson_element_num ORDER BY hreh.lawson_element_num, hreh.hr_employee_element_date, hreh.sequence_num) - 1 IS NULL THEN parse_date('%Y-%m-%d', '9999-12-31')
            ELSE lead(hreh.hr_employee_element_date, 1) OVER (PARTITION BY hreh.employee_num, hreh.lawson_element_num ORDER BY hreh.lawson_element_num, hreh.hr_employee_element_date, hreh.sequence_num) - 1
          END AS eff_to,
          hreh.hr_employee_value_date
        FROM
          {{ params.param_hr_views_dataset_name }}.hr_employee_history AS hreh
        WHERE DATE(hreh.valid_to_date) = DATE('9999-12-31')
         AND hreh.lawson_element_num IN(
          51
        )
        QUALIFY row_number() OVER (PARTITION BY hreh.employee_num, hreh.lawson_element_num, eff_from ORDER BY eff_from, hreh.sequence_num DESC) = 1
    ) AS adjust_hire ON hrmet.employee_sid = adjust_hire.employee_sid
     AND hrmet.date_id BETWEEN adjust_hire.eff_from AND adjust_hire.eff_to
    ----- Add join to person action to pick up the Term Creation Date (Action_Last_Update_Date) which is when the term action was first keyed in to Lawson.  4/26/23 
    LEFT JOIN (
      SELECT 
        employee_sid, 
        action_last_update_date, 
        eff_from_date, 
        action_code
      FROM {{ params.param_hr_base_views_dataset_name }}.person_action 
      WHERE DATE(valid_to_date) = '9999-12-31'
      QUALIFY ROW_NUMBER() Over(PARTITION BY employee_sid, eff_from_date, action_code ORDER BY action_sequence_num DESC) = 1
    ) pa
      ON hrmet.employee_sid = pa.employee_sid
      AND hrmet.date_id = pa.eff_from_date
      AND hrmet.action_code = pa.action_code

      ---Add Join for the Term entry date from GHR   5/19/23
    LEFT JOIN (
      SELECT DISTINCT
        v.variable_name, 
        CAST(v.variable_value_text AS INT64) as employee_num,
        max(eff_date.effective_date) as effective_date,
        max(as_of.entry_date) as as_of_date

      FROM {{ params.param_hr_base_views_dataset_name }}.hr_workunit_variable v

      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit hwu
      ON v.workunit_sid = hwu.workunit_sid
      AND  UPPER(hwu.service_definition_text) IN ('REMOVEWORKASSIGNMENT', 'TERMINATE', 'HCA TERM FINAL', 'TRANSFER')
      AND UPPER(hwu.active_dw_ind) = 'Y'
      AND UPPER(hwu.source_system_code) = 'B'

      INNER JOIN (
        SELECT 
          v.workunit_sid,
          v.variable_name,
          Cast(v.variable_value_text AS DATE Format 'yyyymmdd') AS effective_date

        FROM {{ params.param_hr_base_views_dataset_name }}.hr_workunit_variable v

        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit hwu
          ON v.workunit_sid = hwu.workunit_sid
          AND  UPPER(hwu.service_definition_text) IN ('REMOVEWORKASSIGNMENT', 'TERMINATE', 'HCA TERM FINAL', 'TRANSFER')
          AND UPPER(hwu.active_dw_ind) = 'Y'
          AND UPPER(hwu.source_system_code) = 'B'

        WHERE UPPER(v.variable_name) = 'EFFECTIVEDATE'
          AND UPPER(v.variable_value_text) not in ('0', '00000000', '', 'UNDEFINED') --added '' and 'undefined' due to those strings causing the cast to fail
          AND v.variable_value_text IS NOT NULL 
          AND DATE(v.valid_to_date) = '9999-12-31'
          AND UPPER(v.source_system_code) = 'B'
      ) eff_date
      ON eff_date.workunit_sid = v.workunit_sid 

      INNER JOIN (
        SELECT 
          v.workunit_sid,
          v.variable_name,
          Cast(v.variable_value_text AS DATE Format 'yyyymmdd')  AS as_of_date,
          hwu.close_date_time AS entry_date

        FROM {{ params.param_hr_base_views_dataset_name }}.hr_workunit_variable v

        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit hwu
        ON v.workunit_sid = hwu.workunit_sid
          AND  UPPER(hwu.service_definition_text) IN ('REMOVEWORKASSIGNMENT', 'TERMINATE', 'HCA TERM FINAL', 'TRANSFER')
          AND UPPER(hwu.active_dw_ind) = 'Y'
          AND UPPER(hwu.source_system_code) = 'B'

        WHERE UPPER(v.variable_name) = 'ASOFDATE'
          AND UPPER(v.variable_value_text) not in ('0', '00000000', '', 'UNDEFINED') 
          AND v.variable_value_text IS NOT NULL 
          AND DATE(v.valid_to_date) = '9999-12-31'
          AND UPPER(v.source_system_code) = 'B' 
      )as_of
      ON as_of.workunit_sid = v.workunit_sid
      WHERE UPPER(v.variable_name) = 'EMPLOYEE'
        AND DATE(v.valid_to_date) = '9999-12-31'
        AND UPPER(v.source_system_code) = 'B'
      GROUP BY 1,2
    ) ev
    ON hrmet.employee_num = ev.employee_num
    AND (Cast(hrmet.date_id AS DATE) = Cast(ev.effective_date AS DATE) OR Cast(hrmet.date_id AS DATE) = Cast(ev.effective_date AS DATE)-1)

  WHERE hrmet.date_id > date_add(current_date('US/Central'), interval -37 MONTH)
   AND hrmet.analytics_msr_sid = 80300;
