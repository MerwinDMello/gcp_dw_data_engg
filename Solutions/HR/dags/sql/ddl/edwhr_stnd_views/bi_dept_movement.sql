CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.bi_dept_movement AS SELECT DISTINCT
    /*----#####################################################################################
    * Use Fact Headcount Change Query to create Standard View
    Initial Author: Thomas Jones
    Script copied to Standard View:  Cheryl Costa
    Creation Date: 12/9/2022
    #####################################################################################*/
    tm.total_movement_sid AS change_uid,
    'Out-bound' AS movement_direction,
    CASE
      WHEN jclf.job_class_code = jclt.job_class_code
       AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
      WHEN jclf.job_class_code <> jclt.job_class_code THEN 'SkillMix Chg'
      WHEN jclf.job_class_code = jclt.job_class_code
       AND staf.status_code = stato.status_code THEN 'XFER No Chng'
    END AS chg_group,
    concat(coalesce(hrdrf.coid, '00000'), hrdrf.process_level_code, coalesce(hrdrf.dept_code, '00000'), hrdrf.lawson_company_num) AS from_pl_uid,
    concat(coalesce(hrdrt.coid, '00000'), hrdrt.process_level_code, coalesce(hrdrt.dept_code, '00000'), hrdrt.lawson_company_num) AS to_pl_uid,
    concat(facf.coid, hrdrf.cost_center) AS coid_cost_center,
    last_day(DATE(tm.date_id)) AS pe_date,
    tm.date_id,
    tm.action_code,
    tm.action_reason_text,
    tm.employee_sid,
    tm.employee_num,
    empdet.acute_experience_start_date AS rn_xp_date,
    CASE
      WHEN (tm.from_lawson_company_num) = 300 THEN date_diff(tm.date_id , emp.adjusted_hire_date , month)
      ELSE date_diff(tm.date_id , coalesce(anniv_hist.anniversary_date, emp.anniversary_date) , month)
    END AS yos,
    staf.status_code AS from_aux_status,
    stato.status_code AS to_aux_status,
    tm.from_fte_pct AS from_fte_percent,
    tm.to_fte_pct AS to_fte_percent,
    tm.from_position_sid,
    tm.to_position_sid,
    jobf.schedule_work_code AS from_schedule_work_code,
    fhrc.hr_code_desc AS from_schedule_work_code_desc,
    jobt.schedule_work_code AS to_schedule_work_code,
    thrc.hr_code_desc AS to_schedule_work_code_desc,
    concat((tm.from_position_sid), jobf.eff_to_date) AS from_position_key,
    concat((tm.to_position_sid), jobt.eff_to_date) AS to_position_key,
    concat(coalesce(trim(cast(emp_req.requisition_sid as string)), CAST(NULL as STRING)), coalesce(trim(cast(offer.candidate_profile_sid as string)), CAST(NULL as STRING))) AS req_app_uid,
    (emp_req.requisition_sid) AS requisition_sid,
    CASE
      WHEN upper(jclf.job_class_code) = '103' THEN 'RN'
      WHEN jobcof.job_code IN(
        ndirf.job_code
      ) THEN 'Leadership'
      WHEN jobf.position_code_desc = pctf.job_title_text THEN 'PCT'
      ELSE 'Non-RN'
    END AS from_rn_grouping,
    CASE
      WHEN upper(jclt.job_class_code) = '103' THEN 'RN'
      WHEN jobcot.job_code IN(
        ndirt.job_code
      ) THEN 'Leadership'
      WHEN jobt.position_code_desc = pctt.job_title_text THEN 'PCT'
      ELSE 'Non-RN'
    END AS to_rn_grouping,
    jclf.job_class_code AS from_skill_mix,
    jclt.job_class_code AS to_skill_mix,
    coalesce(offer_link_cte.grad_flagb, CASE
      WHEN (offer_link_cte.new_grad) = 12320 THEN 'Y'
      WHEN (offer_link_cte.new_grad) = 12340 THEN 'Y'
      WHEN (offer_link_cte.new_grad) = 12321 THEN 'Y'
      WHEN (offer_link_cte.new_grad) = 12341 THEN 'Y'
      ELSE 'N'
    END) AS new_grad_flag,
    CASE
      WHEN (upper(jobf.position_code_desc) LIKE '%PRN I'
       OR upper(jobf.position_code_desc) LIKE '%PRN')
       AND upper(jclf.job_class_code) = '103' THEN 'PRN Tier 1'
      WHEN upper(jobf.position_code_desc) LIKE '%PRN II'
       AND upper(jclf.job_class_code) = '103' THEN 'PRN Tier 2'
      WHEN upper(jobf.position_code_desc) LIKE '%PRN III'
       AND upper(jclf.job_class_code) = '103' THEN 'PRN Tier 3'
      ELSE CAST(NULL as STRING)
    END AS prn_tier,
    CASE
      WHEN staf.status_code IN(
        'FT', 'PT'
      )
       OR (upper(jobf.position_code_desc) LIKE '%PRN II'
       OR upper(jobf.position_code_desc) LIKE '%PRN III') THEN 'Core'
      ELSE 'Flex'
    END AS workforce_category,
    0 AS aux_status_change_in,
    CASE
      WHEN upper(CASE
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
        WHEN jclf.job_class_code <> jclt.job_class_code THEN 'SkillMix Chg'
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code = stato.status_code THEN 'XFER No Chng'
      END) = 'AUX STATUS CHANGE'
       AND staf.status_code <> stato.status_code THEN 1
      ELSE 0
    END AS aux_status_change_out,
    0 AS skill_mix_change_in,
    CASE
      WHEN upper(CASE
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
        WHEN jclf.job_class_code <> jclt.job_class_code THEN 'SkillMix Chg'
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code = stato.status_code THEN 'XFER No Chng'
      END) = 'SKILLMIX CHG'
       AND jclf.job_class_code <> jclt.job_class_code THEN 1
      ELSE 0
    END AS skill_mix_change_out,
    0 AS department_transfer_in,
    CASE
      WHEN upper(CASE
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
        WHEN jclf.job_class_code <> jclt.job_class_code THEN 'SkillMix Chg'
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code = stato.status_code THEN 'XFER No Chng'
      END) = 'XFER NO CHNG'
       AND deptf.dept_code <> deptt.dept_code THEN 1
      ELSE 0
    END AS department_transfer_out,
    0 AS other_in,
    CASE
      WHEN upper(CASE
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
        WHEN jclf.job_class_code <> jclt.job_class_code THEN 'SkillMix Chg'
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code = stato.status_code THEN 'XFER No Chng'
      END) = 'XFER NO CHNG'
       AND deptf.dept_code = deptt.dept_code THEN 1
      ELSE 0
    END AS other_out,
    tm.source_system_code
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_total_movement AS tm
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jclf ON tm.from_job_class_sid = jclf.job_class_sid
     AND date(jclf.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jclt ON tm.to_job_class_sid = jclt.job_class_sid
     AND date(jclt.valid_to_date) = '9999-12-31'
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jobcof ON tm.from_job_code_sid = jobcof.job_code_sid
     AND date(jobcof.valid_to_date) = '9999-12-31'
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jobcot ON tm.to_job_code_sid = jobcot.job_code_sid
     AND date(jobcot.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS staf ON tm.from_auxiliary_status_sid = staf.status_sid
     AND upper(staf.status_type_code) = 'AUX'
     AND date(staf.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stato ON tm.to_auxiliary_status_sid = stato.status_sid
     AND upper(stato.status_type_code) = 'AUX'
     AND date(stato.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facf ON tm.from_coid = facf.coid
     AND tm.from_company_code = facf.company_code
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facto ON tm.to_coid = facto.coid
     AND tm.to_company_code = facto.company_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS deptf ON tm.from_dept_sid = deptf.dept_sid
     AND date(deptf.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS deptt ON tm.to_dept_sid = deptt.dept_sid
     AND date(deptt.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS plf ON tm.from_process_level_code = plf.process_level_code
     AND tm.from_lawson_company_num = plf.lawson_company_num
     AND date(plf.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS plt ON tm.to_process_level_code = plt.process_level_code
     AND tm.to_lawson_company_num = plt.lawson_company_num
     AND date(plt.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobf ON tm.from_position_sid = jobf.position_sid
     AND tm.date_id BETWEEN jobf.eff_from_date AND jobf.eff_to_date
     AND date(jobf.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobt ON tm.to_position_sid = jobt.position_sid
     AND tm.date_id BETWEEN jobt.eff_from_date AND jobt.eff_to_date
     AND date(jobt.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emp ON tm.employee_sid = emp.employee_sid
     AND date(emp.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdrf ON tm.from_coid = hrdrf.coid
     AND tm.from_process_level_code = hrdrf.process_level_code
     AND tm.from_dept_sid = hrdrf.dept_sid
     AND tm.from_lawson_company_num = hrdrf.lawson_company_num
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdrt ON tm.to_coid = hrdrt.coid
     AND tm.to_process_level_code = hrdrt.process_level_code
     AND tm.to_dept_sid = hrdrt.dept_sid
     AND tm.to_lawson_company_num = hrdrt.lawson_company_num
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON hrdrf.process_level_code = c.process_level_code
     AND hrdrf.lawson_company_num = c.lawson_company_num
     AND c.user_id = session_user()
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_requisition AS emp_req ON tm.employee_sid = emp_req.employee_sid
     AND tm.date_id = emp_req.eff_from_date
     AND tm.action_code = emp_req.action_code
     AND upper(emp_req.delete_ind) <> 'D'
     AND date(emp_req.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN (
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
           AND date(s.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer AS o ON s.submission_sid = o.submission_sid
           AND date(o.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status AS os ON o.offer_sid = os.offer_sid
           AND date(os.valid_to_date) = '9999-12-31'
        WHERE os.offer_status_id IN(
          10, 1010
        )
         AND date(recr.valid_to_date) = '9999-12-31'
        QUALIFY row_number() OVER (PARTITION BY recr.lawson_requisition_sid ORDER BY o.last_modified_date DESC, o.capture_date DESC, o.sequence_num DESC) = 1
    ) AS offer ON emp_req.requisition_sid = offer.lawson_requisition_sid
    LEFT OUTER JOIN (
      SELECT DISTINCT
          rr.recruitment_requisition_sid,
          o.active,
          ng_flag.grad_flag,
          ng_flag.grad_flagb,
          ng.new_grad,
          ng.new_gradb
        FROM
          {{ params.param_hr_base_views_dataset_name }}.offer_status AS os
          LEFT OUTER JOIN (
            SELECT
                ao.offer_sid,
                ao.submission_sid,
                rank() OVER (PARTITION BY ao.submission_sid ORDER BY ao.sequence_num DESC) AS active
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer AS ao
              WHERE date(ao.valid_to_date) = '9999-12-31'
          ) AS o ON os.offer_sid = o.offer_sid
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s ON o.submission_sid = s.submission_sid
           AND date(s.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp ON s.candidate_profile_sid = cp.candidate_profile_sid
           AND date(cp.valid_to_date) = '9999-12-31'
          INNER JOIN -- --start rr join
          (
            SELECT
                recr.recruitment_requisition_sid
              FROM
                {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS recr
                LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s_0 ON recr.recruitment_requisition_sid = s_0.recruitment_requisition_sid
                 AND date(s_0.valid_to_date) = '9999-12-31'
                LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer AS o_0 ON s_0.submission_sid = o_0.submission_sid
                 AND date(o_0.valid_to_date) = '9999-12-31'
                LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status AS os_0 ON os_0.offer_sid = o_0.offer_sid
                 AND date(os_0.valid_to_date) = '9999-12-31'
                LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_offer_status AS ros ON ros.offer_status_id = os_0.offer_status_id
              WHERE date(recr.valid_to_date) = '9999-12-31'
              QUALIFY row_number() OVER (PARTITION BY recr.lawson_requisition_sid ORDER BY o_0.last_modified_date DESC, o_0.capture_date DESC, recr.approved_sw DESC, recr.recruitment_requisition_sid DESC, o_0.sequence_num DESC) = 1
          ) AS rr ON s.recruitment_requisition_sid = rr.recruitment_requisition_sid
          LEFT OUTER JOIN -- --end rr join
          (
            SELECT
                od.element_detail_type_text,
                od.element_detail_id AS new_grad,
                od.offer_sid,
                od.element_detail_value_text AS new_gradb
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE od.element_detail_type_text IN(
                'New - Grad Nurse Recruitment Tracking', 'GraduateNurseRecruitmentTracking', 'RN_Training_Program'
              )
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 2, 4, 3, 1
          ) AS ng ON o.offer_sid = ng.offer_sid
          LEFT OUTER JOIN (
            SELECT
                od.source_system_code,
                od.element_detail_type_text,
                od.element_detail_id AS grad_flag,
                od.offer_sid,
                od.element_detail_value_text AS grad_flagb
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE od.element_detail_type_text IN(
                'New - Grad Nurse Recruitment Tracking', 'New - Grad Nurse Recruitment Tracking', 'GraduateNurseRecruitmentTracking'
              )
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 1, 3, 5, 4, 2
          ) AS ng_flag ON o.offer_sid = ng_flag.offer_sid
        WHERE os.offer_status_id IN(
          10, 1010
        )
         AND date(os.valid_to_date) = '9999-12-31'
    ) AS offer_link_cte ON offer.recruitment_requisition_sid = offer_link_cte.recruitment_requisition_sid
     AND offer_link_cte.active = 1
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.bi_employee_detail AS empdet ON tm.employee_sid = empdet.employee_sid
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_director AS ndirf ON jobcof.job_code = ndirf.job_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_care_position AS pctf ON jobf.position_code_desc = pctf.job_title_text
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_director AS ndirt ON jobcot.job_code = ndirt.job_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_care_position AS pctt ON jobt.position_code_desc = pctt.job_title_text
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS fhrc ON jobf.schedule_work_code = fhrc.hr_code
     AND upper(fhrc.hr_type_code) = 'WS'
    LEFT OUTER JOIN -- 5/12/2022 Added by Stephen for From_Schedule_Work_Code_Desc
    {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS thrc ON jobt.schedule_work_code = thrc.hr_code
     AND upper(thrc.hr_type_code) = 'WS'
    LEFT OUTER JOIN -- 5/12/2022 Added by Stephen for To_Schedule_Work_Code_Desc
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
        WHERE date(hreh.valid_to_date) = '9999-12-31'
         AND hreh.lawson_element_num = 25
        QUALIFY row_number() OVER (PARTITION BY hreh.employee_num, hreh.lawson_element_num, eff_from ORDER BY eff_from, hreh.sequence_num DESC) = 1
    ) AS anniv_hist ON tm.employee_sid = anniv_hist.employee_sid
     AND tm.date_id BETWEEN anniv_hist.eff_from AND anniv_hist.eff_to
  WHERE tm.analytics_msr_sid IN(
    80700
  )
UNION ALL
SELECT DISTINCT
    tm.total_movement_sid AS change_uid,
    'In-bound' AS movement_direction,
    CASE
      WHEN jclf.job_class_code = jclt.job_class_code
       AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
      WHEN jclf.job_class_code <> jclt.job_class_code THEN 'SkillMix Chg'
      WHEN jclf.job_class_code = jclt.job_class_code
       AND staf.status_code = stato.status_code THEN 'XFER No Chng'
    END AS chg_group,
    concat(coalesce(hrdrf.coid, '00000'), hrdrf.process_level_code, coalesce(hrdrf.dept_code, '00000'), hrdrf.lawson_company_num) AS from_pl_uid,
    concat(coalesce(hrdrt.coid, '00000'), hrdrt.process_level_code, coalesce(hrdrt.dept_code, '00000'), hrdrt.lawson_company_num) AS to_pl_uid,
    concat(facto.coid, hrdrt.cost_center) AS coid_cost_center,
    last_day(DATE(tm.date_id)) AS pe_date,
    tm.date_id,
    tm.action_code,
    tm.action_reason_text,
    tm.employee_sid,
    tm.employee_num,
    empdet.acute_experience_start_date AS rn_xp_date,
    CASE
      WHEN (tm.to_lawson_company_num) = 300 THEN date_diff(tm.date_id , emp.adjusted_hire_date , month)
      ELSE date_diff(tm.date_id , coalesce(anniv_hist.anniversary_date, emp.anniversary_date) , month)
    END AS yos,
    staf.status_code AS from_aux_status,
    stato.status_code AS to_aux_status,
    tm.from_fte_pct AS from_fte_percent,
    tm.to_fte_pct AS to_fte_percent,
    tm.from_position_sid,
    tm.to_position_sid,
    jobf.schedule_work_code AS from_schedule_work_code,
    fhrc.hr_code_desc AS from_schedule_work_code_desc,
    jobt.schedule_work_code AS to_schedule_work_code,
    thrc.hr_code_desc AS to_schedule_work_code_desc,
    concat((tm.from_position_sid), jobf.eff_to_date) AS from_position_key,
    concat((tm.to_position_sid), jobt.eff_to_date) AS to_position_key,
    concat(coalesce(trim(cast(emp_req.requisition_sid as string)), '00000'), coalesce(trim(cast(offer.candidate_profile_sid as string)), '00000')) AS req_app_uid,
    (emp_req.requisition_sid) AS requisition_sid,
    CASE
      WHEN upper(jclf.job_class_code) = '103' THEN 'RN'
      WHEN jobcof.job_code IN(
        ndirf.job_code
      ) THEN 'Leadership'
      WHEN jobcof.job_code_desc = pctf.job_code_desc
       AND jobf.position_code_desc = pctf.job_title_text THEN 'PCT'
      ELSE 'Non-RN'
    END AS from_rn_grouping,
    CASE
      WHEN upper(jclt.job_class_code) = '103' THEN 'RN'
      WHEN jobcot.job_code IN(
        ndirt.job_code
      ) THEN 'Leadership'
      WHEN jobcot.job_code_desc = pctt.job_code_desc
       AND jobt.position_code_desc = pctt.job_title_text THEN 'PCT'
      ELSE 'Non-RN'
    END AS to_rn_grouping,
    jclf.job_class_code AS from_skill_mix,
    jclt.job_class_code AS to_skill_mix,
    coalesce(offer_link_cte.grad_flagb, CASE
      WHEN (offer_link_cte.new_grad) = 12320 THEN 'Y'
      WHEN (offer_link_cte.new_grad) = 12340 THEN 'Y'
      WHEN (offer_link_cte.new_grad) = 12321 THEN 'Y'
      WHEN (offer_link_cte.new_grad) = 12341 THEN 'Y'
      ELSE 'N'
    END) AS new_grad_flag,
    CASE
      WHEN (upper(jobt.position_code_desc) LIKE '%PRN I'
       OR upper(jobt.position_code_desc) LIKE '%PRN')
       AND upper(jclt.job_class_code) = '103' THEN 'PRN Tier 1'
      WHEN upper(jobt.position_code_desc) LIKE '%PRN II'
       AND upper(jclt.job_class_code) = '103' THEN 'PRN Tier 2'
      WHEN upper(jobt.position_code_desc) LIKE '%PRN III'
       AND upper(jclt.job_class_code) = '103' THEN 'PRN Tier 3'
      ELSE CAST(NULL as STRING)
    END AS prn_tier,
    CASE
      WHEN stato.status_code IN(
        'FT', 'PT'
      )
       OR (upper(jobt.position_code_desc) LIKE '%PRN II'
       OR upper(jobt.position_code_desc) LIKE '%PRN III') THEN 'Core'
      ELSE 'Flex'
    END AS workforce_category,
    CASE
      WHEN upper(CASE
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
        WHEN jclf.job_class_code <> jclt.job_class_code THEN 'SkillMix Chg'
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code = stato.status_code THEN 'XFER No Chng'
      END) = 'AUX STATUS CHANGE'
       AND stato.status_code <> staf.status_code THEN 1
      ELSE 0
    END AS aux_status_change_in,
    0 AS aux_status_change_out,
    CASE
      WHEN upper(CASE
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
        WHEN jclf.job_class_code <> jclt.job_class_code THEN 'SkillMix Chg'
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code = stato.status_code THEN 'XFER No Chng'
      END) = 'SKILLMIX CHG'
       AND jclt.job_class_code <> jclf.job_class_code THEN 1
      ELSE 0
    END AS skill_mix_change_in,
    0 AS skill_mix_change_out,
    CASE
      WHEN upper(CASE
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
        WHEN jclf.job_class_code <> jclt.job_class_code THEN 'SkillMix Chg'
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code = stato.status_code THEN 'XFER No Chng'
      END) = 'XFER NO CHNG'
       AND deptf.dept_code <> deptt.dept_code THEN 1
      ELSE 0
    END AS department_transfer_in,
    0 AS department_transfer_out,
    CASE
      WHEN upper(CASE
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code <> stato.status_code THEN 'Aux Status Change'
        WHEN jclf.job_class_code <> jclt.job_class_code THEN 'SkillMix Chg'
        WHEN jclf.job_class_code = jclt.job_class_code
         AND staf.status_code = stato.status_code THEN 'XFER No Chng'
      END) = 'XFER NO CHNG'
       AND deptf.dept_code = deptt.dept_code THEN 1
      ELSE 0
    END AS other_in,
    0 AS other_out,
    tm.source_system_code
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_total_movement AS tm
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jclf ON tm.from_job_class_sid = jclf.job_class_sid
     AND date(jclf.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jclt ON tm.to_job_class_sid = jclt.job_class_sid
     AND date(jclt.valid_to_date) = '9999-12-31'
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jobcof ON tm.from_job_code_sid = jobcof.job_code_sid
     AND date(jobcof.valid_to_date) = '9999-12-31'
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jobcot ON tm.to_job_code_sid = jobcot.job_code_sid
     AND date(jobcot.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS staf ON tm.from_auxiliary_status_sid = staf.status_sid
     AND upper(staf.status_type_code) = 'AUX'
     AND date(staf.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stato ON tm.to_auxiliary_status_sid = stato.status_sid
     AND upper(stato.status_type_code) = 'AUX'
     AND date(stato.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facf ON tm.from_coid = facf.coid
     AND tm.from_company_code = facf.company_code
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS facto ON tm.to_coid = facto.coid
     AND tm.to_company_code = facto.company_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS deptf ON tm.from_dept_sid = deptf.dept_sid
     AND date(deptf.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS deptt ON tm.to_dept_sid = deptt.dept_sid
     AND date(deptt.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS plf ON tm.from_process_level_code = plf.process_level_code
     AND tm.from_lawson_company_num = plf.lawson_company_num
     AND date(plf.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS plt ON tm.to_process_level_code = plt.process_level_code
     AND tm.to_lawson_company_num = plt.lawson_company_num
     AND date(plt.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobf ON tm.from_position_sid = jobf.position_sid
     AND tm.date_id BETWEEN jobf.eff_from_date AND jobf.eff_to_date
     AND date(jobf.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobt ON tm.to_position_sid = jobt.position_sid
     AND tm.date_id BETWEEN jobt.eff_from_date AND jobt.eff_to_date
     AND date(jobt.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emp ON tm.employee_sid = emp.employee_sid
     AND date(emp.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdrf ON tm.from_coid = hrdrf.coid
     AND tm.from_process_level_code = hrdrf.process_level_code
     AND tm.from_dept_sid = hrdrf.dept_sid
     AND tm.from_lawson_company_num = hrdrf.lawson_company_num
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdrt ON tm.to_coid = hrdrt.coid
     AND tm.to_process_level_code = hrdrt.process_level_code
     AND tm.to_dept_sid = hrdrt.dept_sid
     AND tm.to_lawson_company_num = hrdrt.lawson_company_num
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON hrdrt.process_level_code = c.process_level_code
     AND hrdrt.lawson_company_num = c.lawson_company_num
     AND c.user_id = session_user()
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_requisition AS emp_req ON tm.employee_sid = emp_req.employee_sid
     AND tm.date_id = emp_req.eff_from_date
     AND tm.action_code = emp_req.action_code
     AND upper(emp_req.delete_ind) <> 'D'
     AND date(emp_req.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN (
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
           AND date(s.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer AS o ON s.submission_sid = o.submission_sid
           AND date(o.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status AS os ON o.offer_sid = os.offer_sid
           AND date(os.valid_to_date) = '9999-12-31'
        WHERE os.offer_status_id IN(
          10, 1010
        )
         AND date(recr.valid_to_date) = '9999-12-31'
        QUALIFY row_number() OVER (PARTITION BY recr.lawson_requisition_sid ORDER BY o.last_modified_date DESC, o.capture_date DESC, o.sequence_num DESC) = 1
    ) AS offer ON emp_req.requisition_sid = offer.lawson_requisition_sid
    LEFT OUTER JOIN (
      SELECT DISTINCT
          rr.recruitment_requisition_sid,
          o.active,
          ng_flag.grad_flag,
          ng_flag.grad_flagb,
          ng.new_grad,
          ng.new_gradb
        FROM
          {{ params.param_hr_base_views_dataset_name }}.offer_status AS os
          LEFT OUTER JOIN (
            SELECT
                ao.offer_sid,
                ao.submission_sid,
                rank() OVER (PARTITION BY ao.submission_sid ORDER BY ao.sequence_num DESC) AS active
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer AS ao
              WHERE date(ao.valid_to_date) = '9999-12-31'
          ) AS o ON os.offer_sid = o.offer_sid
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s ON o.submission_sid = s.submission_sid
           AND date(s.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp ON s.candidate_profile_sid = cp.candidate_profile_sid
           AND date(cp.valid_to_date) = '9999-12-31'
          INNER JOIN -- --start rr join
          (
            SELECT
                recr.recruitment_requisition_sid
              FROM
                {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS recr
                LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s_0 ON recr.recruitment_requisition_sid = s_0.recruitment_requisition_sid
                 AND date(s_0.valid_to_date) = '9999-12-31'
                LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer AS o_0 ON s_0.submission_sid = o_0.submission_sid
                 AND date(o_0.valid_to_date) = '9999-12-31'
                LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status AS os_0 ON os_0.offer_sid = o_0.offer_sid
                 AND date(os_0.valid_to_date) = '9999-12-31'
                LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_offer_status AS ros ON ros.offer_status_id = os_0.offer_status_id
              WHERE date(recr.valid_to_date) = '9999-12-31'
              QUALIFY row_number() OVER (PARTITION BY recr.lawson_requisition_sid ORDER BY o_0.last_modified_date DESC, o_0.capture_date DESC, recr.approved_sw DESC, recr.recruitment_requisition_sid DESC, o_0.sequence_num DESC) = 1
          ) AS rr ON s.recruitment_requisition_sid = rr.recruitment_requisition_sid
          LEFT OUTER JOIN -- --end rr join
          (
            SELECT
                od.element_detail_type_text,
                od.element_detail_id AS new_grad,
                od.offer_sid,
                od.element_detail_value_text AS new_gradb
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE od.element_detail_type_text IN(
                'New - Grad Nurse Recruitment Tracking', 'GraduateNurseRecruitmentTracking', 'RN_Training_Program'
              )
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 2, 4, 3, 1
          ) AS ng ON o.offer_sid = ng.offer_sid
          LEFT OUTER JOIN (
            SELECT
                od.source_system_code,
                od.element_detail_type_text,
                od.element_detail_id AS grad_flag,
                od.offer_sid,
                od.element_detail_value_text AS grad_flagb
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE od.element_detail_type_text IN(
                'New - Grad Nurse Recruitment Tracking', 'New - Grad Nurse Recruitment Tracking', 'GraduateNurseRecruitmentTracking'
              )
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 1, 3, 5, 4, 2
          ) AS ng_flag ON o.offer_sid = ng_flag.offer_sid
        WHERE os.offer_status_id IN(
          10, 1010
        )
         AND date(os.valid_to_date) = '9999-12-31'
    ) AS offer_link_cte ON offer.recruitment_requisition_sid = offer_link_cte.recruitment_requisition_sid
     AND offer_link_cte.active = 1
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.bi_employee_detail AS empdet ON tm.employee_sid = empdet.employee_sid
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_director AS ndirf ON jobcof.job_code = ndirf.job_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_care_position AS pctf ON jobf.position_code_desc = pctf.job_title_text
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_director AS ndirt ON jobcot.job_code = ndirt.job_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_care_position AS pctt ON jobt.position_code_desc = pctt.job_title_text
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS fhrc ON jobf.schedule_work_code = fhrc.hr_code
     AND upper(fhrc.hr_type_code) = 'WS'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS thrc ON jobt.schedule_work_code = thrc.hr_code
     AND upper(thrc.hr_type_code) = 'WS'
    LEFT OUTER JOIN (
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
        WHERE date(hreh.valid_to_date) = '9999-12-31'
         AND hreh.lawson_element_num = 25
        QUALIFY row_number() OVER (PARTITION BY hreh.employee_num, hreh.lawson_element_num, eff_from ORDER BY eff_from, hreh.sequence_num DESC) = 1
    ) AS anniv_hist ON tm.employee_sid = anniv_hist.employee_sid
     AND tm.date_id BETWEEN anniv_hist.eff_from AND anniv_hist.eff_to
  WHERE tm.analytics_msr_sid IN(
    80700
  )
;
