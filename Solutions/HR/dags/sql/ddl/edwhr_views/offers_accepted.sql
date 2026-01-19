CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.offers_accepted AS SELECT
    hrmet.coid,
    hrmet.company_code,
    hrmet.requisition_sid,
    rr.recruitment_requisition_num_text AS taleo_requisition_num,
    rr.recruitment_requisition_sid,
    hrmet.process_level_code,
    rr.lawson_requisition_num,
    ff.group_name,
    ff.market_name,
    ff.division_name,
    rll.location_desc AS facility_name,
    hrmet.location_code,
    rho.business_unit_name AS hrops_business_unit_name,
    rho.business_unit_segment_name AS hrops_business_segment_name,
    jcl.job_class_desc AS skill_mix_name,
    hrmet.recruiter_owner_user_sid,
    max(concat(recuser.last_name, ', ', recuser.first_name)) AS recruiter_name,
    hrmet.position_sid,
    concat(hrmet.position_sid, jobpos.eff_to_date) AS position_key,
    rj.job_title_name,
    rjs.job_schedule_desc,
    hrmet.work_schedule_code,
    offer_link.extend_date,
    hrmet.date_id AS offer_accepted_date,
    offer_link.start_date,
    offer_link.last_modified_date AS last_status_change_date,
    substep.step_name AS current_submission_step_name,
    status.submission_status_name AS current_submission_status_name,
    hrmet.job_code_sid,
    hrmet.functional_dept_num,
    fd.functional_dept_desc,
    hrmet.sub_functional_dept_num,
    fd.sub_functional_dept_desc,
    gldc.dept_num,
    hrmet.dept_sid,
    max(CASE
      WHEN upper(hrmet.work_schedule_code) LIKE '1%' THEN 'Days'
      WHEN upper(hrmet.work_schedule_code) LIKE '2%' THEN 'Eves'
      WHEN upper(hrmet.work_schedule_code) = 'SECONDPRN' THEN 'Eves'
      WHEN upper(hrmet.work_schedule_code) LIKE '3%' THEN 'Nights'
      WHEN upper(hrmet.work_schedule_code) LIKE 'X%' THEN 'Mixed'
      WHEN upper(hrmet.work_schedule_code) = 'VARY' THEN 'Mixed'
      WHEN upper(hrmet.work_schedule_code) = 'WFH' THEN 'WFH'
      ELSE 'Unknown'
    END) AS shift_desc,
    max(concat(recuser2.last_name, ', ', recuser2.first_name)) AS hiring_manager_name,
    hrmet.requisition_approval_date,
    refkt.key_talent_group_text,
    max(CASE
      WHEN jc.job_code_desc = pct.job_code_desc
       AND jobpos.position_code_desc = pct.job_title_text THEN 'PCT'
      ELSE 'No'
    END) AS pct_ind,
    max(CASE
      WHEN stas.status_code = '01' THEN 'FT'
      WHEN stas.status_code = '02' THEN 'PT'
      WHEN stas.status_code = '03' THEN 'PRN'
      WHEN stas.status_code = '04' THEN 'TEMP'
      ELSE CAST(NULL as STRING)
    END) AS emp_status,
    ff.lob_code,
    ilob1.category_desc AS ilob_category_desc,
    ilob1.sub_category_desc AS ilob_sub_category_desc,
    d.dept_name,
    rd.dept_code,
    hrmet.date_id - hrmet.requisition_approval_date AS time_to_fill,
    rs.status_code,
    postdate.creation_date_time AS first_posted_date,
    c.candidate_num AS taleo_candidate_id,
    max(concat(person.last_name, ', ', person.first_name)) AS candidate_name,
    person.email_address AS candidate_email,
    max(CASE
      WHEN rr.iec = 1 THEN 'External'
      WHEN rr.iec = 2 THEN 'Ext Contract To Perm'
      WHEN rr.iec = 3 THEN 'Internal HCA'
      WHEN rr.iec = 4 THEN 'Internal Same Division'
      WHEN rr.iec = 5 THEN 'Internal Same Facility'
      WHEN rr.iec = 6 THEN 'Internal Same ILOB'
      WHEN upper(rr.iecb) = 'INTERNALHCA' THEN 'Internal HCA'
      WHEN upper(rr.iecb) = 'EXTERNAL' THEN 'External'
      WHEN upper(rr.iecb) = 'INTERNALSAMEINTEGRATEDLINEOFBUSINESS' THEN 'Internal Same ILOB'
      WHEN upper(rr.iecb) = 'INTERNALSAMEFACILITY' THEN 'Internal Same Facility'
      WHEN upper(rr.iecb) = 'INTERNALSAMEDIVISION' THEN 'Internal Same Division'
      WHEN upper(rr.iecb) = 'EXTERNALCONTRACTTOPERM' THEN 'Ext Contract To Perm'
      ELSE 'Unknown'
    END) AS offer_ie_desc,
    max(coalesce(rr.new_gradb, CASE
      WHEN rr.new_grad = 12320 THEN 'Division/Market Program'
      WHEN rr.new_grad = 12340 THEN 'No Formal Program'
      WHEN upper(rr.new_gradb) = 'NOFORMALRNTRAININGPROGRAM' THEN 'No Formal Program'
      WHEN rr.new_grad = 12321 THEN 'Facility Program'
      WHEN rr.new_grad = 12300 THEN 'Not Applicable'
      WHEN rr.new_grad = 12341 THEN 'StaRN'
      ELSE 'NULL'
    END)) AS taleo_new_grad_desc,
    max(CASE WHEN rr.grad_program = 12301 THEN 'NeonatalIntensiveCareUnit'
      WHEN rr.grad_program = 12302 THEN 'Obstetrics'
      WHEN rr.grad_program = 12303 THEN 'Telemetry'
      WHEN rr.grad_program = 12322 THEN 'BehavioralHealth'
      WHEN rr.grad_program = 12323 THEN 'EmergencyRoom'
      WHEN rr.grad_program= 12324 THEN 'IntensiveCareUnit'
      WHEN rr.grad_program = 12342 THEN 'NotApplicable'
      WHEN rr.grad_program = 12343 THEN 'LaborAndDelivery'
      WHEN rr.grad_program = 12344 THEN 'MedSurg'
      WHEN rr.grad_program = 12345 THEN 'OperatingRoom'
      ELSE rr.grad_programb 
    END) AS ng_program_type,
    max(coalesce(rr.grad_flagb, CASE
      WHEN rr.new_grad = 12320 THEN 'Y'
      WHEN rr.new_grad = 12340 THEN 'Y'
      WHEN rr.new_grad = 12321 THEN 'Y'
      WHEN rr.new_grad = 12341 THEN 'Y'
      ELSE 'N'
    END)) AS new_grad_desc,
    coalesce(rr.rn_acute_exp_dateb, rr.rn_acute_exp_date) AS rn_acute_exp_date,
    coalesce(rr.job_experience_dateb, rr.job_experience_date) AS job_experience_date,
    coalesce(rr.onboarding_triggerb, cast(rr.onboarding_trigger as string)) AS onboarding_trigger_desc,
    co.onboarding_confirmation_date AS confirmation_date,
    round2_start.min_step_start AS round2_date
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS hrmet
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS r ON hrmet.requisition_sid = r.requisition_sid
     AND date(r.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_department AS rd ON r.requisition_sid = rd.requisition_sid
     AND date(rd.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS d ON rd.dept_sid = d.dept_sid
     AND date(d.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN edwhr_views.hr_dept_rollup AS hrdr ON hrmet.process_level_code = hrdr.process_level_code
     AND hrmet.dept_sid = hrdr.dept_sid
     AND hrmet.lawson_company_num = hrdr.lawson_company_num
     AND hrmet.coid = hrdr.coid
    LEFT OUTER JOIN (
      SELECT
          recr.recruitment_requisition_sid,
          recr.lawson_requisition_sid,
          recr.recruitment_requisition_num_text,
          recr.lawson_requisition_num,
          recr.requisition_num,
          recr.recruitment_job_sid,
          recr.hiring_manager_user_sid,
          recr.source_system_code,
          o.accept_date,
          o.offer_sid,
          recr.process_level_code,
          ros.offer_status_desc,
          recr.approved_sw,
          s.submission_sid,
          recr.workflow_id,
          cp.completion_date,
          s.current_submission_status_id,
          s.current_submission_step_id,
          o.extend_date,
          o.start_date,
          o.active,
          s.last_modified_date,
          s.candidate_profile_sid,
          iec.iec,
          iec.iecb,
          ng.new_grad,
          ng.new_gradb,
          gp.grad_program,
          gp.grad_programb,
          jed.job_experience_date,
          jed.job_experience_dateb,
          rnex.rn_acute_exp_date,
          rnex.rn_acute_exp_dateb,
          onbdtrg.onboarding_trigger,
          onbdtrg.onboarding_triggerb,
          ng_flag.grad_flag,
          ng_flag.grad_flagb,
          nursing_license.rn_licenseb
        FROM
          {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS recr
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s ON recr.recruitment_requisition_sid = s.recruitment_requisition_sid
           AND date(s.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN (
            SELECT
                ao.offer_sid,
                ao.submission_sid,
                ao.sequence_num,
                ao.extend_date,
                ao.start_date,
                ao.valid_to_date,
                ao.accept_date,
                ao.last_modified_date,
                ao.capture_date,
                rank() OVER (PARTITION BY ao.submission_sid ORDER BY ao.sequence_num DESC) AS active
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer AS ao
              WHERE date(ao.valid_to_date) = '9999-12-31'
          ) AS o ON s.submission_sid = o.submission_sid
           AND date(o.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status AS os ON os.offer_sid = o.offer_sid
           AND date(os.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_offer_status AS ros ON ros.offer_status_id = os.offer_status_id
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp ON s.candidate_profile_sid = cp.candidate_profile_sid
           AND date(cp.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN (
            SELECT
                od.element_detail_id AS iec,
                od.element_detail_value_text AS iecb,
                od.offer_sid
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE upper(od.element_detail_type_text) = 'INTERNAL_EXTERNAL_CANDIDATE'
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 1, 3, 2
          ) AS iec ON o.offer_sid = iec.offer_sid
          LEFT OUTER JOIN (
            SELECT
                od.element_detail_id AS onboarding_trigger,
                od.element_detail_value_text AS onboarding_triggerb,
                od.offer_sid
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE upper(od.element_detail_type_text) = 'ONBOARDING REQUIRED FOR HIRED CANDIDATE OFFER'
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 1, 2, 3
          ) AS onbdtrg ON o.offer_sid = onbdtrg.offer_sid
          LEFT OUTER JOIN (
            SELECT
                od.offer_sid,
                od.element_detail_value_text AS job_experience_date,
                od.element_detail_value_text AS job_experience_dateb
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE upper(od.element_detail_type_text) = 'JOB_EXPERIENCE_DATE'
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 3, 3, 1
          ) AS jed ON o.offer_sid = jed.offer_sid
          LEFT OUTER JOIN (
            SELECT
                od.offer_sid,
                od.element_detail_value_text AS rn_acute_exp_date,
                od.element_detail_value_text AS rn_acute_exp_dateb
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE upper(od.element_detail_type_text) = 'RN_ACUTE_EXP_DATE'
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 3, 3, 1
          ) AS rnex ON o.offer_sid = rnex.offer_sid
          LEFT OUTER JOIN (
            SELECT
                od.element_detail_id AS new_grad,
                od.offer_sid,
                od.element_detail_value_text AS new_gradb,
                rank() OVER (PARTITION BY od.offer_sid ORDER BY od.element_detail_id DESC) AS ngrank
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE od.element_detail_type_text IN(
                'New - Grad Nurse Recruitment Tracking', 'GraduateNurseRecruitmentTracking', 'RN_Training_Program'
              )
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 2, 3, 1, od.valid_from_date, 1
          ) AS ng ON o.offer_sid = ng.offer_sid
           AND ng.ngrank = 1
          LEFT OUTER JOIN (
            SELECT
                od.element_detail_type_text,
                od.element_detail_id AS rn_license,
                od.offer_sid,
                od.element_detail_value_text AS rn_licenseb
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE upper(od.element_detail_type_text) = 'RN_LICENSE_TYPE'
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 2, 4, 3, 1
          ) AS nursing_license ON o.offer_sid = nursing_license.offer_sid
          LEFT OUTER JOIN (
            SELECT
                od.source_system_code,
                od.element_detail_type_text,
                od.element_detail_id AS grad_flag,
                od.offer_sid,
                od.element_detail_value_text AS grad_flagb,
                rank() OVER (PARTITION BY od.offer_sid ORDER BY od.element_detail_id DESC) AS ngbrank
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE upper(od.element_detail_type_text) IN(
                'NEW - GRAD NURSE RECRUITMENT TRACKING', 'NEW - GRAD NURSE RECRUITMENT TRACKING', 'GRADUATENURSERECRUITMENTTRACKING'
              )
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 1, 3, 5, 4, 2
          ) AS ng_flag ON o.offer_sid = ng_flag.offer_sid
           AND ng_flag.ngbrank = 1
          LEFT OUTER JOIN (
            SELECT
                od.element_detail_id AS grad_program,
                od.offer_sid,
                od.element_detail_value_text AS grad_programb
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE upper(od.element_detail_type_text) IN(
                'NEWPROGRAMSPECIALTY'
              )
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 1, 3, 2
          ) AS gp ON o.offer_sid = gp.offer_sid
        WHERE date(recr.valid_to_date) = '9999-12-31'
         AND os.offer_status_id IN(
          10, 1010
        )
         AND date(os.valid_to_date) = '9999-12-31'
        QUALIFY row_number() OVER (PARTITION BY recr.lawson_requisition_sid ORDER BY o.capture_date DESC, recr.approved_sw DESC, recr.recruitment_requisition_sid DESC, o.sequence_num DESC) = 1
    ) AS rr ON r.requisition_sid = rr.lawson_requisition_sid
    LEFT OUTER JOIN (
      SELECT
          recruitment_requisition_history.creation_date_time,
          recruitment_requisition_history.source_system_code,
          recruitment_requisition_history.recruitment_requisition_sid,
          rank() OVER (PARTITION BY recruitment_requisition_history.recruitment_requisition_sid ORDER BY recruitment_requisition_history.creation_date_time) AS firstsourcing
        FROM
          {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history
        WHERE recruitment_requisition_history.requisition_status_id = 13
         AND upper(recruitment_requisition_history.source_system_code) = 'T'
    ) AS postdate ON rr.recruitment_requisition_sid = postdate.recruitment_requisition_sid
     AND rr.source_system_code = postdate.source_system_code
     AND postdate.firstsourcing = 1
    LEFT OUTER JOIN (
      SELECT
          req.recruitment_requisition_sid,
          req.source_system_code,
          req.posting_date,
          rank() OVER (PARTITION BY req.recruitment_requisition_sid ORDER BY req.posting_date) AS firstposted
        FROM
          edwhr_views.sourcing_request AS req
        WHERE date(req.valid_to_date) = '9999-12-31'
         AND upper(req.source_system_code) = 'B'
         AND req.posting_date IS NOT NULL
    ) AS postdate2 ON rr.recruitment_requisition_sid = postdate2.recruitment_requisition_sid
     AND rr.source_system_code = postdate2.source_system_code
     AND postdate2.firstposted = 1
    LEFT OUTER JOIN (
      SELECT
          recruitment_requisition_status.valid_from_date AS open_date,
          recruitment_requisition_status.recruitment_requisition_sid,
          rank() OVER (PARTITION BY recruitment_requisition_status.recruitment_requisition_sid ORDER BY recruitment_requisition_status.valid_from_date DESC) AS openrank
        FROM
          {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_status
        WHERE recruitment_requisition_status.requisition_status_id IN(
          3, 13, 1001
        )
    ) AS opentime ON rr.recruitment_requisition_sid = opentime.recruitment_requisition_sid
     AND opentime.openrank = 1
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj ON rr.recruitment_job_sid = rj.recruitment_job_sid
     AND date(rj.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_job_schedule AS rjs ON rj.job_schedule_id = rjs.job_schedule_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS recuser ON rj.recruiter_user_sid = recuser.recruitment_user_sid
     AND date(recuser.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN -- --------------Join to get Recruiter information
    {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS recuser2 ON rr.hiring_manager_user_sid = recuser2.recruitment_user_sid
     AND date(recuser2.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN -- --------------Join to get HM information
    {{ params.param_hr_base_views_dataset_name }}.requisition_status AS rs ON hrmet.requisition_sid = rs.requisition_sid
     AND date(rs.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jc ON hrmet.job_code_sid = jc.job_code_sid
     AND date(jc.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jcl ON jc.job_class_sid = jcl.job_class_sid
     AND date(jcl.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_position AS reqpos ON r.requisition_sid = reqpos.requisition_sid
     AND date(reqpos.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobpos ON reqpos.position_sid = jobpos.position_sid
     AND date(jobpos.valid_to_date) = '9999-12-31'
     AND hrmet.date_id BETWEEN jobpos.eff_from_date AND jobpos.eff_to_date
    LEFT OUTER JOIN -- --AND Jobpos.eff_to_Date = '9999-12-31'
    {{ params.param_hr_base_views_dataset_name }}.ref_patient_care_position AS pct ON jobpos.position_code_desc = pct.job_title_text
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS gldc ON jobpos.account_unit_num = gldc.account_unit_num
     AND jobpos.gl_company_num = gldc.gl_company_num
     AND jobpos.process_level_code = gldc.process_level_code
     AND date(gldc.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN edw_pub_views.functional_sub_functional_department AS fd ON gldc.dept_num = fd.dept_num
     AND gldc.coid = fd.coid
     AND gldc.company_code = fd.company_code
    LEFT OUTER JOIN edw_pub_views.fact_facility AS ff ON gldc.company_code = ff.company_code
     AND gldc.coid = ff.coid
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_location AS rll ON r.location_code = rll.location_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stas ON r.application_status_sid = stas.status_sid
     AND date(stas.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN /*Ilob mapping Start*/
    {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS ilob1 ON hrmet.integrated_lob_id = ilob1.integrated_lob_id
    LEFT OUTER JOIN /*ilob mapping end*/
    /*Join the Business Unit*/
    {{ params.param_hr_base_views_dataset_name }}.ref_hr_operations AS rho ON hrmet.process_level_code = rho.process_level_code
    LEFT OUTER JOIN /*Join the Business Unit END*/
    /*Key Talent Join*/
    {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS refkt ON hrmet.key_talent_id = refkt.key_talent_id
    LEFT OUTER JOIN /*Key Talent Join END*/
    (
      SELECT DISTINCT
          rr_0.recruitment_requisition_sid,
          s.submission_sid,
          s.current_submission_status_id,
          s.current_submission_step_id,
          o.extend_date,
          o.start_date,
          o.offer_sid,
          s.last_modified_date,
          s.candidate_profile_sid,
          iec.iec,
          iec.iecb,
          ng.new_grad,
          ng.new_gradb,
          gp.grad_program,
          gp.grad_programb,
          jed.job_experience_date,
          jed.job_experience_dateb,
          rnex.rn_acute_exp_date,
          rnex.rn_acute_exp_dateb,
          onbdtrg.onboarding_trigger,
          onbdtrg.onboarding_triggerb
        FROM
          {{ params.param_hr_base_views_dataset_name }}.offer_status AS os
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer AS o ON os.offer_sid = o.offer_sid
           AND date(o.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s ON o.submission_sid = s.submission_sid
           AND date(s.valid_to_date) = '9999-12-31'
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rr_0 ON s.recruitment_requisition_sid = rr_0.recruitment_requisition_sid
           AND date(rr_0.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN (
            SELECT
                od.element_detail_id AS iec,
                od.element_detail_value_text AS iecb,
                od.offer_sid
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE upper(od.element_detail_type_text) = 'INTERNAL_EXTERNAL_CANDIDATE'
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 1, 3, 2
          ) AS iec ON o.offer_sid = iec.offer_sid
          LEFT OUTER JOIN (
            SELECT
                od.element_detail_id AS onboarding_trigger,
                od.element_detail_value_text AS onboarding_triggerb,
                od.offer_sid
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE upper(od.element_detail_type_text) = 'ONBOARDING REQUIRED FOR HIRED CANDIDATE OFFER'
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 1, 2, 3
          ) AS onbdtrg ON o.offer_sid = onbdtrg.offer_sid
          LEFT OUTER JOIN (
            SELECT
                od.offer_sid,
                od.element_detail_value_text AS job_experience_date,
                od.element_detail_value_text AS job_experience_dateb
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE upper(od.element_detail_type_text) = 'JOB_EXPERIENCE_DATE'
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 3, 3, 1
          ) AS jed ON o.offer_sid = jed.offer_sid
          LEFT OUTER JOIN (
            SELECT
                od.offer_sid,
                od.element_detail_value_text AS rn_acute_exp_date,
                od.element_detail_value_text AS rn_acute_exp_dateb
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE upper(od.element_detail_type_text) = 'RN_ACUTE_EXP_DATE'
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 3, 3, 1
          ) AS rnex ON o.offer_sid = rnex.offer_sid
          LEFT OUTER JOIN (
            SELECT
                od.element_detail_id AS new_grad,
                od.offer_sid,
                od.element_detail_value_text AS new_gradb
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE od.element_detail_type_text IN(
                'New - Grad Nurse Recruitment Tracking', 'GraduateNurseRecruitmentTracking'
              )
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 1, 3, 2
          ) AS ng ON o.offer_sid = ng.offer_sid
          LEFT OUTER JOIN (
            SELECT
                od.element_detail_id AS grad_program,
                od.offer_sid,
                od.element_detail_value_text AS grad_programb
              FROM
                {{ params.param_hr_base_views_dataset_name }}.offer_detail AS od
              WHERE od.element_detail_type_text IN(
                'NewProgramSpecialty'
              )
               AND date(od.valid_to_date) = '9999-12-31'
              GROUP BY 1, 3, 2
          ) AS gp ON o.offer_sid = gp.offer_sid
        WHERE os.offer_status_id IN(
          10, 1010
        )
         AND date(os.valid_to_date) = '9999-12-31'
    ) AS offer_link ON rr.recruitment_requisition_sid = offer_link.recruitment_requisition_sid
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp ON offer_link.candidate_profile_sid = cp.candidate_profile_sid
     AND date(cp.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS c ON cp.candidate_sid = c.candidate_sid
     AND date(c.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_person AS person ON c.candidate_sid = person.candidate_sid
     AND date(person.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_status AS status ON offer_link.current_submission_status_id = status.submission_status_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_step AS substep ON offer_link.current_submission_step_id = substep.step_id
    LEFT OUTER JOIN -- -- Add confirmation date
    (
      SELECT DISTINCT
          ob.requisition_sid,
          ob.candidate_sid,
          ob.process_level_code,
          max(ob.onboarding_confirmation_date) AS onboarding_confirmation_date
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding AS ob
        WHERE date(ob.valid_to_date) = '9999-12-31'
        GROUP BY 1, 2, 3
    ) AS co ON hrmet.requisition_sid = co.requisition_sid
     AND hrmet.process_level_code = co.process_level_code
     AND cp.candidate_sid = co.candidate_sid
    LEFT OUTER JOIN -- --Add Round 2 Date
    (
      SELECT DISTINCT
          fin.recruitment_requisition_sid,
          fin.candidate_profile_sid,
          min(fin.event_start) AS min_step_start,
          max(fin.event_end) AS max_step_end
        FROM
          (
            SELECT
                recruitment_requisition_sid,
                sub.candidate_profile_sid,
                submission_sid,
                step_short_name,
                coalesce(event_date_time, creation_date_time) AS event_start,
                lag(coalesce(event_date_time, creation_date_time), 1) OVER (PARTITION BY str.candidate_profile_sid ORDER BY coalesce(event_date_time, creation_date_time) DESC) AS event_end
              FROM
                {{ params.param_hr_base_views_dataset_name }}.submission AS sub
                LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission_tracking AS str ON sub.candidate_profile_sid = str.candidate_profile_sid
                 AND date(str.valid_to_date) = '9999-12-31'
                LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_submission_step AS st ON str.tracking_step_id = st.step_id
              WHERE date(str.valid_to_date) = '9999-12-31'
               AND date(sub.valid_to_date) = '9999-12-31'
               AND sub.recruitment_requisition_sid IS NOT NULL
               AND str.tracking_step_id IS NOT NULL
          ) AS fin
        WHERE upper(fin.step_short_name) = 'ROUND 2'
        GROUP BY 1, 2
    ) AS round2_start ON rr.recruitment_requisition_sid = round2_start.recruitment_requisition_sid
     AND cp.candidate_profile_sid = round2_start.candidate_profile_sid
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS hrs ON hrmet.process_level_code = hrs.process_level_code
     AND hrmet.lawson_company_num = hrs.lawson_company_num
     AND hrs.user_id = session_user()
  WHERE hrmet.analytics_msr_sid = 80500
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, upper(concat(recuser.last_name, ', ', recuser.first_name)), 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, upper(CASE
    WHEN upper(hrmet.work_schedule_code) LIKE '1%' THEN 'Days'
    WHEN upper(hrmet.work_schedule_code) LIKE '2%' THEN 'Eves'
    WHEN upper(hrmet.work_schedule_code) = 'SECONDPRN' THEN 'Eves'
    WHEN upper(hrmet.work_schedule_code) LIKE '3%' THEN 'Nights'
    WHEN upper(hrmet.work_schedule_code) LIKE 'X%' THEN 'Mixed'
    WHEN upper(hrmet.work_schedule_code) = 'VARY' THEN 'Mixed'
    WHEN upper(hrmet.work_schedule_code) = 'WFH' THEN 'WFH'
    ELSE 'Unknown'
  END), upper(concat(recuser2.last_name, ', ', recuser2.first_name)), 38, 39, upper(CASE
    WHEN jc.job_code_desc = pct.job_code_desc
     AND jobpos.position_code_desc = pct.job_title_text THEN 'PCT'
    ELSE 'No'
  END), upper(CASE
    WHEN stas.status_code = '01' THEN 'FT'
    WHEN stas.status_code = '02' THEN 'PT'
    WHEN stas.status_code = '03' THEN 'PRN'
    WHEN stas.status_code = '04' THEN 'TEMP'
    ELSE CAST(NULL as STRING)
  END), 42, 43, 44, 45, 46, 47, 48, 49, 50, upper(concat(person.last_name, ', ', person.first_name)), 52, upper(CASE
    WHEN rr.iec = 1 THEN 'External'
    WHEN rr.iec = 2 THEN 'Ext Contract To Perm'
    WHEN rr.iec = 3 THEN 'Internal HCA'
    WHEN rr.iec = 4 THEN 'Internal Same Division'
    WHEN rr.iec = 5 THEN 'Internal Same Facility'
    WHEN rr.iec = 6 THEN 'Internal Same ILOB'
    WHEN upper(rr.iecb) = 'INTERNALHCA' THEN 'Internal HCA'
    WHEN upper(rr.iecb) = 'EXTERNAL' THEN 'External'
    WHEN upper(rr.iecb) = 'INTERNALSAMEINTEGRATEDLINEOFBUSINESS' THEN 'Internal Same ILOB'
    WHEN upper(rr.iecb) = 'INTERNALSAMEFACILITY' THEN 'Internal Same Facility'
    WHEN upper(rr.iecb) = 'INTERNALSAMEDIVISION' THEN 'Internal Same Division'
    WHEN upper(rr.iecb) = 'EXTERNALCONTRACTTOPERM' THEN 'Ext Contract To Perm'
    ELSE 'Unknown'
  END), upper(coalesce(rr.new_gradb, CASE
    WHEN rr.new_grad = 12320 THEN 'Division/Market Program'
    WHEN rr.new_grad = 12340 THEN 'No Formal Program'
    WHEN upper(rr.new_gradb) = 'NOFORMALRNTRAININGPROGRAM' THEN 'No Formal Program'
    WHEN rr.new_grad = 12321 THEN 'Facility Program'
    WHEN rr.new_grad = 12300 THEN 'Not Applicable'
    WHEN rr.new_grad = 12341 THEN 'StaRN'
    ELSE 'NULL'
  END)), upper(CASE
      WHEN rr.grad_program = 12301 THEN 'Neonatal ICU'
      WHEN rr.grad_program = 12302 THEN 'Obstetrics'
      WHEN rr.grad_program = 12303 THEN 'Telemetry'
      WHEN rr.grad_program = 12322 THEN 'Behavioral Health'
      WHEN rr.grad_program = 12323 THEN 'Emergency Room'
      WHEN rr.grad_program = 12324 THEN 'Intensive Care'
      WHEN rr.grad_program = 12342 THEN 'Not Applicable'
      WHEN rr.grad_program = 12343 THEN 'Labor & Delivery'
      WHEN rr.grad_program = 12344 THEN 'Med/Surg'
      WHEN rr.grad_program = 12345 THEN 'Operating Room'
    WHEN upper(rr.grad_programb) = 'OBSTETRICS' THEN 'Obstetrics'
    WHEN upper(rr.grad_programb) = 'EMERGENCYROOM' THEN 'Emergency Room'
    WHEN upper(rr.grad_programb) = 'LABORANDDELIVERY' THEN 'Labor & Delivery'
    WHEN upper(rr.grad_programb) = 'MEDSURG' THEN 'Med/Surg'
    WHEN upper(rr.grad_programb) = 'NEONATALINTENSIVECAREUNIT' THEN 'Neonatal ICU'
    WHEN upper(rr.grad_programb) = 'OPERATINGROOM' THEN 'Operating Room'
    WHEN upper(rr.grad_programb) = 'INTENSIVECAREUNIT' THEN 'Intensive Care'
    WHEN upper(rr.grad_programb) = 'BEHAVIORALHEALTH' THEN 'Behavioral Health'
    ELSE 'NULL'
  END), upper(coalesce(rr.grad_flagb, CASE
    WHEN rr.new_grad = 12320 THEN 'Y'
    WHEN rr.new_grad = 12340 THEN 'Y'
    WHEN rr.new_grad = 12321 THEN 'Y'
    WHEN rr.new_grad = 12341 THEN 'Y'
    ELSE 'N'
  END)), 57, 58, 59, 60, 61
;
