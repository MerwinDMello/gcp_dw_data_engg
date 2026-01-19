create or replace view {{ params.param_hr_bi_views_dataset_name }}.factfills_daily as 
    --  =============================================
    --  Author:        Cheryl Costa
    --  Create date:    09/13/2019
    -- Update Date:  02/12/2020 to add the First Posted Date
    -- -Updated to correct error in the Position join
    -- -Updated 03/27/2020 to include Offer Attribute (Int Ext Ind) Remove UID and clean up Status)
    -- -Updated 4/1/2020 to include the New_Grad flag from Taleo.  TFS # 1414
    -- -Updated 4/30/2020 to correct Offer_Link, Add Experience Dates, and update ILOB Join TFS #1555.
    --  Updated 6/8/2020 o correct Offer_Link, Add Experience Dates, and update ILOB Join TFS #1555 and lost updates
    -- -Updated 8/26/2020 to remove join impacting NCD entries and update req # to pull from r.
    -- --Updated 8/31/2020 to change ff join to use COID from HRMetic fact.
    -- --Updated 9/1/2020 to change jobpos join to use position_SID from HRMetic fact.
    -- --Updated 9/30/2020 to add offer status, posting status, and req status values for ATS.
    -- --Updated 10/16/2020 to connect offer detail for ATS
    -- -Updated 10/22/2020 to correct Posting Date join for ATS (13, 1001).  Removed hold days join and a duplicate offer join.  Simplified TTF calculation.
    -- - Updated 10/27/2020 to add Candidate speicfic data (name etc) for use by KT Team
    -- -Updated 01/20/21 to remove coalesce and add values to the Internal/External indicator case statement for ATS
    -- -Updated 02/16/21 to add pathwaty Num, Name and total SLA
    -- -Updated 3/1 to include min/max/HR Review and Max NonHR Review date
    -- -Updated 3/9/21 to include the Total_Approval_Duration and update Pathway_SLA
    -- -Updated to include min/max/HR Review and Max NonHR Review date
    -- Updated 3/19 to include the case statement on sourcing time
    -- Updated 3/23 to include the hired candidate's submisison completed date
    -- Updated 3/25 to synch versions and add the Total Approval duration back in and update Ref_Pathway Join to exclude subpaths.
    -- Updated 3/26 to add the Round 2 and confirmation date. Replaced step_name with Step_short_name in CTE join.
    -- Updated 4/4/21 to improve TTS calculation
    -- Updated 4/6/21 to add matched candidate indicator, and modify join for R2 date
    -- Updated 4/20/21 to adjust qualify to look to the offer for correct RR Sid.
    -- Updated 4/28/21 to add EEO info on the filled candidate
    -- - Updated 5/3/21 to define ATS program values and add to new grad indicator and source system value for tracking
    -- - Updated 5/17/21 to clean up change pulling date from Snapshot, Date Filter change, and add FTE Value.
    -- -Updated 6/9/21 to add valid to date to CO join and improve offer_link to look only at most recent offer
    -- - Updated 6/15/21 to include ATS training program values and RN License
    -- - Updated 7/7/21 to include Candidate_IE_Ind
    -- -Updated 7/23/21 to put valid to date in the TAD calculation
    -- -Updated 10/8/21 to edit offer lilnk join and combine with rr to get rid of duplicates, combine new grad values
    -- - Updated 10/28/21 to add qualify to NG and NG Flag joins.
    -- -Updated 4/5/22 to add new source for EEO data values.
    -- --Updated 5/24/22 to replace the Veteran Status logic and add DW_Last_Update_Date_Time for TA
    -- -Updated 6/3/22 qualify on the RR Join to eliminate last modified, adjusted confirmation join to pull Max(Confirmation.Date)
    -- -Updated 7/26/22 to modify New Grad Flag case statement adding GHR values.
    ---Updated 2/28/23 to update case statement for RN Program value CCosta
    --  Description: Fills Daily
    -- Metrics:  Fills, TTF
    -- Filters: {{ params.param_hr_base_views_dataset_name }}.fact_HR_Metric.Analytics_Msr_Sid = '80500'
    --  =============================================
  WITH reqstatus as (
    SELECT
      rrh.recruitment_requisition_sid,
      rrh.requisition_status_id,
      rrs.status_desc,
      rrh.creation_date_time AS req_status_start,
      lead(rrh.creation_date_time, 1) OVER(PARTITION BY rrh.recruitment_requisition_sid ORDER BY rrh.creation_date_time) AS req_status_end,
      rrh.source_system_code
    FROM {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history rrh
    LEFT JOIN {{ params.param_hr_base_views_dataset_name }}.ref_requisition_status rrs
      ON rrh.requisition_status_id = rrs.requisition_status_id
    WHERE DATE(rrh.valid_to_date) = '9999-12-31'
  )
  SELECT
    hrmet.date_id AS pe_date,
    hrmet.coid,
    hrmet.requisition_sid,
    max(concat(coalesce(hrdr.coid, '00000'), hrdr.process_level_code, coalesce(hrdr.dept_code, '00000'), hrdr.lawson_company_num)) AS pl_uid,
    concat(coalesce((r.requisition_sid), 00000), coalesce((cp.candidate_profile_sid), 00000)) AS req_app_uid,
    rr.recruitment_requisition_num_text AS taleo_requisition_num,
    rr.recruitment_requisition_sid,
    r.requisition_num AS lawson_requisition_num,
    hrmet.position_sid,
    concat((hrmet.position_sid), jobpos.eff_to_date) AS position_key,
    hrmet.job_code_sid,
    hrmet.location_code,
    hrmet.functional_dept_num,
    hrmet.sub_functional_dept_num,
    hrdr.cost_center,
    hrmet.dept_sid,
    hrmet.process_level_code,
    rjs.job_schedule_desc AS job_schedule,
    r.open_fte_percent AS fte_value,
    hrmet.work_schedule_code,
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
    max(CASE
      WHEN (rr.iec) = 1 THEN 'External'
      WHEN (rr.iec) = 2 THEN 'Ext Contract To Perm'
      WHEN (rr.iec) = 3 THEN 'Internal HCA'
      WHEN (rr.iec) = 4 THEN 'Internal Same Division'
      WHEN (rr.iec) = 5 THEN 'Internal Same Facility'
      WHEN (rr.iec) = 6 THEN 'Internal Same ILOB'
      WHEN upper(rr.iecb) = 'INTERNALHCA' THEN 'Internal HCA'
      WHEN upper(rr.iecb) = 'EXTERNAL' THEN 'External'
      WHEN upper(rr.iecb) = 'INTERNALSAMEINTEGRATEDLINEOFBUSINESS' THEN 'Internal Same ILOB'
      WHEN upper(rr.iecb) = 'INTERNALSAMEFACILITY' THEN 'Internal Same Facility'
      WHEN upper(rr.iecb) = 'INTERNALSAMEDIVISION' THEN 'Internal Same Division'
      WHEN upper(rr.iecb) = 'EXTERNALCONTRACTTOPERM' THEN 'Ext Contract To Perm'
      ELSE 'Unknown'
    END) AS offer_ie_ind,
    coalesce(cast(rr.new_grad as string), rr.new_gradb) AS new_grad_desc,
    max(CASE
      WHEN (rr.new_grad) = 12320 THEN 'Local Program'
      WHEN (rr.new_grad) = 12321 THEN 'Local Program'
      WHEN upper(rr.new_gradb) = 'LOCALRNTRAININGPROGRAM' THEN 'Local Program'
      WHEN (rr.new_grad) = 12340 THEN 'No Formal Program'
      WHEN upper(rr.new_gradb) = 'NOFORMALRNTRAININGPROGRAM' THEN 'No Formal Program'
      WHEN (rr.new_grad) = 12300 THEN 'Not Applicable'
      WHEN upper(rr.new_gradb) = 'HWSRNTRAININGPROGRAM' THEN 'HWS/StaRN'
      WHEN (rr.new_grad) = 12341 THEN 'HWS/StaRN'
      ELSE 'NULL'
    END) AS taleo_new_grad,
    max(coalesce(rr.grad_flagb, CASE
      WHEN (rr.new_grad) = 12320 THEN 'Y'
      WHEN (rr.new_grad) = 12340 THEN 'Y'
      WHEN (rr.new_grad) = 12321 THEN 'Y'
      WHEN (rr.new_grad) = 12341 THEN 'Y'
      ELSE 'N'
    END)) AS new_grad_flag,
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
    coalesce(rr.job_experience_dateb, rr.job_experience_date) AS job_experience_date,
    coalesce(rr.rn_acute_exp_dateb, rr.rn_acute_exp_date) AS rn_acute_exp_date,
    coalesce(cast(rr.onboarding_triggerb as string), cast(rr.onboarding_trigger as string)) AS onboarding_trigger_desc,
    hrmet.recruiter_owner_user_sid,
    max(concat(recuser.last_name, ', ', recuser.first_name)) AS recruiter_name,
    recuser.employee_34_login_code AS recruiter_34,
    max(concat(recuser2.last_name, ', ', recuser2.first_name)) AS hiring_manager_name,
    c.candidate_num AS taleo_candidate_id,
    max(concat(person.last_name, ', ', person.first_name)) AS candidate_name,
    hrmet.requisition_approval_date,
    rr.completion_date AS fill_submission_date,
    rr.extend_date AS offer_extend_date,
    rr.start_date AS offer_start_date,
    hrmet.key_talent_id,
    --  modify to look at only job_title_text for PCTs
    max(CASE
      WHEN upper(trim(jobpos.position_code_desc)) = upper(trim(pct.job_title_text)) THEN 'PCT'
      ELSE 'No'
    END) AS pct,
    max(CASE
      WHEN stas.status_code = '01' THEN 'FT'
      WHEN stas.status_code = '02' THEN 'PT'
      WHEN stas.status_code = '03' THEN 'PRN'
      WHEN stas.status_code = '04' THEN 'TEMP'
      ELSE CAST(NULL as STRING)
    END) AS emp_status,
    hrmet.integrated_lob_id,
    d.dept_name,
    rd.dept_code,
    rs.status_code AS lawson_status,
    hrr_measure.min_step_start AS min_hr_review_date,
    hrr_measure.max_step_end AS max_hr_review_date,
    CASE
      WHEN sst_measure.min_step_start < opentime.open_date THEN CAST(NULL as date)
      ELSE sst_measure.min_step_start
    END AS min_nonhrreview_date,
    opentime.open_date,
    tad2.total_approval_duration,
    CAST(date_diff(hrmet.requisition_approval_date, r.requisition_origination_date, DAY) as FLOAT64) AS tta,
    CAST(date_diff(opentime.open_date, hrmet.requisition_approval_date, DAY) as FLOAT64) AS tto,
    CAST(date_diff(rr.extend_date, hrmet.requisition_approval_date, DAY) as FLOAT64) AS tte,
    CAST(date_diff(hrmet.date_id, hrmet.requisition_approval_date, DAY) as FLOAT64) AS ttf,
    CAST(date_diff(starts.date_id, hrmet.requisition_approval_date, DAY) as FLOAT64) AS tts,
    p.pathway_num,
    p.pathway_name,
    max(CASE
      WHEN p.pathway_num = 1 THEN '15'
      WHEN p.pathway_num = 3 THEN '19'
      WHEN p.pathway_num = 9 THEN '11'
      ELSE CAST(NULL as STRING)
    END) AS path_sla,
    coalesce(postdate.creation_date_time, postdate2.posting_date) AS first_posted,
    starts.date_id AS start_date,
    co.onboarding_confirmation_date AS confirmation_date,
    round2_start.min_step_start AS round2_date,
    rpm.profile_medium_desc,
    max(CASE
      WHEN (upper(jobpos.position_code_desc) LIKE '%PRN I'
       OR upper(jobpos.position_code_desc) LIKE '%PRN')
       AND upper(jcl.job_class_code) = '103' THEN 'PRN Tier 1'
      WHEN upper(jobpos.position_code_desc) LIKE '%PRN II'
       AND upper(jcl.job_class_code) = '103' THEN 'PRN Tier 2'
      WHEN upper(jobpos.position_code_desc) LIKE '%PRN III'
       AND upper(jcl.job_class_code) = '103' THEN 'PRN Tier 3'
      ELSE CAST(NULL as STRING)
    END) AS prn_tier,
    max(CASE
      WHEN stas.status_code IN(
        '01', '02'
      )
       OR (upper(jobpos.position_code_desc) LIKE '%PRN II'
       OR upper(jobpos.position_code_desc) LIKE '%PRN III') THEN 'Core'
      ELSE 'Flex'
    END) AS workforce_category,
    coalesce(gen1.element_detail_value_text, gen2.element_detail_value_text, gen3.element_detail_value_text, gen4.element_detail_value_text, gen5.element_detail_value_text) AS gender_name,
    max(coalesce(eth1.ethnicity, eth2.ethnicity, eth3.ethnicity, eth4.ethnicity, eth5.ethnicity, eth6.ethnicity)) AS ethnicity_name,
    max(coalesce(vet1.veteran_desc, vet2.veteran_desc, vet3.veteran_desc, vet4.veteran_desc, vet5.veteran_desc, vet6.veteran_desc, vet7.veteran_desc, vet8.veteran_desc, vet9.veteran_desc)) AS veteran_desc,
    max(coalesce(dis1.disability, dis2.disability, dis3.disability, dis4.disability, dis5.disability)) AS disability_ind,
    max(CASE
      WHEN c.internal_candidate_sw = 0 THEN 'External'
      WHEN c.internal_candidate_sw = 1 THEN 'Internal'
      ELSE 'Unknown'
    END) AS candidate_ie_ind,
    rr.rn_licenseb AS nursing_license,
    max(CASE
      WHEN upper(rr.source_system_code) = 'B'
       AND rr.matched_from_requisition_num = 1 THEN 'Y'
      WHEN upper(rr.source_system_code) = 'B'
       AND rst.recruitment_source_type_desc IN(
        'ATCH CAND', 'INT INV TO APPLY', 'EXT INV TO APPLY'
      ) THEN 'Y'
      WHEN upper(rr.source_system_code) = 'T'
       AND upper(rpm.profile_medium_desc) = 'MATCHED TO JOB' THEN 'Y'
      ELSE 'N'
    END) AS matched_candidate,
    hrmet.source_system_code,
    metric_numerator_qty AS fills,
    holds.total_duration AS total_hold_days,
    cast(date_diff(hrmet.date_id, hrmet.requisition_approval_date, DAY) AS NUMERIC) - holds.total_duration AS ttf_less_hold, --Time to Fill
    cast(date_diff(starts.date_id, hrmet.requisition_approval_date, DAY) AS NUMERIC) - holds.total_duration AS tts_less_hold, -- time to start
    hrmet.dw_last_update_date_time
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS hrmet
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS r ON hrmet.requisition_sid = r.requisition_sid
     AND date(r.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_department AS rd ON r.requisition_sid = rd.requisition_sid
     AND date(rd.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS d ON rd.dept_sid = d.dept_sid
     AND date(d.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_dept_rollup AS hrdr ON hrmet.process_level_code = hrdr.process_level_code
     AND hrmet.coid = hrdr.coid
     AND hrmet.dept_sid = hrdr.dept_sid
     AND hrmet.lawson_company_num = hrdr.lawson_company_num
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
          cp_0.completion_date,
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
          nursing_license.rn_licenseb,
          s.matched_from_requisition_num
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
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp_0 ON s.candidate_profile_sid = cp_0.candidate_profile_sid
           AND date(cp_0.valid_to_date) = '9999-12-31'
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
              WHERE upper(od.element_detail_type_text) IN(
                'NEW - GRAD NURSE RECRUITMENT TRACKING', 'GRADUATENURSERECRUITMENTTRACKING', 'RN_TRAINING_PROGRAM'
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
              WHERE (upper(od.element_detail_type_text) IN(
                'NEW - GRAD NURSE RECRUITMENT TRACKING', 'GRADUATENURSERECRUITMENTTRACKING'
              )
               AND upper(od.source_system_code) = 'T'
               OR upper(od.element_detail_type_text) IN(
                'NEW - GRADUATE NURSE RECRUITMENT TRACKING'
              )
               AND upper(od.element_detail_value_text) = 'Y'
               AND upper(od.source_system_code) = 'B')
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
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_status AS rrs ON rr.recruitment_requisition_sid = rrs.recruitment_requisition_sid
    LEFT OUTER JOIN (
      SELECT
          recruitment_requisition_history.creation_date_time,
          recruitment_requisition_history.source_system_code,
          recruitment_requisition_history.recruitment_requisition_sid,
          rank() OVER (PARTITION BY recruitment_requisition_history.recruitment_requisition_sid ORDER BY recruitment_requisition_history.creation_date_time) AS firstsourcing
        FROM
          {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history
        WHERE (recruitment_requisition_history.requisition_status_id) = 13
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
          {{ params.param_hr_views_dataset_name }}.sourcing_request AS req
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
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobpos ON hrmet.position_sid = jobpos.position_sid
     AND date(jobpos.valid_to_date) = '9999-12-31'
     AND hrmet.date_id BETWEEN jobpos.eff_from_date AND jobpos.eff_to_date
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_care_position AS pct ON upper(trim(jobpos.position_code_desc)) = upper(trim(pct.job_title_text))
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department AS sfd ON hrmet.sub_functional_dept_num = sfd.sub_functional_dept_num
     AND hrmet.functional_dept_num = sfd.functional_dept_num
     AND hrmet.coid = sfd.coid
     AND substr(d.dept_code, 0, 4) = sfd.dept_num
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON hrmet.coid = ff.coid
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stas ON r.application_status_sid = stas.status_sid
     AND date(stas.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN -- -Join to add Candidate Specific Data
    {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp ON rr.candidate_profile_sid = cp.candidate_profile_sid
     AND date(cp.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS c ON cp.candidate_sid = c.candidate_sid
     AND date(c.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_person AS person ON c.candidate_sid = person.candidate_sid
     AND date(person.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_profile_medium AS rpm ON cp.profile_medium_id = rpm.profile_medium_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_source AS rrs2 ON cp.recruitment_source_id = rrs2.recruitment_source_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_recruitment_source_type AS rst ON rrs2.recruitment_source_type_id = rst.recruitment_source_type_id
    LEFT OUTER JOIN -- - end candidate info
    {{ params.param_hr_base_views_dataset_name }}.candidate_pathway_microstep_snapshot AS cpm ON cp.candidate_profile_sid = cpm.candidate_profile_sid
    LEFT OUTER JOIN (
      SELECT
          ref_pathway.pathway_num,
          ref_pathway.subpathway_code,
          ref_pathway.pathway_name,
          ref_pathway.pathway_id
        FROM
          {{ params.param_hr_base_views_dataset_name }}.ref_pathway
        WHERE ref_pathway.subpathway_code IS NULL
    ) AS p ON cpm.pathway_id = p.pathway_id
    LEFT OUTER JOIN (
      SELECT DISTINCT
          microstep_pathway_sla.pathway_id,
          sum(microstep_pathway_sla.sla_day_cnt) AS path_sla
        FROM
          {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla
        GROUP BY 1
    ) AS sla ON cpm.pathway_id = sla.pathway_id
    LEFT OUTER JOIN -- ----Add CTEs
    (
      SELECT DISTINCT
          fin.recruitment_requisition_sid,
          min(fin.event_start) AS min_step_start,
          max(fin.event_end) AS max_step_end
        FROM
          (
            SELECT
                recruitment_requisition_sid,
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
        WHERE upper(fin.step_short_name) = 'HR REVIEW'
        GROUP BY 1
    ) AS hrr_measure ON rr.recruitment_requisition_sid = hrr_measure.recruitment_requisition_sid
    LEFT OUTER JOIN (
      SELECT DISTINCT
          fin.recruitment_requisition_sid,
          min(fin.event_start) AS min_step_start,
          max(fin.event_end) AS max_step_end
        FROM
          (
            SELECT
                recruitment_requisition_sid,
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
        WHERE upper(fin.step_short_name) <> 'HR REVIEW'
        GROUP BY 1
    ) AS sst_measure ON rr.recruitment_requisition_sid = sst_measure.recruitment_requisition_sid
    LEFT OUTER JOIN (
      SELECT DISTINCT
          hrm.requisition_sid,
          max(hrm.date_id) AS date_id,
          r_0.requisition_open_date,
          max(hrm.date_id) - r_0.requisition_open_date AS tts
        FROM
          {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS hrm
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS r_0 ON hrm.requisition_sid = r_0.requisition_sid
           AND date(r_0.valid_to_date) = '9999-12-31'
        WHERE (hrm.analytics_msr_sid) = 80200
         AND upper(hrm.action_code) IN(
          --  hires
          '1HIREAPPL', '1REHIRE', '1XFERPOS', '1XFEREIN-S', '1XFERINT'
        )
         AND hrm.date_id BETWEEN '2016-01-01' AND date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
        GROUP BY 1, 3
    ) AS starts ON r.requisition_sid = starts.requisition_sid
    LEFT OUTER JOIN -- -Add Approval Time
    (
      SELECT DISTINCT
          tad.requisition_sid,
          sum(tad.duration) AS total_approval_duration
        FROM
          (
            SELECT
                m.requisition_sid,
                m.approver_order,
                CASE
                  WHEN date_diff(CASE
                    WHEN DATE(m.end_timestamp) = DATE '1800-01-01' THEN CAST(NULL as DATE)
                    ELSE DATE(m.end_timestamp)
                  END, DATE(m.start_timestamp), DAY) <= 365 THEN CAST(timestamp_diff(m.end_timestamp, m.start_timestamp, HOUR) as NUMERIC) / 24
                  WHEN date_diff(DATE(m.end_timestamp), DATE(m.start_timestamp), DAY) BETWEEN 365 AND 9999 THEN CAST(timestamp_diff(m.end_timestamp, m.start_timestamp, DAY) as NUMERIC)
                  ELSE CAST(NULL as NUMERIC)
                END AS duration
              FROM
                --  less than a year
                --  between 365 and 9999(Max size of DAY(4) interval field)
                --  NULLs and range beyond -9999 and 9999
                (
                  SELECT
                      rw.requisition_sid,
                      rw.workflow_seq_num AS approver_order,
                      rw.start_date,
                      rw.start_time,
                      rw.start_date + (rw.start_time - TIME '00:00:00') AS start_timestamp,
                      rw.end_date,
                      rw.end_time,
                      rw.end_date + (rw.end_time - TIME '00:00:00') AS end_timestamp
                    FROM
                      {{ params.param_hr_base_views_dataset_name }}.requisition_workflow AS rw
                    WHERE (rw.end_date) < '9999-12-31'
                     AND date(rw.valid_to_date) = '9999-12-31'
                ) AS m
          ) AS tad
        GROUP BY 1
    ) AS tad2 ON hrmet.requisition_sid = tad2.requisition_sid
    LEFT OUTER JOIN -- -- Add confirmation date
    {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding AS co ON hrmet.requisition_sid = co.requisition_sid
     AND cp.candidate_sid = co.candidate_sid
     AND date(co.valid_to_date) = '9999-12-31'
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
    LEFT OUTER JOIN -- --Add EEO info (there are several prescreen questions for each topic,  Pulling the result from each then doing coalesce in the select in order to select the response from the most recently created question)
    (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_value_text,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'GENDER_2018'
         AND date(cand.valid_to_date) = '9999-12-31'
         AND cand.element_detail_value_text IS NOT NULL
        GROUP BY 1, 2, cand.element_detail_id, 3, cand.valid_from_date
    ) AS gen1 ON cp.candidate_sid = gen1.candidate_sid
     AND gen1.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_value_text,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'EEO2B GENDER'
         AND date(cand.valid_to_date) = '9999-12-31'
         AND cand.element_detail_value_text IS NOT NULL
        GROUP BY 1, 2, cand.element_detail_id, 3, cand.valid_from_date
    ) AS gen2 ON cp.candidate_sid = gen2.candidate_sid
     AND gen2.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_value_text,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'EEO2A GENDER'
         AND date(cand.valid_to_date) = '9999-12-31'
         AND cand.element_detail_value_text IS NOT NULL
        GROUP BY 1, 2, cand.element_detail_id, 3, cand.valid_from_date
    ) AS gen3 ON cp.candidate_sid = gen3.candidate_sid
     AND gen3.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_value_text,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'EEO1 GENDER'
         AND date(cand.valid_to_date) = '9999-12-31'
         AND cand.element_detail_value_text IS NOT NULL
        GROUP BY 1, 2, cand.element_detail_id, 3, cand.valid_from_date
    ) AS gen4 ON cp.candidate_sid = gen4.candidate_sid
     AND gen4.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_value_text,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'GENDER'
         AND date(cand.valid_to_date) = '9999-12-31'
         AND cand.element_detail_value_text IS NOT NULL
        GROUP BY 1, 2, cand.element_detail_id, 3, cand.valid_from_date
    ) AS gen5 ON cp.candidate_sid = gen5.candidate_sid
     AND gen5.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_id,
          cand.source_system_code,
          cand.element_detail_value_text AS ethnicity,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'RACE_ETHNIC_IDENTIFICATION_2020'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS eth1 ON cp.candidate_sid = eth1.candidate_sid
     AND eth1.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_id,
          cand.source_system_code,
          cand.element_detail_value_text AS ethnicity,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'RACE_ETHNIC_IDENTIFICATION_2018'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS eth2 ON cp.candidate_sid = eth2.candidate_sid
     AND eth2.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_id,
          cand.source_system_code,
          cand.element_detail_value_text AS ethnicity,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'RACE_ETHNIC_IDENTIFICATION'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS eth3 ON cp.candidate_sid = eth3.candidate_sid
     AND eth3.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_id,
          cand.source_system_code,
          CASE
            WHEN upper(cand.source_system_code) = 'T'
             AND (cand.element_detail_id) = 10441 THEN 'Native Hawaiian or Other Pacific Islander (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'T'
             AND (cand.element_detail_id) = 10345 THEN 'Black or African American (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'T'
             AND (cand.element_detail_id) = 10344 THEN 'Asian (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'T'
             AND (cand.element_detail_id) = 10347 THEN 'Native American or Alaska Native (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'T'
             AND (cand.element_detail_id) = 10348 THEN 'White (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'T'
             AND (cand.element_detail_id) = 10440 THEN 'Two or More Races (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'T'
             AND (cand.element_detail_id) = 15660 THEN 'I do not wish to disclose'
            WHEN upper(cand.source_system_code) = 'T'
             AND (cand.element_detail_id) = 10346 THEN 'Hispanic or Latino'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'P' THEN 'Native Hawaiian or Other Pacific Islander (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'B' THEN 'Black or African American (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'A' THEN 'Asian (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'I' THEN 'Native American or Alaska Native (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'W' THEN 'White (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'T' THEN 'Two or More Races (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'X' THEN 'I do not wish to disclose'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'WITHHOLD' THEN 'I do not wish to disclose'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'H' THEN 'Hispanic or Latino'
            ELSE element_detail_value_text
          END AS ethval,
          coalesce(CASE
            WHEN upper(cand.source_system_code) = 'T'
             AND (cand.element_detail_id) = 10441 THEN 'Native Hawaiian or Other Pacific Islander (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'T'
             AND (cand.element_detail_id) = 10345 THEN 'Black or African American (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'T'
             AND (cand.element_detail_id) = 10344 THEN 'Asian (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'T'
             AND (cand.element_detail_id) = 10347 THEN 'Native American or Alaska Native (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'T'
             AND (cand.element_detail_id) = 10348 THEN 'White (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'T'
             AND (cand.element_detail_id) = 10440 THEN 'Two or More Races (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'T'
             AND (cand.element_detail_id) = 15660 THEN 'I do not wish to disclose'
            WHEN upper(cand.source_system_code) = 'T'
             AND (cand.element_detail_id) = 10346 THEN 'Hispanic or Latino'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'P' THEN 'Native Hawaiian or Other Pacific Islander (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'B' THEN 'Black or African American (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'A' THEN 'Asian (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'I' THEN 'Native American or Alaska Native (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'W' THEN 'White (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'T' THEN 'Two or More Races (Not Hispanic or Latino)'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'X' THEN 'I do not wish to disclose'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'WITHHOLD' THEN 'I do not wish to disclose'
            WHEN upper(cand.source_system_code) = 'B'
             AND upper(cand.element_detail_value_text) = 'H' THEN 'Hispanic or Latino'
            ELSE element_detail_value_text
          END, element_detail_value_text) AS ethnicity,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_element_detail AS red ON cand.element_detail_id = red.element_detail_id
           AND (red.element_definition_selection_id) = 10282
        WHERE upper(cand.element_detail_type_text) = 'ETHNICITY'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS eth4 ON cp.candidate_sid = eth4.candidate_sid
     AND eth4.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_id,
          cand.source_system_code,
          cand.element_detail_value_text AS ethnicity,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'EEO2A ETHNICITY'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS eth5 ON cp.candidate_sid = eth5.candidate_sid
     AND eth5.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_id,
          cand.source_system_code,
          cand.element_detail_value_text AS ethnicity,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'EEO2B ETHNICITY'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS eth6 ON cp.candidate_sid = eth6.candidate_sid
     AND eth6.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_id,
          cand.element_detail_value_text,
          cand.valid_from_date,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent,
          CASE
            WHEN upper(cand.element_detail_value_text) LIKE 'NO%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'N' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'Y' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM NOT%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM A PROTECT%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'SPECIAL DISABLED VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'CHOICES 1 AND 2' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = '(1) VIETNAM ERA VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'YES%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE '%OTHER%' THEN 'Yes'
            ELSE 'Not Disclosed'
          END AS veteran_desc
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'OTHER_PROTECTED_VETERAN_2018'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS vet1 ON cp.candidate_sid = vet1.candidate_sid
     AND vet1.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_id,
          cand.element_detail_value_text,
          cand.valid_from_date,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent,
          CASE
            WHEN upper(cand.element_detail_value_text) LIKE 'NO%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'N' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'Y' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM NOT%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM A PROTECT%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'SPECIAL DISABLED VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'CHOICES 1 AND 2' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = '(1) VIETNAM ERA VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'YES%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE '%OTHER%' THEN 'Yes'
            ELSE 'Not Disclosed'
          END AS veteran_desc
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'VETERAN STATUS'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS vet2 ON cp.candidate_sid = vet2.candidate_sid
     AND vet2.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_id,
          cand.element_detail_value_text,
          cand.valid_from_date,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent,
          CASE
            WHEN upper(cand.element_detail_value_text) LIKE 'NO%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'N' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'Y' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM NOT%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM A PROTECT%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'SPECIAL DISABLED VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'CHOICES 1 AND 2' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = '(1) VIETNAM ERA VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'YES%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE '%OTHER%' THEN 'Yes'
            ELSE 'Not Disclosed'
          END AS veteran_desc
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'OTHER_PROTECTED_VETERAN'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS vet3 ON cp.candidate_sid = vet3.candidate_sid
     AND vet3.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_id,
          cand.element_detail_value_text,
          cand.valid_from_date,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent,
          CASE
            WHEN upper(cand.element_detail_value_text) LIKE 'NO%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'N' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'Y' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM NOT%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM A PROTECT%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'SPECIAL DISABLED VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'CHOICES 1 AND 2' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = '(1) VIETNAM ERA VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'YES%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE '%OTHER%' THEN 'Yes'
            ELSE 'Not Disclosed'
          END AS veteran_desc
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'EEO2B VETERAN'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS vet4 ON cp.candidate_sid = vet4.candidate_sid
     AND vet4.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_id,
          cand.element_detail_value_text,
          cand.valid_from_date,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent,
          CASE
            WHEN upper(cand.element_detail_value_text) LIKE 'NO%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'N' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'Y' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM NOT%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM A PROTECT%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'SPECIAL DISABLED VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'CHOICES 1 AND 2' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = '(1) VIETNAM ERA VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'YES%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE '%OTHER%' THEN 'Yes'
            ELSE 'Not Disclosed'
          END AS veteran_desc
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'EEO2A VETERAN'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS vet5 ON cp.candidate_sid = vet5.candidate_sid
     AND vet5.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_id,
          cand.element_detail_value_text,
          cand.valid_from_date,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent,
          CASE
            WHEN upper(cand.element_detail_value_text) LIKE 'NO%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'N' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'Y' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM NOT%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM A PROTECT%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'SPECIAL DISABLED VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'CHOICES 1 AND 2' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = '(1) VIETNAM ERA VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'YES%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE '%OTHER%' THEN 'Yes'
            ELSE 'Not Disclosed'
          END AS veteran_desc
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'EEO1 VETERAN'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS vet6 ON cp.candidate_sid = vet6.candidate_sid
     AND vet6.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_id,
          cand.element_detail_value_text,
          cand.valid_from_date,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent,
          CASE
            WHEN upper(cand.element_detail_value_text) LIKE 'NO%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'N' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'Y' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM NOT%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM A PROTECT%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'SPECIAL DISABLED VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'CHOICES 1 AND 2' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = '(1) VIETNAM ERA VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'YES%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE '%OTHER%' THEN 'Yes'
            ELSE 'Not Disclosed'
          END AS veteran_desc
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'EEO2A SPECIAL DISABLED VETERAN'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS vet7 ON cp.candidate_sid = vet7.candidate_sid
     AND vet7.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_id,
          cand.element_detail_value_text,
          cand.valid_from_date,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent,
          CASE
            WHEN upper(cand.element_detail_value_text) LIKE 'NO%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'N' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'Y' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM NOT%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM A PROTECT%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'SPECIAL DISABLED VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'CHOICES 1 AND 2' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = '(1) VIETNAM ERA VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'YES%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE '%OTHER%' THEN 'Yes'
            ELSE 'Not Disclosed'
          END AS veteran_desc
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'EEO2B SPECIAL DISABLED VETERAN'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS vet8 ON cp.candidate_sid = vet8.candidate_sid
     AND vet8.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.element_detail_type_text,
          cand.element_detail_id,
          cand.element_detail_value_text,
          cand.valid_from_date,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent,
          CASE
            WHEN upper(cand.element_detail_value_text) LIKE 'NO%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'N' THEN 'No'
            WHEN upper(cand.element_detail_value_text) = 'Y' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM NOT%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) LIKE 'I AM A PROTECT%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'SPECIAL DISABLED VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = 'CHOICES 1 AND 2' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) = '(1) VIETNAM ERA VETERAN' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE 'YES%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE '%OTHER%' THEN 'Yes'
            ELSE 'Not Disclosed'
          END AS veteran_desc
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'EEO1 SPECIAL DISABLED VETERAN'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS vet9 ON cp.candidate_sid = vet9.candidate_sid
     AND vet9.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.valid_from_date,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent,
          cand.element_detail_type_text,
          cand.element_detail_value_text,
          CASE
            WHEN upper(cand.element_detail_value_text) LIKE 'NO%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) LIKE 'YES%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE '%WISH%' THEN 'Not Disclosed'
          END AS disability
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'DISABILITY'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS dis2 ON c.candidate_sid = dis2.candidate_sid
     AND dis2.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.valid_from_date,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent,
          cand.element_detail_type_text,
          cand.element_detail_value_text,
          CASE
            WHEN upper(cand.element_detail_value_text) LIKE 'NO%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) LIKE 'YES%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE '%WISH%' THEN 'Not Disclosed'
          END AS disability
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'DISABILITY_2018'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS dis1 ON c.candidate_sid = dis1.candidate_sid
     AND dis1.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.valid_from_date,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent,
          cand.element_detail_type_text,
          cand.element_detail_value_text,
          CASE
            WHEN upper(cand.element_detail_value_text) LIKE 'NO%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) LIKE 'YES%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE '%WISH%' THEN 'Not Disclosed'
          END AS disability
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'EEO1 DISABILITY'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS dis3 ON c.candidate_sid = dis3.candidate_sid
     AND dis3.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.valid_from_date,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent,
          cand.element_detail_type_text,
          cand.element_detail_value_text,
          CASE
            WHEN upper(cand.element_detail_value_text) LIKE 'NO%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) LIKE 'YES%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE '%WISH%' THEN 'Not Disclosed'
          END AS disability
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'EEO2A DISABILITY'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS dis4 ON c.candidate_sid = dis4.candidate_sid
     AND dis4.recent = 1
    LEFT OUTER JOIN (
      SELECT
          cand.candidate_sid,
          cand.valid_from_date,
          rank() OVER (PARTITION BY cand.candidate_sid ORDER BY cand.valid_from_date DESC) AS recent,
          cand.element_detail_type_text,
          cand.element_detail_value_text,
          CASE
            WHEN upper(cand.element_detail_value_text) LIKE 'NO%' THEN 'No'
            WHEN upper(cand.element_detail_value_text) LIKE 'YES%' THEN 'Yes'
            WHEN upper(cand.element_detail_value_text) LIKE '%WISH%' THEN 'Not Disclosed'
          END AS disability
        FROM
          {{ params.param_hr_base_views_dataset_name }}.candidate_detail AS cand
        WHERE upper(cand.element_detail_type_text) = 'EEO2B DISABILITY'
         AND date(cand.valid_to_date) = '9999-12-31'
    ) AS dis5 ON c.candidate_sid = dis5.candidate_sid
     AND dis5.recent = 1
---- END EEO info

    LEFT JOIN (
      SELECT
        th.recruitment_requisition_sid,
        SUM(duration) AS total_duration
      FROM (
        SELECT
          reqstatus.recruitment_requisition_sid,
          reqstatus.req_status_start,
          reqstatus.req_status_end,
          reqstatus.requisition_status_id,
          CASE
            WHEN (datetime_diff(reqstatus.req_status_end,reqstatus.req_status_start, DAY)) <= 365
              THEN CAST((datetime_diff(reqstatus.req_status_end,reqstatus.req_status_start, HOUR)) AS NUMERIC) / 24 --less than a year
            WHEN (datetime_diff(reqstatus.req_status_end,reqstatus.req_status_start, DAY)) BETWEEN 365 AND 9999
              THEN CAST((datetime_diff(reqstatus.req_status_end,reqstatus.req_status_start, DAY)) AS NUMERIC) -- Between 365 and 9999(Max size of DAY(4) interval field)
            ELSE NULL
          END AS duration,
          reqstatus.source_system_code
        FROM reqstatus
        WHERE requisition_status_id IN (7, 1008)
      ) th
      GROUP BY 1
    ) holds
    ON rr.recruitment_requisition_sid = holds.recruitment_requisition_sid

  WHERE (hrmet.analytics_msr_sid) = 80500
   AND hrmet.date_id > date_add(current_date('US/Central'), interval -37 MONTH)
  GROUP BY 1, 2, 3, upper(concat(coalesce(hrdr.coid, '00000'), hrdr.process_level_code, coalesce(hrdr.dept_code, '00000'), hrdr.lawson_company_num)), 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, upper(CASE
    WHEN upper(hrmet.work_schedule_code) LIKE '1%' THEN 'Days'
    WHEN upper(hrmet.work_schedule_code) LIKE '2%' THEN 'Eves'
    WHEN upper(hrmet.work_schedule_code) = 'SECONDPRN' THEN 'Eves'
    WHEN upper(hrmet.work_schedule_code) LIKE '3%' THEN 'Nights'
    WHEN upper(hrmet.work_schedule_code) LIKE 'X%' THEN 'Mixed'
    WHEN upper(hrmet.work_schedule_code) = 'VARY' THEN 'Mixed'
    WHEN upper(hrmet.work_schedule_code) = 'WFH' THEN 'WFH'
    ELSE 'Unknown'
  END), upper(CASE
    WHEN (rr.iec) = 1 THEN 'External'
    WHEN (rr.iec) = 2 THEN 'Ext Contract To Perm'
    WHEN (rr.iec) = 3 THEN 'Internal HCA'
    WHEN (rr.iec) = 4 THEN 'Internal Same Division'
    WHEN (rr.iec) = 5 THEN 'Internal Same Facility'
    WHEN (rr.iec) = 6 THEN 'Internal Same ILOB'
    WHEN upper(rr.iecb) = 'INTERNALHCA' THEN 'Internal HCA'
    WHEN upper(rr.iecb) = 'EXTERNAL' THEN 'External'
    WHEN upper(rr.iecb) = 'INTERNALSAMEINTEGRATEDLINEOFBUSINESS' THEN 'Internal Same ILOB'
    WHEN upper(rr.iecb) = 'INTERNALSAMEFACILITY' THEN 'Internal Same Facility'
    WHEN upper(rr.iecb) = 'INTERNALSAMEDIVISION' THEN 'Internal Same Division'
    WHEN upper(rr.iecb) = 'EXTERNALCONTRACTTOPERM' THEN 'Ext Contract To Perm'
    ELSE 'Unknown'
  END), 23, upper(CASE
    WHEN (rr.new_grad) = 12320 THEN 'Local Program'
    WHEN (rr.new_grad) = 12321 THEN 'Local Program'
    WHEN upper(rr.new_gradb) = 'LOCALRNTRAININGPROGRAM' THEN 'Local Program'
    WHEN (rr.new_grad) = 12340 THEN 'No Formal Program'
    WHEN upper(rr.new_gradb) = 'NOFORMALRNTRAININGPROGRAM' THEN 'No Formal Program'
    WHEN (rr.new_grad) = 12300 THEN 'Not Applicable'
    WHEN upper(rr.new_gradb) = 'HWSRNTRAININGPROGRAM' THEN 'HWS/StaRN'
    WHEN (rr.new_grad) = 12341 THEN 'HWS/StaRN'
    ELSE 'NULL'
  END), upper(coalesce(rr.grad_flagb, CASE
    WHEN (rr.new_grad) = 12320 THEN 'Y'
    WHEN (rr.new_grad) = 12340 THEN 'Y'
    WHEN (rr.new_grad) = 12321 THEN 'Y'
    WHEN (rr.new_grad) = 12341 THEN 'Y'
    ELSE 'N'
  END)), upper(CASE
    WHEN (rr.grad_program) = 12301 THEN 'Neonatal ICU'
    WHEN (rr.grad_program) = 12302 THEN 'Obstetrics'
    WHEN (rr.grad_program) = 12303 THEN 'Telemetry'
    WHEN (rr.grad_program) = 12322 THEN 'Behavioral Health'
    WHEN (rr.grad_program) = 12323 THEN 'Emergency Room'
    WHEN (rr.grad_program) = 12324 THEN 'Intensive Care'
    WHEN (rr.grad_program) = 12342 THEN 'Not Applicable'
    WHEN (rr.grad_program) = 12343 THEN 'Labor & Delivery'
    WHEN (rr.grad_program) = 12344 THEN 'Med/Surg'
    WHEN (rr.grad_program) = 12345 THEN 'Operating Room'
    WHEN upper(rr.grad_programb) = 'OBSTETRICS' THEN 'Obstetrics'
    WHEN upper(rr.grad_programb) = 'EMERGENCYROOM' THEN 'Emergency Room'
    WHEN upper(rr.grad_programb) = 'LABORANDDELIVERY' THEN 'Labor & Delivery'
    WHEN upper(rr.grad_programb) = 'MEDSURG' THEN 'Med/Surg'
    WHEN upper(rr.grad_programb) = 'NEONATALINTENSIVECAREUNIT' THEN 'Neonatal ICU'
    WHEN upper(rr.grad_programb) = 'OPERATINGROOM' THEN 'Operating Room'
    WHEN upper(rr.grad_programb) = 'INTENSIVECAREUNIT' THEN 'Intensive Care'
    WHEN upper(rr.grad_programb) = 'BEHAVIORALHEALTH' THEN 'Behavioral Health'
    ELSE 'NULL'
  END), 27, 28, 29, 30, upper(concat(recuser.last_name, ', ', recuser.first_name)), 32, upper(concat(recuser2.last_name, ', ', recuser2.first_name)), 34, upper(concat(person.last_name, ', ', person.first_name)), 36, 37, 38, 39, 40, upper(CASE
    WHEN jobpos.position_code_desc = pct.job_title_text THEN 'PCT'
    ELSE 'No'
  END), upper(CASE
    WHEN stas.status_code = '01' THEN 'FT'
    WHEN stas.status_code = '02' THEN 'PT'
    WHEN stas.status_code = '03' THEN 'PRN'
    WHEN stas.status_code = '04' THEN 'TEMP'
    ELSE CAST(NULL as STRING)
  END), 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, upper(CASE
    WHEN p.pathway_num = 1 THEN '15'
    WHEN p.pathway_num = 3 THEN '19'
    WHEN p.pathway_num = 9 THEN '11'
    ELSE CAST(NULL as STRING)
  END), 60, 61, 62, 63, 64, upper(CASE
    WHEN (upper(jobpos.position_code_desc) LIKE '%PRN I'
     OR upper(jobpos.position_code_desc) LIKE '%PRN')
     AND upper(jcl.job_class_code) = '103' THEN 'PRN Tier 1'
    WHEN upper(jobpos.position_code_desc) LIKE '%PRN II'
     AND upper(jcl.job_class_code) = '103' THEN 'PRN Tier 2'
    WHEN upper(jobpos.position_code_desc) LIKE '%PRN III'
     AND upper(jcl.job_class_code) = '103' THEN 'PRN Tier 3'
    ELSE CAST(NULL as STRING)
  END), upper(CASE
    WHEN stas.status_code IN(
      '01', '02'
    )
     OR (upper(jobpos.position_code_desc) LIKE '%PRN II'
     OR upper(jobpos.position_code_desc) LIKE '%PRN III') THEN 'Core'
    ELSE 'Flex'
  END), 67, upper(coalesce(eth1.ethnicity, eth2.ethnicity, eth3.ethnicity, eth4.ethnicity, eth5.ethnicity, eth6.ethnicity)), upper(coalesce(vet1.veteran_desc, vet2.veteran_desc, vet3.veteran_desc, vet4.veteran_desc, vet5.veteran_desc, vet6.veteran_desc, vet7.veteran_desc, vet8.veteran_desc, vet9.veteran_desc)), upper(coalesce(dis1.disability, dis2.disability, dis3.disability, dis4.disability, dis5.disability)), upper(CASE
    WHEN c.internal_candidate_sw = 0 THEN 'External'
    WHEN c.internal_candidate_sw = 1 THEN 'Internal'
    ELSE 'Unknown'
  END), 72, upper(CASE
    WHEN upper(rr.source_system_code) = 'B'
     AND rr.matched_from_requisition_num = 1 THEN 'Y'
    WHEN upper(rr.source_system_code) = 'B'
     AND upper(rst.recruitment_source_type_desc) IN(
      'ATCH CAND', 'INT INV TO APPLY', 'EXT INV TO APPLY'
    ) THEN 'Y'
    WHEN upper(rr.source_system_code) = 'T'
     AND upper(rpm.profile_medium_desc) = 'MATCHED TO JOB' THEN 'Y'
    ELSE 'N'
  END), 74, 75, 76, 77, 78, 79;