CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.bi_dept_fills_snapshot AS SELECT DISTINCT
    last_day(DATE(hrmet.date_id)) AS pe_date,
    hrmet.date_id AS date_id,
    hrmet.requisition_approval_date,
    r.requisition_num,
    rr.recruitment_requisition_num_text,
    ff.sector_name,
    ff.group_name,
    ff.division_name,
    ff.market_name,
    ff.lob_code,
    ff.sub_lob_code,
    ff.cons_facility_name,
    hrmet.coid,
    ff.coid_name,
    hrmet.process_level_code,
    hrmet.location_code,
    gldc.dept_num,
    rd.dept_code,
    d.dept_name,
    fd.functional_dept_desc,
    fd.sub_functional_dept_desc,
    ilob.category_desc,
    ilob.sub_category_desc,
    jc.job_code_desc,
    jc.job_code,
    jobpos.position_code_desc,
    kt.key_talent_group_text,
    mat1.category_desc as ilob_category,
    mat1.sub_category_desc as ilob_sub_category,
    hrops.business_unit_name,
    hrops.business_unit_segment_name,
    jcl.job_class_desc,
    jcl.job_class_code,
    CASE
      WHEN upper(jcl.job_class_code) = '103' THEN 'RN'
      WHEN jc.job_code IN(
        nd.job_code
      ) THEN 'Leadership'
      WHEN jobpos.position_code_desc = pct.job_title_text THEN 'PCT'
      ELSE 'Non-RN'
    END AS rn_grouping_text,
    CASE
      WHEN trim(stas.status_code) = '01' THEN 'FT'
      WHEN trim(stas.status_code) = '02' THEN 'PT'
      WHEN trim(stas.status_code) = '03' THEN 'PRN'
      WHEN trim(stas.status_code) = '04' THEN 'TEMP'
      ELSE CAST(NULL as STRING)
    END AS emp_status_code,
    CAST(r.open_fte_percent AS NUMERIC) AS fte_percent, 
    rjs.job_schedule_desc,
    CASE
      WHEN upper(hrmet.work_schedule_code) LIKE '1%' THEN 'Days'
      WHEN upper(hrmet.work_schedule_code) LIKE '2%' THEN 'Eves'
      WHEN upper(hrmet.work_schedule_code) = 'SECONDPRN' THEN 'Eves'
      WHEN upper(hrmet.work_schedule_code) LIKE '3%' THEN 'Nights'
      WHEN upper(hrmet.work_schedule_code) LIKE 'X%' THEN 'Mixed'
      WHEN upper(hrmet.work_schedule_code) = 'VARY' THEN 'Mixed'
      WHEN upper(hrmet.work_schedule_code) = 'WFH' THEN 'WFH'
      ELSE 'Unknown'
    END AS shift_desc,
    rj.job_title_name,
    CASE 
      WHEN  rr.iec = 1 THEN 'External'
      WHEN rr.iec = 2 THEN 'Ext Contract To Perm'
      WHEN rr.iec = 3 THEN 'Internal HCA'
      WHEN rr.iec = 4 THEN 'Internal Same Division'
      WHEN rr.iec = 5 THEN 'Internal Same Facility'
      WHEN rr.iec = 6 THEN 'Internal Same ILOB'
      WHEN rr.iecb = 'InternalHCA' THEN 'Internal HCA'
      WHEN rr.iecb = 'External' THEN 'External'
      WHEN rr.iecb = 'InternalSameIntegratedLineOfBusiness' THEN  'Internal Same ILOB'
      WHEN rr.iecb = 'InternalSameFacility' THEN  'Internal Same Facility'
      WHEN rr.iecb = 'InternalSameDivision' THEN 'Internal Same Division'
      WHEN rr.iecb = 'ExternalContractToPerm' THEN 'Ext Contract To Perm'
      ELSE 'Unknown' 
    END AS offer_ie_ind,
    COALESCE(CAST(rr.new_grad AS STRING), rr.new_gradb) AS new_grad_desc,
    COALESCE(rr.new_gradb, CASE WHEN  rr.new_grad = 12320 THEN 'Division/Market Program'
    WHEN  rr.new_grad = 12340  THEN 'No Formal Program'
    WHEN  rr.new_gradb = 'NoFormalRNTrainingProgram'  THEN 'No Formal Program'
    WHEN  rr.new_grad = 12321  THEN 'Facility Program'
    WHEN rr.new_grad = 12300  THEN 'Not Applicable'
    WHEN rr.new_grad = 12341  THEN 'StaRN'
    ELSE 'NULL' END) AS taleo_new_grad_desc,
    CASE 
      WHEN rr.grad_program = 12301 THEN 'NeonatalIntensiveCareUnit'
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
    END AS ng_program_type_desc,
    COALESCE(rr.job_experience_dateb, rr.job_experience_date) AS job_experience_date,
    COALESCE(rr.rn_acute_exp_dateb, rr.rn_acute_exp_date) AS rn_acute_exp_date,
    concat(recuser.last_name, ', ', recuser.first_name) AS recruiter_name,
    concat(recuser2.last_name, ', ', recuser2.first_name) AS hiring_manager_name,
    rs.status_code,
    hrmet.date_id - hrmet.requisition_approval_date AS time_to_fill,
    nd.director_grouping_desc,
    metric_numerator_qty AS fills_total_num
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot AS hrmet
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS r ON hrmet.requisition_sid = r.requisition_sid
     AND date(r.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_department AS rd ON r.requisition_sid = rd.requisition_sid
     AND date(rd.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS d ON rd.dept_sid = d.dept_sid
     AND date(d.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.hr_dept_rollup AS hrdr 
     ON hrmet.process_level_code = hrdr.process_level_code
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
          new_grad,
          new_gradb,
          grad_program,
          grad_programb,
          job_experience_date,
          job_experience_dateb,
          rn_acute_exp_date,
          rn_acute_exp_dateb,
          onboarding_trigger,
          onboarding_triggerb,
          grad_flag,
          grad_flagb,
          nursing_license.rn_licenseb
        FROM {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS recr
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s 
           ON recr.recruitment_requisition_sid = s.recruitment_requisition_sid
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
              RANK() over(PARTITION BY ao.submission_sid ORDER BY ao.sequence_num DESC) AS active 
            FROM {{ params.param_hr_base_views_dataset_name }}.offer ao 
            WHERE date(ao.valid_to_date) = '9999-12-31'
          ) o 
           ON s.submission_sid = o.submission_sid 
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status AS os 
           ON os.offer_sid = o.offer_sid
           AND date(os.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_offer_status AS ros 
           ON ros.offer_status_id = os.offer_status_id
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS cp
           ON s.candidate_profile_sid = cp.candidate_profile_sid
           AND date(cp.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN (
            SELECT 
              od.element_detail_id as iec, 
              od.element_detail_value_text as iecb, 
              od.offer_sid 
            FROM {{ params.param_hr_base_views_dataset_name }}.offer_detail od 
            WHERE UPPER(od.element_detail_type_text) = 'INTERNAL_EXTERNAL_CANDIDATE'
		          AND date(od.valid_to_date) = '9999-12-31' 
            GROUP BY od.element_detail_id, od.offer_sid,od.element_detail_value_text
          ) AS iec
           ON o.offer_sid = iec.offer_sid
          LEFT OUTER JOIN (
            SELECT
              od.element_detail_id as onboarding_trigger, 
              od.element_detail_value_text as onboarding_triggerb, 
              od.offer_sid 
            FROM {{ params.param_hr_base_views_dataset_name }}.offer_detail od
		        WHERE UPPER(od.element_detail_type_text) = UPPER('Onboarding Required for Hired Candidate OFFER') 
              AND date(od.valid_to_date) = '9999-12-31' 
            GROUP BY od.element_detail_id, od.element_detail_value_text, od.offer_sid
          ) AS onbdtrg
		       ON o.offer_sid = onbdtrg.offer_sid
          LEFT OUTER JOIN(
            SELECT 
              od.offer_sid, 
              od.element_detail_value_text as job_experience_date, 
              od.element_detail_value_text as job_experience_dateb
		        FROM {{ params.param_hr_base_views_dataset_name }}.offer_detail od 
            WHERE od.element_detail_type_text = 'Job_Experience_Date' 
              AND date(od.valid_to_date) = '9999-12-31' 
            GROUP BY od.element_detail_value_text, od.element_detail_value_text, od.offer_sid
          ) AS jed
		       ON o.offer_sid= jed.offer_sid
          LEFT OUTER JOIN(
            SELECT 
              od.offer_sid, 
              od.element_detail_value_text as rn_acute_exp_date, 
              od.element_detail_value_text as rn_acute_exp_dateb 
            FROM {{ params.param_hr_base_views_dataset_name }}.offer_detail od
		        WHERE UPPER(od.element_detail_type_text) = UPPER('RN_Acute_Exp_Date') 
              AND date(od.valid_to_date) = '9999-12-31' 
            GROUP BY od.element_detail_value_text, od.element_detail_value_text, od.offer_sid
          ) AS rnex
		       ON o.offer_sid= rnex.offer_sid
          LEFT OUTER JOIN(
            SELECT 
              od.element_detail_id as new_grad,
              od.offer_sid,
              od.element_detail_value_text as new_gradb, 
              rank() over(partition by od.offer_sid order by od.element_detail_id desc) as ngrank
            FROM {{ params.param_hr_base_views_dataset_name }}.offer_detail od
            WHERE UPPER(od.element_detail_type_text) IN (UPPER('New - Grad Nurse Recruitment Tracking'), UPPER('GraduateNurseRecruitmentTracking'), UPPER('RN_Training_Program')) 
              AND date(od.valid_to_date) = '9999-12-31' 
            GROUP BY  od.offer_sid,  od.element_detail_value_text, od.element_detail_id, valid_from_date, od.element_detail_id
          ) AS ng
           ON o.offer_sid= ng.offer_sid
           AND ng.ngrank = 1
          LEFT OUTER JOIN (
            SELECT 
              od.element_detail_type_text, 
              od.element_detail_id as rn_license, 
              od.offer_sid, 
              od.element_detail_value_text as rn_licenseb 
            FROM {{ params.param_hr_base_views_dataset_name }}.offer_detail od
            WHERE UPPER(od.element_detail_type_text) = UPPER('RN_License_Type')
              AND date(od.valid_to_date) = '9999-12-31' 
            GROUP BY od.element_detail_id, od.element_detail_value_text, od.offer_sid, od.element_detail_type_text
          ) AS nursing_license
           ON o.offer_sid = nursing_license.offer_sid
          LEFT OUTER JOIN (
            SELECT 
              od.source_system_code, 
              od.element_detail_type_text, 
              od.element_detail_id as grad_flag, 
              od.offer_sid, 
              od.element_detail_value_text as grad_flagb, 
              rank() over(partition by od.offer_sid order by od.element_detail_id desc) as ngbrank
            FROM {{ params.param_hr_base_views_dataset_name }}.offer_detail od 
            WHERE UPPER(od.element_detail_type_text) IN ('NEW - GRAD NURSE RECRUITMENT TRACKING', 'NEW - GRAD NURSE RECRUITMENT TRACKING', 'GRADUATENURSERECRUITMENTTRACKING') 
              AND date(od.valid_to_date) = '9999-12-31'  
            GROUP BY od.source_system_code, od.element_detail_id, od.element_detail_value_text, od.offer_sid, od.element_detail_type_text
          ) AS ng_flag
           ON o.offer_sid= ng_flag.offer_sid
           AND ng_flag.ngbrank = 1
          LEFT OUTER JOIN (
            SELECT 
              od.element_detail_id as grad_program, 
              od.offer_sid, 
              od.element_detail_value_text as grad_programb 
            FROM {{ params.param_hr_base_views_dataset_name }}.offer_detail od
          WHERE UPPER(od.element_detail_type_text) IN ('NEWPROGRAMSPECIALTY') 
            AND date(od.valid_to_date) = '9999-12-31' 
          GROUP BY od.element_detail_id, od.element_detail_value_text, od.offer_sid
        ) AS gp
         ON o.offer_sid= gp.offer_sid
        WHERE date(recr.valid_to_date) = '9999-12-31'
          AND os.offer_status_id IN (10,1010) 
          AND date(os.valid_to_date) = '9999-12-31'
        QUALIFY row_number() OVER (PARTITION BY recr.lawson_requisition_sid ORDER BY o.capture_date DESC, recr.approved_sw DESC, recr.recruitment_requisition_sid DESC, o.sequence_num DESC) = 1
    ) AS rr ON r.requisition_sid = rr.lawson_requisition_sid
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_status AS rrs ON rr.recruitment_requisition_sid = rrs.recruitment_requisition_sid
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj ON rr.recruitment_job_sid = rj.recruitment_job_sid
     AND date(rj.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_job_schedule AS rjs ON rj.job_schedule_id = rjs.job_schedule_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS recuser ON rj.recruiter_user_sid = recuser.recruitment_user_sid
     AND date(recuser.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS recuser2 ON rr.hiring_manager_user_sid = recuser2.recruitment_user_sid
     AND date(recuser2.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_status AS rs ON hrmet.requisition_sid = rs.requisition_sid
     AND date(rs.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jc ON hrmet.job_code_sid = jc.job_code_sid
     AND date(jc.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_position AS reqpos ON r.requisition_sid = reqpos.requisition_sid
     AND date(reqpos.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jobpos ON hrmet.position_sid = jobpos.position_sid
     AND date(jobpos.valid_to_date) = '9999-12-31'
     AND hrmet.date_id BETWEEN jobpos.eff_from_date AND jobpos.eff_to_date
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS gldc ON jobpos.account_unit_num = gldc.account_unit_num
     AND jobpos.gl_company_num = gldc.gl_company_num
     AND date(gldc.valid_to_date) = '9999-12-31'
     AND jobpos.process_level_code = gldc.process_level_code
     AND jobpos.lawson_company_num = gldc.lawson_company_num
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department AS fd ON gldc.dept_num = fd.dept_num
     AND gldc.coid = fd.coid
     AND gldc.company_code = fd.company_code
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON hrmet.coid = ff.coid
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stas ON r.application_status_sid = stas.status_sid
     AND date(stas.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_department AS func ON hrmet.functional_dept_num = func.functional_dept_num
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.sub_functional_department AS subf ON hrmet.sub_functional_dept_num = subf.sub_functional_dept_num
     AND hrmet.functional_dept_num = func.functional_dept_num
    LEFT OUTER JOIN (
      SELECT
          rrh.recruitment_requisition_sid,
          rrh.creation_date_time,
          rrh.closed_date_time,
          date_diff(rrh.closed_date_time, rrh.creation_date_time, DAY) AS hold_days
        FROM
          {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history AS rrh
        WHERE rrh.requisition_status_id IN(
          7, 1007
        )
        GROUP BY 1, 2, 3
    ) AS holds ON rr.recruitment_requisition_sid = holds.recruitment_requisition_sid
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS kt ON hrmet.key_talent_id = kt.key_talent_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_patient_care_position AS pct ON jobpos.position_code_desc = pct.job_title_text
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_director AS nd ON jc.job_code = nd.job_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jcl ON jc.job_class_sid = jcl.job_class_sid
     AND date(jcl.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS ilob ON hrmet.integrated_lob_id = ilob.integrated_lob_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_operations AS hrops ON hrmet.process_level_code = hrops.process_level_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat1
      ON hrmet.integrated_lob_id = mat1.integrated_lob_id
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON hrmet.process_level_code = c.process_level_code
     AND hrmet.lawson_company_num = c.lawson_company_num
     AND c.user_id = session_user()
    INNER JOIN -- -- Added snapshot date join to only pull the most recent snapshots
    (
      SELECT
          max(fact_hr_metric_snapshot.snapshot_date) AS maxsnap
        FROM
          {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot
    ) AS snapdt ON hrmet.snapshot_date = snapdt.maxsnap
  WHERE (hrmet.analytics_msr_sid) = 80500
   AND hrmet.date_id BETWEEN '2016-01-01' AND date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
;
-- --fills
-- --default value 2016-01-01;;