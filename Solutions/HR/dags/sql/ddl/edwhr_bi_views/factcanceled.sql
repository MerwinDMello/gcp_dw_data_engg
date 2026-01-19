CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.factcanceled AS SELECT
    --  =============================================
    --  Author:        Cheryl Costa
    --  Create date:    03/02/2022
    --  Description: Results of this query inlcude requisitions that are currently in cancelled status in Taleo/ATS.  Cancel_Date reflects the date of last status change.
    --  =============================================*/
    recreq.source_system_code,
    recreq.recruitment_requisition_num_text AS taleo_requisition_num,
    CASE
      WHEN upper(recreq.source_system_code) = 'B' THEN cast(recreq.requisition_num as string)
      ELSE recreq.recruitment_requisition_num_text
    END AS recruitment_requisition_num,
    recreq.recruitment_requisition_sid,
    recreq.lawson_requisition_sid,
    r.requisition_num AS lawson_requisition_num,
    rs.status_code AS lawson_status,
    recstatus.status_desc AS recruitment_status,
    hrdr.process_level_code,
    max(hrdr.pl_uid) AS pl_uid,
    concat(jp.position_sid, jp.eff_to_date) AS position_key,
    r.open_fte_percent AS fte_value,
    max(concat(recuser.last_name, ', ', recuser.first_name)) AS recruiter_name,
    max(concat(recuser2.last_name, ', ', recuser2.first_name)) AS hiring_manager_name,
    coalesce(rkeyt1.key_talent_id, rkeyt2.key_talent_id, rkeyt3.key_talent_id, rkeyt4.key_talent_id, rkeyt5.key_talent_id, rkeyt6.key_talent_id, rkeyt7.key_talent_id, rkeyt8.key_talent_id) AS key_talent_id,
    coalesce(mat1.integrated_lob_id, mat4.integrated_lob_id, mat3.integrated_lob_id, mat2.integrated_lob_id) AS integrated_lob_id,
    coalesce(rw.end_date, r.requisition_open_date) AS requisition_approval_date,
    coalesce(opentimeg.open_date, opentime.open_date) AS open_status_date,
    coalesce(postdate.creation_date_time, postdate2.posting_date) AS first_posted,
    postdate.creation_date_time AS taleo_first_posted,
    postdate2.posting_date AS ats_first_posted,
    coalesce(canceled_g.cancel_date, canceled.cancel_date) AS cancel_date,
    max(CASE
      WHEN opening.metric_numerator_qty IS NULL THEN 'No'
      ELSE 'Yes'
    END) AS was_opening,
    max(CASE
      WHEN filled.metric_numerator_qty IS NULL THEN 'No'
      ELSE 'Yes'
    END) AS was_fill
  FROM
    {{ params.param_hr_base_views_dataset_name }}.requisition AS r
    INNER JOIN (
      SELECT
          rr.recruitment_requisition_sid,
          rr.lawson_requisition_sid,
          o.accept_date,
          o.offer_sid,
          rr.process_level_code,
          ros.offer_status_desc,
          rr.approved_sw,
          s.submission_sid,
          rr.source_system_code,
          o.sequence_num,
          rr.requisition_num,
          rr.recruitment_requisition_num_text,
          rr.recruitment_job_sid,
          rr.hiring_manager_user_sid
        FROM
          {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS rr
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s ON rr.recruitment_requisition_sid = s.recruitment_requisition_sid
           AND date(s.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer AS o ON s.submission_sid = o.submission_sid
           AND date(o.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status AS os ON os.offer_sid = o.offer_sid
           AND date(os.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_offer_status AS ros ON ros.offer_status_id = os.offer_status_id
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_workflow AS rw_0 ON rr.workflow_id = rw_0.workflow_id
        WHERE date(rr.valid_to_date) = '9999-12-31'
         AND (rw_0.workflow_code IS NULL
         OR upper(rw_0.workflow_code) NOT LIKE '%ACQ%')
        QUALIFY row_number() OVER (PARTITION BY rr.lawson_requisition_sid, rr.source_system_code ORDER BY o.last_modified_date DESC, o.last_modified_time DESC, o.capture_date DESC, rr.approved_sw DESC, rr.recruitment_requisition_sid DESC, o.sequence_num DESC) = 1
    ) AS recreq ON r.requisition_sid = recreq.lawson_requisition_sid
    LEFT OUTER JOIN -- pulling the requisition associated with the latest offer, if there are multiple then pulling the active approved_sw and latest Recruitment_Requisition_SID
    (
      SELECT
          requisition_workflow.requisition_sid,
          max(requisition_workflow.end_date) AS end_date
        FROM
          {{ params.param_hr_base_views_dataset_name }}.requisition_workflow
        WHERE date(requisition_workflow.valid_to_date) = '9999-12-31'
         AND (select max(end_date)  FROM
          {{ params.param_hr_base_views_dataset_name }}.requisition_workflow) <> DATE'9999-12-31'
        GROUP BY 1
    ) AS rw ON r.requisition_sid = rw.requisition_sid
    LEFT OUTER JOIN -- pulling the latest version of a requisition
    (
      SELECT
          recruitment_requisition_history.creation_date_time,
          recruitment_requisition_history.source_system_code,
          recruitment_requisition_history.recruitment_requisition_sid,
          rank() OVER (PARTITION BY recruitment_requisition_history.recruitment_requisition_sid ORDER BY recruitment_requisition_history.creation_date_time) AS firstsourcing
        FROM
          {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history
        WHERE recruitment_requisition_history.requisition_status_id = 13
         AND upper(recruitment_requisition_history.source_system_code) = 'T'
    ) AS postdate ON recreq.recruitment_requisition_sid = postdate.recruitment_requisition_sid
     AND recreq.source_system_code = postdate.source_system_code
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
    ) AS postdate2 ON recreq.recruitment_requisition_sid = postdate2.recruitment_requisition_sid
     AND recreq.source_system_code = postdate2.source_system_code
     AND postdate2.firstposted = 1
    LEFT OUTER JOIN (
      SELECT
          recruitment_requisition_history.creation_date_time AS open_date,
          recruitment_requisition_history.recruitment_requisition_sid,
          rank() OVER (PARTITION BY recruitment_requisition_history.recruitment_requisition_sid ORDER BY recruitment_requisition_history.creation_date_time) AS openrank
        FROM
          {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history
        WHERE recruitment_requisition_history.requisition_status_id = 3
         AND date(recruitment_requisition_history.valid_to_date) = '9999-12-31'
    ) AS opentime ON recreq.recruitment_requisition_sid = opentime.recruitment_requisition_sid
     AND opentime.openrank = 1
    LEFT OUTER JOIN (
      SELECT
          recruitment_requisition_history.closed_date_time AS open_date,
          recruitment_requisition_history.recruitment_requisition_sid,
          rank() OVER (PARTITION BY recruitment_requisition_history.recruitment_requisition_sid ORDER BY recruitment_requisition_history.closed_date_time) AS openrankg
        FROM
          {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history
        WHERE recruitment_requisition_history.requisition_status_id = 1001
         AND date(recruitment_requisition_history.valid_to_date) = '9999-12-31'
    ) AS opentimeg ON recreq.recruitment_requisition_sid = opentimeg.recruitment_requisition_sid
     AND opentimeg.openrankg = 1
    LEFT OUTER JOIN (
      SELECT
          recruitment_requisition_history.creation_date_time AS cancel_date,
          recruitment_requisition_history.recruitment_requisition_sid,
          rank() OVER (PARTITION BY recruitment_requisition_history.recruitment_requisition_sid ORDER BY recruitment_requisition_history.creation_date_time DESC) AS cancelrank
        FROM
          {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history
        WHERE recruitment_requisition_history.requisition_status_id = 12
         AND date(recruitment_requisition_history.valid_to_date) = '9999-12-31'
    ) AS canceled ON recreq.recruitment_requisition_sid = canceled.recruitment_requisition_sid
     AND canceled.cancelrank = 1
    LEFT OUTER JOIN (
      SELECT
          recruitment_requisition_history.closed_date_time AS cancel_date,
          recruitment_requisition_history.recruitment_requisition_sid,
          rank() OVER (PARTITION BY recruitment_requisition_history.recruitment_requisition_sid ORDER BY recruitment_requisition_history.closed_date_time DESC) AS cancelrankg
        FROM
          {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history
        WHERE recruitment_requisition_history.requisition_status_id = 1005
         AND date(recruitment_requisition_history.valid_to_date) = '9999-12-31'
    ) AS canceled_g ON recreq.recruitment_requisition_sid = canceled_g.recruitment_requisition_sid
     AND canceled_g.cancelrankg = 1
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_status AS rrs ON recreq.recruitment_requisition_sid = rrs.recruitment_requisition_sid
     AND date(rrs.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.ref_requisition_status AS recstatus ON rrs.requisition_status_id = recstatus.requisition_status_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj ON recreq.recruitment_job_sid = rj.recruitment_job_sid
     AND date(rj.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS recuser ON rj.recruiter_user_sid = recuser.recruitment_user_sid
     AND date(recuser.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN -- --------------Join to get Recruiter information
    {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS recuser2 ON recreq.hiring_manager_user_sid = recuser2.recruitment_user_sid
     AND date(recuser2.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN -- --------------Join to get HM information
    {{ params.param_hr_base_views_dataset_name }}.requisition_status AS rs ON r.requisition_sid = rs.requisition_sid
     AND date(rs.valid_to_date) = '9999-12-31'
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_position AS rp ON rp.requisition_sid = r.requisition_sid
     AND date(rp.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON rp.position_sid = jp.position_sid
     AND date(jp.valid_to_date) = '9999-12-31'
     AND date(jp.eff_to_date) = '9999-12-31'
     AND upper(jp.active_dw_ind) = 'Y'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stas ON r.application_status_sid = stas.status_sid
     AND date(stas.valid_to_date) = '9999-12-31'
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_department AS rd ON rd.requisition_sid = r.requisition_sid
     AND date(rd.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN (
      SELECT
          hr_dept_rollup.coid,
          hr_dept_rollup.process_level_code,
          hr_dept_rollup.lawson_company_num,
          hr_dept_rollup.account_unit_num,
          hr_dept_rollup.gl_company_num,
          hr_dept_rollup.cost_center,
          hr_dept_rollup.dept_code,
          concat(coalesce(hr_dept_rollup.coid, '00000'), hr_dept_rollup.process_level_code, coalesce(hr_dept_rollup.dept_code, '00000'), hr_dept_rollup.lawson_company_num) AS pl_uid
        FROM
          {{ params.param_hr_bi_views_dataset_name }}.hr_dept_rollup
    ) AS hrdr ON r.process_level_code = hrdr.process_level_code
     AND recreq.process_level_code = hrdr.process_level_code
     AND r.lawson_company_num = hrdr.lawson_company_num
     AND jp.account_unit_num = hrdr.account_unit_num
     AND jp.gl_company_num = hrdr.gl_company_num
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS gldc ON jp.gl_company_num = gldc.gl_company_num
     AND jp.account_unit_num = gldc.account_unit_num
     AND date(gldc.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON gldc.coid = ff.coid
     AND gldc.company_code = ff.company_code
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jc ON jp.job_code_sid = jc.job_code_sid
     AND date(jc.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON rd.dept_sid = dept.dept_sid
     AND date(dept.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN -- -------------------------Thomas Add for ILOB-----------------------------------
    {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department AS sf ON sf.dept_num = gldc.dept_num
     AND sf.coid = gldc.coid
     AND sf.company_code = gldc.company_code
    LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_department AS df ON sf.functional_dept_num = df.functional_dept_num
    LEFT OUTER JOIN -- -------------------------Thomas Add for ILOB-----------------------------------
    -- ------Integrated LOB---------------------------
    {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat1 ON recreq.process_level_code = mat1.process_level_code
     AND dept.dept_code = mat1.dept_code
     AND mat1.match_level_num = 1
    LEFT OUTER JOIN -- Process Level AND Dept Num
    {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat2 ON ff.lob_code = mat2.lob_code
     AND ff.sub_lob_code = mat2.sub_lob_code
     AND mat2.match_level_num = 2
    LEFT OUTER JOIN -- LOB and Sub LOB
    {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat3 ON df.functional_dept_desc = mat3.functional_dept_desc
     AND sf.sub_functional_dept_desc = mat3.sub_functional_dept_desc
     AND mat3.match_level_num = 3
    LEFT OUTER JOIN -- Function and Sub Function
    {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat4 ON recreq.process_level_code = mat4.process_level_code
     AND mat4.match_level_num = 4
    LEFT OUTER JOIN -- Process Level
    -- ------KeyTalent Mapping Start-------------
    {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt1 ON rkeyt1.match_level_num = 1
     AND jc.job_code = rkeyt1.job_code
     AND jc.job_code_desc = rkeyt1.job_code_desc
     AND upper(jp.position_code_desc) LIKE 'ACMO%'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt2 ON rkeyt2.match_level_num = 2
     AND jc.job_code = rkeyt2.job_code
     AND ff.lob_code = rkeyt2.lob_code
     AND jc.job_code_desc = rkeyt2.job_code_desc
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt3 ON rkeyt3.match_level_num = 3
     AND jc.job_code = rkeyt3.job_code
     AND jc.job_code_desc = rkeyt3.job_code_desc
     AND jp.position_code_desc = rkeyt3.job_title_text
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt4 ON rkeyt4.match_level_num = 4
     AND jc.job_code = rkeyt4.job_code
     AND jp.position_code_desc = rkeyt4.job_title_text
     AND recreq.process_level_code = rkeyt4.process_level_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt5 ON rkeyt5.match_level_num = 5
     AND jc.job_code = rkeyt5.job_code
     AND recreq.process_level_code = rkeyt5.process_level_code
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt6 ON rkeyt6.match_level_num = 6
     AND jc.job_code = rkeyt6.job_code
     AND recreq.process_level_code = rkeyt6.process_level_code
     AND upper(jp.position_code_desc) LIKE 'DIR PRGM%'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt7 ON rkeyt6.match_level_num = 7
     AND jc.job_code = rkeyt7.job_code
     AND recreq.process_level_code = rkeyt7.process_level_code
     AND dept.dept_code BETWEEN '70000' AND '79999'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt8 ON rkeyt8.match_level_num = 8
     AND jc.job_code = rkeyt8.job_code
     AND jc.job_code_desc = rkeyt8.job_code_desc
    LEFT OUTER JOIN (
      SELECT DISTINCT
          fact_hr_metric_snapshot.requisition_sid,
          fact_hr_metric_snapshot.source_system_code,
          fact_hr_metric_snapshot.metric_numerator_qty
        FROM
          {{ params.param_hr_views_dataset_name }}.fact_hr_metric_snapshot
        WHERE fact_hr_metric_snapshot.analytics_msr_sid = 80600
    ) AS opening ON recreq.lawson_requisition_sid = opening.requisition_sid
     AND recreq.source_system_code = opening.source_system_code
    LEFT OUTER JOIN (
      SELECT DISTINCT
          fact_hr_metric_snapshot.requisition_sid,
          fact_hr_metric_snapshot.source_system_code,
          fact_hr_metric_snapshot.metric_numerator_qty
        FROM
          {{ params.param_hr_views_dataset_name }}.fact_hr_metric_snapshot
        WHERE fact_hr_metric_snapshot.analytics_msr_sid = 80500
    ) AS filled ON recreq.lawson_requisition_sid = filled.requisition_sid
     AND recreq.source_system_code = filled.source_system_code
  WHERE (canceled_g.cancel_date > date_add(current_date('US/Central'), interval -37 MONTH)
   OR canceled.cancel_date > date_add(current_date('US/Central'), interval -37 MONTH))
   AND date(r.valid_to_date) = '9999-12-31'
   AND recstatus.status_desc IN(
    -- --Rolling 36 months
    -- --Rolling 36 months
    'Canceled', 'Cancelled'
  )
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, upper(hrdr.pl_uid), 11, 12, upper(concat(recuser.last_name, ', ', recuser.first_name)), upper(concat(recuser2.last_name, ', ', recuser2.first_name)), 15, 16, 17, 18, 19, 20, 21, 22, upper(CASE
    WHEN opening.metric_numerator_qty IS NULL THEN 'No'
    ELSE 'Yes'
  END), upper(CASE
    WHEN filled.metric_numerator_qty IS NULL THEN 'No'
    ELSE 'Yes'
  END)
;
