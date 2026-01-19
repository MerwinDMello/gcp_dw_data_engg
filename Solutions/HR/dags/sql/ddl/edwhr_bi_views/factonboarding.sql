create or replace view {{ params.param_hr_bi_views_dataset_name }}.factonboarding as SELECT
    --  =============================================
    --  Author:             Cheryl Costa
    --  Create date:    12/14/2020
    --  Description:    Candidate Onboarding
    --  =============================================
    co.candidate_onboarding_sid,
    co.requisition_sid,
    rr.recruitment_requisition_sid,
    co.employee_sid,
    co.candidate_sid,
    co.candidate_first_name,
    co.candidate_last_name,
    co.tour_start_date,
    co.tour_id,
    ot.tour_name,
    co.tour_status_id,
    ts.tour_status_text,
    co.tour_completion_pct,
    co.workflow_id,
    ow.workflow_name,
    co.workflow_status_id,
    ows.workflow_status_text,
    co.email_sent_status_id,
    ethr.email_sent_status_text,
    ethr.hr_status_desc,
    co.onboarding_confirmation_date,
    rr.lawson_requisition_num,
    co.recruitment_requisition_num_text AS taleo_requisition_num,
    c.candidate_num AS taleo_candidate_id,
    co.process_level_code,
    tc.completed_date AS tour_completed_date,
    ds.completed_date AS drugscreen_complete_date,
    hrdr.coid,
    concat(coalesce(hrdr.coid, '00000'), coalesce(hrdr.cost_center, '000')) AS coid_uid,
    concat(coalesce(hrdr.coid, '00000'), hrdr.process_level_code, coalesce(hrdr.dept_code, '00000'), hrdr.lawson_company_num) AS pl_uid,
    co.source_system_code
  FROM
   {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding AS co 
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_email_to_hr_status AS ethr ON co.email_sent_status_id = ethr.email_sent_status_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_tour AS ot ON co.tour_id = ot.tour_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_tour_status AS ts ON co.tour_status_id = ts.tour_status_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_workflow AS ow ON co.workflow_id = ow.workflow_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_onboarding_workflow_status AS ows ON co.workflow_status_id = ows.workflow_status_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_event AS tc ON co.recruitment_requisition_num_text = tc.recruitment_requisition_num_text
     AND tc.event_type_id = '2'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_onboarding_event AS ds ON co.recruitment_requisition_num_text = ds.recruitment_requisition_num_text
     AND ds.event_type_id = '1'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate AS c ON co.candidate_sid = c.candidate_sid
     AND DATE(c.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN (
      SELECT
          recr.recruitment_requisition_sid,
          recr.lawson_requisition_sid,
          recr.recruitment_requisition_num_text,
          recr.lawson_requisition_num,
          recr.recruitment_job_sid,
          recr.hiring_manager_user_sid,
          recr.source_system_code
        FROM
         {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS recr
        WHERE DATE(recr.valid_to_date) = DATE('9999-12-31')
        QUALIFY row_number() OVER (PARTITION BY recr.lawson_requisition_sid ORDER BY recr.source_system_code) = 1
    ) AS rr ON co.recruitment_requisition_num_text = rr.recruitment_requisition_num_text
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS r ON rr.lawson_requisition_sid = r.requisition_sid
     AND DATE(r.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_department AS rd ON r.requisition_sid = rd.requisition_sid
     AND DATE(rd.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_position AS rp ON r.requisition_sid = rp.requisition_sid
     AND DATE(rp.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON rp.position_sid = jp.position_sid
     AND DATE(jp.valid_to_date) = DATE('9999-12-31')
     AND DATE(jp.eff_to_date) = DATE('9999-12-31')
     AND upper(jp.active_dw_ind) = 'Y'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS d ON rd.dept_sid = d.dept_sid
     AND DATE(d.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_bi_views_dataset_name }}.hr_dept_rollup AS hrdr ON r.process_level_code = hrdr.process_level_code
     AND d.dept_code = hrdr.dept_code
     AND r.lawson_company_num = hrdr.lawson_company_num
     AND jp.account_unit_num = hrdr.account_unit_num
     AND jp.gl_company_num = hrdr.gl_company_num
  WHERE DATE(co.valid_to_date) = DATE('9999-12-31')