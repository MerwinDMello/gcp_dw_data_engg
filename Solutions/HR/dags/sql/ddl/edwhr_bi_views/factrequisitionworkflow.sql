create or replace view {{ params.param_hr_bi_views_dataset_name }}.factrequisitionworkflow as SELECT
    --  =============================================
    --  Author: Cheryl Costa
    --  Create date: 03/12/2020
    --  Description: Requisition Approvals (Workflow)
    -- Metrics: How long did my requisition take to get approved? How many reqs did X approve?
    --  =============================================
    -- DISTINCT
    r.requisition_sid,
    r.process_level_code,
    r.location_code,
    r.requisition_num AS lawson_requisition_num,
    r.lawson_company_num,
    jcl.job_class_desc,
    r.requisition_eff_date,
    r.requisition_open_date,
    r.requisition_closed_date,
    r.requisition_origination_date,
    r.position_needed_date,
    r.last_update_date,
    rp.position_sid,
    concat(trim(cast(rp.position_sid as string)), jp.eff_to_date) AS position_key,
    hrdr.coid,
    concat(coalesce(hrdr.coid, '00000'), hrdr.process_level_code, coalesce(hrdr.dept_code, '00000'), r.lawson_company_num) AS pl_uid,
    r.originator_login_3_4_code,
    emp2.employee_num AS orig_emp_num,
    emp2.employee_first_name AS originator_firstname,
    emp2.employee_last_name AS originator_lastname,
    jpo.position_code_desc AS originator_position_desc,
    r.replacement_employee_num,
    r.work_schedule_code,
    r.open_fte_percent,
    CASE
      WHEN stas.status_code = '01' THEN 'FT'
      WHEN stas.status_code = '02' THEN 'PT'
      WHEN stas.status_code = '03' THEN 'PRN'
      WHEN stas.status_code = '04' THEN 'TEMP'
      ELSE CAST(NULL as STRING)
    END AS emp_status,
    rw.workflow_seq_num AS approver_order,
    rw.workflow_role_name,
    CASE
      WHEN upper(rw.workflow_role_name) = 'DIVISIONCSO' THEN 'Division CSO Approval'
      WHEN upper(rw.workflow_role_name) = 'CSO' THEN 'CSO Approval'
      WHEN upper(rw.workflow_role_name) = 'HR REPS' THEN 'HR Approval'
      ELSE 'All Other Approvers'
    END AS approver_group,
    rw.workflow_task_name,
    rw.workflow_user_id_code,
    emp.employee_first_name AS approver_firstname,
    emp.employee_last_name AS approver_lastname,
    rw.start_date,
    rw.start_time,
    rw.start_date + (rw.start_time - TIME '00:00:00') AS start_timestamp,
    rw.end_date,
    rw.end_time,
    rw.end_date + (rw.end_time - TIME '00:00:00') AS end_timestamp,
    rw.source_system_code
  FROM
   {{ params.param_hr_base_views_dataset_name }}.requisition_workflow AS rw 
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition AS r ON rw.requisition_sid = r.requisition_sid
     AND DATE(r.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN (
      SELECT
          employee.employee_34_login_code,
          employee.employee_num
        FROM
         {{ params.param_hr_base_views_dataset_name }}.employee
        WHERE DATE(employee.valid_to_date) = DATE('9999-12-31')
        GROUP BY 1, 2
    ) AS e ON rw.workflow_user_id_code = e.employee_34_login_code
    LEFT OUTER JOIN (
      SELECT
          employee_person.employee_first_name,
          employee_person.employee_last_name,
          employee_person.employee_num
        FROM
         {{ params.param_hr_base_views_dataset_name }}.employee_person
        WHERE DATE(employee_person.valid_to_date) = DATE('9999-12-31')
        GROUP BY 1, 2, 3
    ) AS emp ON e.employee_num = emp.employee_num
    LEFT OUTER JOIN (
      SELECT
          employee.employee_34_login_code,
          employee.employee_num
        FROM
         {{ params.param_hr_base_views_dataset_name }}.employee
        WHERE DATE(employee.valid_to_date) = DATE('9999-12-31')
        GROUP BY 1, 2
    ) AS e2 ON r.originator_login_3_4_code = e2.employee_34_login_code
    LEFT OUTER JOIN (
      SELECT
          employee_person.employee_sid,
          employee_person.employee_first_name,
          employee_person.employee_last_name,
          employee_person.employee_num
        FROM
         {{ params.param_hr_base_views_dataset_name }}.employee_person
        WHERE DATE(employee_person.valid_to_date) = DATE('9999-12-31')
    ) AS emp2 ON e2.employee_num = emp2.employee_num
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_position AS ep ON emp2.employee_sid = ep.employee_sid
     AND DATE(ep.valid_to_date) = DATE('9999-12-31')
     AND r.requisition_origination_date BETWEEN ep.eff_from_date AND ep.eff_to_date
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jpo ON ep.position_sid = jpo.position_sid
     AND DATE(jpo.valid_to_date) = DATE('9999-12-31')
     AND ep.eff_from_date BETWEEN jpo.eff_from_date AND jpo.eff_to_date
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_position AS rp ON r.requisition_sid = rp.requisition_sid
     AND r.process_level_code = rp.process_level_code
     AND DATE(rp.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON rp.position_sid = jp.position_sid
     AND DATE(jp.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_department AS rd ON r.requisition_sid = rd.requisition_sid
     AND DATE(rd.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS d ON rd.dept_sid = d.dept_sid
     AND DATE(d.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.hr_dept_rollup AS hrdr ON r.process_level_code = hrdr.process_level_code
     AND d.dept_code = hrdr.dept_code
     AND r.lawson_company_num = hrdr.lawson_company_num
     AND jp.account_unit_num = hrdr.account_unit_num
     AND jp.gl_company_num = hrdr.gl_company_num
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jc ON jp.job_code_sid = jc.job_code_sid
     AND DATE(jc.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jcl ON jc.job_class_sid = jcl.job_class_sid
     AND DATE(jcl.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stas ON r.application_status_sid = stas.status_sid
     AND DATE(stas.valid_to_date) = DATE('9999-12-31')
  WHERE r.requisition_eff_date BETWEEN date_add(last_day(date_add(current_date('US/Central'), interval -24 MONTH)), interval 1 DAY) AND date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
   AND DATE(rw.end_date) < DATE('9999-12-31')
  QUALIFY row_number() OVER (PARTITION BY rw.requisition_sid, approver_order ORDER BY approver_order, rw.workflow_user_id_code) = 1
