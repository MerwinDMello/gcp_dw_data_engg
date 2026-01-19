
CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.requisition_origination AS SELECT
    r.lawson_company_num AS hr_company,
    hrdr.coid,
    r.process_level_code AS process_level,
    r.requisition_num AS lawson_requisition_num,
    CASE
      WHEN upper(rr.source_system_code) = 'B' THEN rr.requisition_num
      ELSE NULL
    END AS infor_req_num,
    CASE
      WHEN upper(rr.source_system_code) = 'T' THEN rr.recruitment_requisition_num_text
      ELSE NULL
    END AS taleo_req_num,
    rr.source_system_code,
    rs.status_code,
    d.dept_code,
    ff.lob_code,
    jp.position_code,
    r.requisition_desc,
    jcl.job_class_desc,
    CASE
      WHEN trim(stas.status_code) = '01' THEN 'FT'
      WHEN trim(stas.status_code) = '02' THEN 'PT'
      WHEN trim(stas.status_code) = '03' THEN 'PRN'
      WHEN trim(stas.status_code) = '04' THEN 'TEMP'
      ELSE CAST(NULL as STRING)
    END AS emp_status,
    r.open_fte_percent AS fte_percentage,
    r.requisition_eff_date,
    r.requisition_open_date,
    r.requisition_closed_date,
    r.requisition_origination_date,
    r.position_needed_date,
    r.replacement_employee_num,
    emp.employee_last_name AS replacement_employee_last_name,
    emp.employee_first_name AS replacement_employee_first_name,
    concat(trim(recuser.last_name), ', ', trim(recuser.first_name)) AS recruiter_name,
    r.job_opening_cnt,
    r.replacement_flag,
    r.special_requirement_text AS acquisition_flag
  FROM
    {{ params.param_hr_base_views_dataset_name }}.requisition AS r
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_department AS rd ON r.requisition_sid = rd.requisition_sid
     AND date(rd.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN -- begin join to recruitment requisition
    (
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
          s.submission_sid
        FROM
          {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition AS recr
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS s ON recr.recruitment_requisition_sid = s.recruitment_requisition_sid
           AND date(s.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer AS o ON s.submission_sid = o.submission_sid
           AND date(o.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.offer_status AS os ON os.offer_sid = o.offer_sid
           AND date(os.valid_to_date) = '9999-12-31'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_offer_status AS ros ON ros.offer_status_id = os.offer_status_id
        WHERE date(recr.valid_to_date) = '9999-12-31'
        QUALIFY row_number() OVER (PARTITION BY recr.lawson_requisition_sid ORDER BY o.last_modified_date DESC, o.capture_date DESC, recr.approved_sw DESC, recr.recruitment_requisition_sid DESC, o.sequence_num DESC) = 1
    ) AS rr ON r.requisition_sid = rr.lawson_requisition_sid
    LEFT OUTER JOIN -- end join to recruitment requisition
    {{ params.param_hr_base_views_dataset_name }}.requisition_status AS rs ON r.requisition_sid = rs.requisition_sid
     AND date(rs.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN -- -Join to get Lawson Requisition status
    {{ params.param_hr_base_views_dataset_name }}.department AS d ON rd.dept_sid = d.dept_sid
     AND date(d.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_position AS rp ON r.requisition_sid = rp.requisition_sid
     AND date(rp.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON rp.position_sid = jp.position_sid
     AND date(jp.valid_to_date) = '9999-12-31'
     AND date(jp.eff_to_date) = '9999-12-31'
     AND upper(jp.active_dw_ind) = 'Y'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jc ON jp.job_code_sid = jc.job_code_sid
     AND date(jc.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jcl ON jc.job_class_sid = jcl.job_class_sid
     AND date(jcl.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_job AS rj ON rr.recruitment_job_sid = rj.recruitment_job_sid
     AND date(rj.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_job_schedule AS rjs ON rj.job_schedule_id = rjs.job_schedule_id
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS recuser ON rj.recruiter_user_sid = recuser.recruitment_user_sid
     AND date(recuser.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN -- --------Join to Last/Current Recruiter
    {{ params.param_hr_stnd_views_dataset_name }}.hr_dept_rollup AS hrdr ON r.process_level_code = hrdr.process_level_code
     AND d.dept_code = hrdr.dept_code
     AND r.lawson_company_num = hrdr.lawson_company_num
     AND jp.account_unit_num = hrdr.account_unit_num
     AND jp.gl_company_num = hrdr.gl_company_num
    LEFT OUTER JOIN (
      SELECT
          fact_facility.coid,
          fact_facility.lob_code
        FROM
          {{ params.param_pub_views_dataset_name }}.fact_facility
    ) AS ff ON hrdr.coid = ff.coid
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stas ON r.application_status_sid = stas.status_sid
     AND date(stas.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS emp ON r.replacement_employee_num = emp.employee_num
     AND r.lawson_company_num = emp.lawson_company_num
     AND date(emp.valid_to_date) = '9999-12-31'
  WHERE date(r.valid_to_date) = '9999-12-31'
   AND r.requisition_origination_date > date_add(current_date(), interval -37 MONTH)
;
