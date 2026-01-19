CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.pi_requisition AS WITH supervisor AS (
  SELECT
      -- --------GHR_Req_Status
      -- --------GHR Job NUMBER
      -- -- Job Code Desc
      -- --------GHR Job Category,
      -- --------GHR Job Category Description,
      -- --------Reason for Opening,
      -- --------Comments
      jp.position_sid,
      concat(trim(epsup.employee_last_name), ', ', trim(epsup.employee_first_name)) AS supervisor_name,
      CASE
        WHEN trim(sup.officer_code) = 'OFFICER' THEN 'Y'
        ELSE 'N'
      END AS is_supervisor_officer,
      concat(trim(epsup_parent.employee_last_name), ', ', trim(epsup_parent.employee_first_name)) AS parent_supervisor_name,
      CASE
        WHEN trim(sup_parent.officer_code) = 'OFFICER' THEN 'Y'
        ELSE 'N'
      END AS is_parent_supervisor_officer
    FROM
      {{ params.param_hr_base_views_dataset_name }}.job_position AS jp
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.supervisor AS sup ON jp.supervisor_sid = sup.supervisor_sid
       AND date(sup.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS epsup ON sup.employee_sid = epsup.employee_sid
       AND date(epsup.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.supervisor AS sup_parent ON sup.reporting_supervisor_sid = sup_parent.supervisor_sid
       AND date(sup_parent.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS epsup_parent ON sup_parent.employee_sid = epsup_parent.employee_sid
       AND date(epsup_parent.valid_to_date) = '9999-12-31'
    WHERE date(jp.valid_to_date) = '9999-12-31'
     AND date(jp.eff_to_date) = '9999-12-31'
)
SELECT DISTINCT
    rr.requisition_num AS infor_req_num,
    r.requisition_open_date AS req_open_date,
    rs.valid_from_date AS date_in_status,
    r.requisition_num AS lawson_requisition_num,
    rs.status_code AS legacy_lawson_req_status,
    rj.job_title_name AS requisition_title,
    r.lawson_company_num AS hr_company,
    ff.group_name,
    ff.division_name,
    ff.market_name,
    ff.lob_code AS lob,
    r.process_level_code AS process_level,
    pl.process_level_name,
    hrdr.cost_center AS gl_dept,
    d.dept_name,
    d.dept_code,
    jp.location_code AS primary_location,
    rl.location_desc AS primary_location_name,
    r.open_fte_percent AS fte_value,
    jcl.job_class_code,
    jcl.job_class_desc,
    jc.job_code_desc AS ghr_job_description,
    concat(recuser2.last_name, ', ', recuser2.first_name) AS hiring_manager_eid,
    recuser2.employee_num AS hiring_manager_name,
    ephire.email_text AS hiring_manager_email,
    concat(recuser.last_name, ', ', recuser.first_name) AS recruiter_eid,
    recuser.employee_num AS recruiter_name,
    eprecruit.email_text AS recruiter_email,
    CASE
      WHEN trim(stas.status_code) = '01' THEN 'FT'
      WHEN trim(stas.status_code) = '02' THEN 'PT'
      WHEN trim(stas.status_code) = '03' THEN 'PRN'
      WHEN trim(stas.status_code) = '04' THEN 'TEMP'
      ELSE CAST(NULL as STRING)
    END AS work_schedule,
    r.work_schedule_code AS shift,
    hrc.hr_code_desc AS shift_description,
    r.union_code,
    s1.supervisor_name,
    s1.is_supervisor_officer,
    s1.parent_supervisor_name,
    s1.is_parent_supervisor_officer,
    /*Trim(epsup.Employee_Last_Name)||', '||Trim(epsup.Employee_First_Name) AS Supervisor_Name, --Added Field
    CASE WHEN Trim(sup.Officer_Code) = 'OFFICER' THEN 'Y' ELSE 'N' END AS Is_Supervisor_Officer, --Added Field
    Trim(epsup_parent.Employee_Last_Name)||', '||Trim(epsup_parent.Employee_First_Name) AS Parent_Supervisor_Name, --Added Field
    CASE WHEN Trim(sup_parent.Officer_Code) = 'OFFICER' THEN 'Y' ELSE 'N' END AS Is_Parent_Supervisor_Officer, --Added Field*/
    r.replacement_employee_num,
    emp.employee_last_name AS replacement_employee_last_name,
    emp.employee_first_name AS replacement_employee_first_name
  FROM
    {{ params.param_hr_base_views_dataset_name }}.requisition AS r
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_department AS rd ON r.requisition_sid = rd.requisition_sid
     AND date(rd.valid_to_date) = '9999-12-31'
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
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_status AS rs ON r.requisition_sid = rs.requisition_sid
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
    LEFT OUTER JOIN -- --------------Join to get Recruiter information
    {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS recuser2 ON rr.hiring_manager_user_sid = recuser2.recruitment_user_sid
     AND date(recuser2.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN -- --------------Join to get HM information
    {{ params.param_hr_stnd_views_dataset_name }}.hr_dept_rollup AS hrdr ON r.process_level_code = hrdr.process_level_code
     AND d.dept_code = hrdr.dept_code
     AND r.lawson_company_num = hrdr.lawson_company_num
     AND jp.account_unit_num = hrdr.account_unit_num
     AND jp.gl_company_num = hrdr.gl_company_num
    LEFT OUTER JOIN (
      SELECT
          fact_facility.coid,
          fact_facility.lob_code,
          fact_facility.group_name,
          fact_facility.market_name,
          fact_facility.division_name
        FROM
          {{ params.param_pub_views_dataset_name }}.fact_facility
    ) AS ff ON hrdr.coid = ff.coid
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS stas ON r.application_status_sid = stas.status_sid
     AND date(stas.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS emp ON r.replacement_employee_num = emp.employee_num
     AND r.lawson_company_num = emp.lawson_company_num
     AND date(emp.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS hrc ON r.work_schedule_code = hrc.hr_code
     AND upper(hrc.hr_type_code) = 'WS'
     AND upper(hrc.active_ind) = 'A'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS pl ON r.process_level_code = pl.process_level_code
     AND date(pl.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_location AS rl ON jp.location_code = rl.location_code
    LEFT OUTER JOIN -- Added Location Join
    {{ params.param_hr_base_views_dataset_name }}.employee_person AS ephire ON recuser2.employee_num = ephire.employee_num
     AND date(ephire.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN -- Added Hiring Manager Email Join
    {{ params.param_hr_base_views_dataset_name }}.employee_person AS eprecruit ON recuser.employee_num = eprecruit.employee_num
     AND date(eprecruit.valid_to_date) = '9999-12-31'
     AND (eprecruit.lawson_company_num) = 5920
    LEFT OUTER JOIN -- Added Recruiter Email Join
    /*LEFT JOIN EDWHR_BASE_VIEWS.Supervisor sup --Added Immediate Supervisor Join
     ON jp.Supervisor_SID = sup.Supervisor_SID
     AND sup.Valid_To_Date = '9999-12-31'
    LEFT JOIN EDWHR_BASE_VIEWS.Employee_Person epsup --Added Immediate Supervisor Name Join
     ON sup.Employee_SID = epsup.Employee_SID
     AND epsup.Valid_To_Date = '9999-12-31'
    LEFT JOIN EDWHR_BASE_VIEWS.Supervisor sup_parent --Added Parent Supervisor Join
     ON sup.Reporting_Supervisor_SID = sup_parent.Supervisor_SID
     AND sup_parent.Valid_To_Date = '9999-12-31'
    LEFT JOIN EDWHR_BASE_VIEWS.Employee_Person epsup_parent --Added Parent Supervisor Name Join
     ON sup_parent.Employee_SID = epsup_parent.Employee_SID
     AND epsup_parent.Valid_To_Date = '9999-12-31'*/
    supervisor AS s1 ON jp.position_sid = s1.position_sid
  WHERE date(r.valid_to_date) = '9999-12-31'
   AND upper(rr.source_system_code) = 'B'
;
