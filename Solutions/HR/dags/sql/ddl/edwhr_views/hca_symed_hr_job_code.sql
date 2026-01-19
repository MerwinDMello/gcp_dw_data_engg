
  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.hca_symed_hr_job_code AS SELECT DISTINCT
      empcount.total AS active_employee_cnt,
      reqcount.total AS open_requisition_cnt,
      ff.lob_code,
      ff.sub_lob_code,
      ff.division_name,
      ff.market_name,
      concat(jp.lawson_company_num, trim(jp.position_code)) AS source_code,
      jp.lawson_company_num,
      hrc.company_name,
      jp.position_code,
      jp.position_code_desc,
      jp.eff_from_date,
      jp.process_level_code,
      pl.process_level_name,
      rchc.hospital_level_code,
      dept.dept_code,
      dept.dept_name,
      fsfd.functional_dept_num,
      fsfd.functional_dept_desc,
      fsfd.sub_functional_dept_num,
      fsfd.sub_functional_dept_desc,
      jcl.job_class_code,
      jcd.job_code,
      jcd.job_code_desc,
      jp.location_code,
      rl.location_desc,
      jp.user_level_code,
      jp.union_code,
      sup.supervisor_code,
      sup.supervisor_desc,
      CASE
        WHEN sup.employee_num = 0 THEN NULL
        ELSE sup.employee_num
      END AS supervisor_employee_num,
      concat(trim(supep.employee_last_name), ', ', trim(supep.employee_first_name), ' ', trim(supep.employee_middle_name)) AS supervisor_full_name,
      rsup.supervisor_code AS supervisor_report_to_code,
      sup.officer_code AS supervisor_user_1_code,
      lsup.supervisor_code AS supervisor_link_code,
      jp.overtime_exempt_ind,
      jp.schedule_work_code AS work_schedule_code,
      jp.overtime_plan_code,
      jp.pay_grade_schedule_code,
      jp.pay_grade_code,
      CASE
        WHEN jpcrn.personnel_code IS NOT NULL THEN 'X'
        ELSE CAST(NULL as STRING)
      END AS rn_license_required_ind
    FROM
      {{ params.param_hr_base_views_dataset_name }}.job_position AS jp
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk ON jp.account_unit_num = xwalk.account_unit_num
       AND jp.gl_company_num = xwalk.gl_company_num
       AND date(xwalk.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON xwalk.coid = ff.coid
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON jp.dept_sid = dept.dept_sid
       AND date(dept.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS pl ON dept.process_level_sid = pl.process_level_sid
       AND date(pl.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department AS fsfd ON xwalk.coid = fsfd.coid
       AND xwalk.dept_num = fsfd.dept_num
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_company AS hrc ON jp.hr_company_sid = hrc.hr_company_sid
       AND date(hrc.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jcd ON jp.job_code_sid = jcd.job_code_sid
       AND date(jcd.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jcl ON jcd.job_class_sid = jcl.job_class_sid
       AND date(jcl.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.supervisor AS sup ON jp.supervisor_sid = sup.supervisor_sid
       AND date(sup.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS supep ON sup.employee_sid = supep.employee_sid
       AND date(supep.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.supervisor AS rsup ON sup.reporting_supervisor_sid = rsup.supervisor_sid
       AND date(rsup.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.supervisor AS lsup ON jp.link_supervisor_sid = lsup.supervisor_sid
       AND date(lsup.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_personnel_code AS jpcrn ON jp.position_sid = jpcrn.position_sid
       AND date(jpcrn.valid_to_date) = '9999-12-31'
       AND upper(jpcrn.personnel_code) = 'NURLIC080'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_personnel_code AS jpc ON jp.position_sid = jpc.position_sid
       AND date(jpc.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_location AS rl ON jp.location_code = rl.location_code
      LEFT OUTER JOIN (
        SELECT
            ref_coid_hospital_category.coid,
            ref_coid_hospital_category.year_num,
            ref_coid_hospital_category.company_code,
            ref_coid_hospital_category.hospital_level_code
          FROM
            {{ params.param_hr_base_views_dataset_name }}.ref_coid_hospital_category
          QUALIFY row_number() OVER (PARTITION BY ref_coid_hospital_category.coid ORDER BY ref_coid_hospital_category.year_num DESC) = 1
      ) AS rchc ON ff.coid = rchc.coid
       AND ff.company_code = rchc.company_code
      LEFT OUTER JOIN --  qualify because the categories can change based on EBITDA year over year, so we want the highest year num / most recent year's value vs. hardcoding a year
      (
        SELECT
            ep.position_sid,
            count(ep.employee_num) AS total
          FROM
            {{ params.param_hr_base_views_dataset_name }}.employee_position AS ep
            INNER JOIN {{ params.param_hr_base_views_dataset_name }}.junc_employee_status AS jes ON ep.employee_sid = jes.employee_sid
             AND date(jes.valid_to_date) = '9999-12-31'
             AND upper(jes.status_type_code) = 'EMP'
             AND jes.status_code IN(
              '01', '02', '03', '04', '05'
            )
          WHERE date(ep.valid_to_date) = '9999-12-31'
           AND date(ep.eff_to_date) = '9999-12-31'
          GROUP BY 1
      ) AS empcount ON jp.position_sid = empcount.position_sid
      LEFT OUTER JOIN (
        SELECT
            rp.position_sid,
            count(req.requisition_sid) AS total
          FROM
            {{ params.param_hr_base_views_dataset_name }}.requisition AS req
            INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_status AS rs ON req.requisition_sid = rs.requisition_sid
             AND date(rs.valid_to_date) = '9999-12-31'
             AND rs.status_code IN(
              'WFINPROG', 'WFAPPROVE', 'REQ-HOLD', 'INTERNAL'
            )
            INNER JOIN {{ params.param_hr_base_views_dataset_name }}.requisition_position AS rp ON req.requisition_sid = rp.requisition_sid
             AND date(rp.valid_to_date) = '9999-12-31'
          WHERE date(req.valid_to_date) = '9999-12-31'
          GROUP BY 1
      ) AS reqcount ON jp.position_sid = reqcount.position_sid
    WHERE date(jp.valid_to_date) = '9999-12-31'
     AND date(jp.eff_to_date) = '9999-12-31'
  ;

