BEGIN
DECLARE DUP_COUNT INT64;
BEGIN TRANSACTION;
INSERT INTO {{ params.param_hr_core_dataset_name }}.fact_hr_metric_mv (employee_sid, requisition_sid, position_sid, date_id, analytics_msr_sid, dept_sid, job_class_sid, job_code_sid, location_code, coid, company_code, functional_dept_num, sub_functional_dept_num, auxiliary_status_sid, employee_status_sid, key_talent_id, integrated_lob_id, action_code, action_reason_text, lawson_company_num, process_level_code, work_schedule_code, recruiter_owner_user_sid, requisition_approval_date, employee_num, metric_numerator_qty, metric_denominator_qty, source_system_code, dw_last_update_date_time)
    SELECT
        pa.employee_sid,
        coalesce(emp_req.requisition_sid, 0) AS requisition_sid,
        pa.position_sid,
        pa.eff_from_date AS date_id,
        dm.analytics_msr_sid,
        pa.dept_sid,
        jc.job_class_sid,
        jc.job_code_sid,
        pa.working_location_code AS location_code,
        gldc.coid,
        gldc.company_code,
        sf.functional_dept_num,
        sf.sub_functional_dept_num,
        coalesce(s.aux_status_sid, pa.status_sid) AS auxiliary_status_sid,
        es.emp_status_sid AS employee_status_sid,
        coalesce(rkeyt1.key_talent_id, rkeyt2.key_talent_id, rkeyt3.key_talent_id, rkeyt4.key_talent_id, rkeyt5.key_talent_id, rkeyt6.key_talent_id, rkeyt7.key_talent_id, rkeyt8.key_talent_id) AS key_talent_id,
        coalesce(mat1.integrated_lob_id, mat4.integrated_lob_id, mat3.integrated_lob_id, mat2.integrated_lob_id) AS integrated_lob_id,
        pa.action_code,
        pa.action_reason_text,
        pa.lawson_company_num,
        pa.process_level_code,
        '0' AS work_schedule_code,
        0 AS recruiter_owner_user_sid,
        CAST(NULL AS DATE) AS requisition_approval_date,
        pa.employee_num,
        1 AS metric_numerator_qty,
        0 AS metric_denominator_qty,
        pa.source_system_code,
        datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM
        (
          SELECT
              paxfer.employee_sid,
              paxfer.action_code,
              paxfer.employee_num,
              paxfer.lawson_company_num,
              paxfer.action_reason_text,
              paxfer.eff_from_date,
              paxfer.action_last_update_date,
              paxfer.valid_to_date,
              ep1.working_location_code,
              ep1.process_level_code,
              ep1.position_sid,
              ep1.dept_sid,
              ep1.gl_company_num,
              ep1.account_unit_num,
              jes.status_sid,
              e.source_system_code
            FROM
              {{ params.param_hr_base_views_dataset_name }}.person_action AS paxfer
              INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS e ON e.employee_sid = paxfer.employee_sid
               AND e.valid_to_date =  DATETIME("9999-12-31 23:59:59")
              LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.junc_employee_status AS jes ON e.employee_sid = jes.employee_sid
               AND upper(trim(jes.status_type_code)) = 'AUX'
               AND jes.valid_to_date =  DATETIME("9999-12-31 23:59:59")
              INNER JOIN (
                SELECT
                    a.employee_sid,
                    a.position_sid,
                    a.lawson_company_num,
                    a.process_level_code,
                    a.eff_from_date,
                    a.eff_to_date,
                    a.working_location_code,
                    a.dept_sid,
                    a.gl_company_num,
                    a.account_unit_num,
                    row_number() OVER (PARTITION BY a.employee_sid, a.position_level_sequence_num ORDER BY a.eff_from_date) AS row_rank
                  FROM
                    (
                      SELECT
                          employee_position.employee_sid,
                          employee_position.position_sid,
                          employee_position.lawson_company_num,
                          employee_position.process_level_code,
                          employee_position.working_location_code,
                          employee_position.dept_sid,
                          employee_position.gl_company_num,
                          employee_position.account_unit_num,
                          employee_position.position_level_sequence_num,
                          employee_position.eff_from_date,
                          employee_position.eff_to_date
                        FROM
                          {{ params.param_hr_base_views_dataset_name }}.employee_position
                        WHERE employee_position.valid_to_date = DATETIME("9999-12-31 23:59:59")
                    ) AS a
              ) AS ep2 ON ep2.employee_sid = paxfer.employee_sid
               AND ep2.eff_from_date = paxfer.eff_from_date
              INNER JOIN (
                SELECT
                    a.employee_sid,
                    a.position_sid,
                    a.lawson_company_num,
                    a.process_level_code,
                    a.eff_from_date,
                    a.eff_to_date,
                    a.working_location_code,
                    a.dept_sid,
                    a.gl_company_num,
                    a.account_unit_num,
                    row_number() OVER (PARTITION BY a.employee_sid, a.position_level_sequence_num ORDER BY a.eff_from_date) AS row_rank
                  FROM
                    (
                      SELECT
                          employee_position.employee_sid,
                          employee_position.position_sid,
                          employee_position.lawson_company_num,
                          employee_position.process_level_code,
                          employee_position.working_location_code,
                          employee_position.dept_sid,
                          employee_position.gl_company_num,
                          employee_position.account_unit_num,
                          employee_position.position_level_sequence_num,
                          employee_position.eff_from_date,
                          employee_position.eff_to_date
                        FROM
                          {{ params.param_hr_base_views_dataset_name }}.employee_position
                        WHERE employee_position.valid_to_date =  DATETIME("9999-12-31 23:59:59")
                    ) AS a
              ) AS ep1 ON ep1.employee_sid = paxfer.employee_sid
               AND paxfer.eff_from_date - 1 BETWEEN ep1.eff_from_date AND ep1.eff_to_date
            WHERE upper(trim(paxfer.action_code)) LIKE '%1XFER%'
             AND paxfer.valid_to_date =  DATETIME("9999-12-31 23:59:59")
             AND paxfer.eff_from_date >= '2016-01-01'
             AND ep1.row_rank + 1 = ep2.row_rank
             AND ep1.process_level_code = ep2.process_level_code
            QUALIFY row_number() OVER (PARTITION BY paxfer.employee_num, paxfer.lawson_company_num, paxfer.eff_from_date ORDER BY paxfer.action_last_update_date DESC) = 1
        ) AS pa
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS gldc ON pa.gl_company_num = gldc.gl_company_num
         AND pa.account_unit_num = gldc.account_unit_num
         AND gldc.valid_to_date =  DATETIME("9999-12-31 23:59:59")
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON pa.position_sid = jp.position_sid
         AND jp.valid_to_date =  DATETIME("9999-12-31 23:59:59")
         AND pa.eff_from_date BETWEEN jp.eff_from_date AND jp.eff_to_date
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jc ON jp.job_code_sid = jc.job_code_sid
         AND jc.valid_to_date =  DATETIME("9999-12-31 23:59:59")
        LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department AS sf ON sf.dept_num = gldc.dept_num
         AND sf.coid = gldc.coid
         AND sf.company_code = gldc.company_code
        LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.aux_status AS s ON pa.employee_sid = s.employee_sid
         AND pa.eff_from_date BETWEEN s.status_from_date AND s.status_to_date
         AND upper(trim(s.aux_status_code)) IN(
          'PRN', 'FT', 'PT', 'TEMP'
        )
        INNER JOIN {{ params.param_hr_stage_dataset_name }}.emp_status AS es ON pa.employee_sid = es.employee_sid
         AND pa.eff_from_date - 1 BETWEEN es.status_from_date AND es.status_to_date
         AND upper(trim(es.emp_status_code)) IN(
          '01', '02', '03', '04', '05'
        )
        INNER JOIN {{ params.param_dim_base_views_dataset_name }}.dim_analytics_measure AS dm ON upper(trim(dm.analytics_msr_name_child)) = 'HR_LAWSON_TRANSFERS'
        LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON gldc.coid = ff.coid
         AND gldc.company_code = ff.company_code
        LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_department AS df ON sf.functional_dept_num = df.functional_dept_num
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON pa.dept_sid = dept.dept_sid
         AND dept.valid_to_date =  DATETIME("9999-12-31 23:59:59")
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat1 ON pa.process_level_code = mat1.process_level_code
         AND dept.dept_code = mat1.dept_code
         AND mat1.match_level_num = 1
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat2 ON ff.lob_code = mat2.lob_code
         AND ff.sub_lob_code = mat2.sub_lob_code
         AND mat2.match_level_num = 2
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat3 ON upper(trim(df.functional_dept_desc)) = upper(trim(mat3.functional_dept_desc))
         AND upper(trim(sf.sub_functional_dept_desc)) = upper(trim(mat3.sub_functional_dept_desc))
         AND mat3.match_level_num = 3
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat4 ON pa.process_level_code = mat4.process_level_code
         AND mat4.match_level_num = 4
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt1 ON rkeyt1.match_level_num = 1
         AND jc.job_code = rkeyt1.job_code
         AND upper(trim(jc.job_code_desc)) = upper(trim(rkeyt1.job_code_desc))
         AND upper(trim(jp.position_code_desc)) LIKE 'ACMO%'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt2 ON rkeyt2.match_level_num = 2
         AND jc.job_code = rkeyt2.job_code
         AND ff.lob_code = rkeyt2.lob_code
         AND upper(trim(jc.job_code_desc)) = upper(trim(rkeyt2.job_code_desc))
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt3 ON rkeyt3.match_level_num = 3
         AND jc.job_code = rkeyt3.job_code
         AND upper(trim(jc.job_code_desc)) = upper(trim(rkeyt3.job_code_desc))
         AND upper(trim(jp.position_code_desc)) = upper(trim(rkeyt3.job_title_text))
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt4 ON rkeyt4.match_level_num = 4
         AND jc.job_code = rkeyt4.job_code
         AND upper(trim(jp.position_code_desc)) = upper(trim(rkeyt4.job_title_text))
         AND pa.process_level_code = rkeyt4.process_level_code
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt5 ON rkeyt5.match_level_num = 5
         AND jc.job_code = rkeyt5.job_code
         AND pa.process_level_code = rkeyt5.process_level_code
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt6 ON rkeyt6.match_level_num = 6
         AND jc.job_code = rkeyt6.job_code
         AND pa.process_level_code = rkeyt6.process_level_code
         AND upper(trim(jp.position_code_desc)) LIKE 'DIR PRGM%'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt7 ON rkeyt6.match_level_num = 7
         AND jc.job_code = rkeyt7.job_code
         AND pa.process_level_code = rkeyt7.process_level_code
         AND dept.dept_code BETWEEN '70000' AND '79999'
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt8 ON rkeyt8.match_level_num = 8
         AND jc.job_code = rkeyt8.job_code
         AND upper(trim(jc.job_code_desc)) = upper(trim(rkeyt8.job_code_desc))
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_requisition AS emp_req ON pa.employee_sid = emp_req.employee_sid
         AND pa.eff_from_date = emp_req.eff_from_date
         AND pa.action_code = emp_req.action_code
         AND emp_req.valid_to_date =  DATETIME("9999-12-31 23:59:59")
      WHERE pa.lawson_company_num <> 300
      QUALIFY row_number() OVER (PARTITION BY pa.employee_sid, emp_req.requisition_sid, pa.position_sid, date_id ORDER BY pa.action_last_update_date DESC) = metric_numerator_qty;


/* Test Unique Primary Index constarint set in Teradata*/
SET DUP_COUNT =(
  select count(*)
  from (
  select Employee_SID, Requisition_SID, Position_SID, Date_Id, Analytics_Msr_Sid
  from {{ params.param_hr_core_dataset_name }}.fact_hr_metric_mv
  group by Employee_SID, Requisition_SID, Position_SID, Date_Id, Analytics_Msr_Sid
  having count(*)>1
  )
);
IF DUP_COUNT <> 0 THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE =concat('Duplicates are not allowed in the table :{{ params.param_hr_core_dataset_name }}.fact_hr_metric_mv');
ELSE  
  COMMIT  TRANSACTION;
END IF;

END;