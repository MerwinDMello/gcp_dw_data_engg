  CREATE TEMPORARY TABLE future_term
    AS
      SELECT
          pa.employee_sid,
          pa.employee_num,
          pa.eff_from_date,
          pa.action_code,
          pa.action_reason_text,
          ar.action_reason_desc,
          date_diff(pa.eff_from_date, current_date(), DAY) AS days_until_term
        FROM
          {{ params.param_hr_base_views_dataset_name }}.person_action AS pa
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_action_reason AS ar ON pa.lawson_company_num = ar.lawson_company_num
           AND pa.action_reason_text = ar.action_reason_text
        WHERE pa.valid_to_date = DATETIME("9999-12-31 23:59:59") 
         AND upper(pa.action_code) = '1TERMPEND'
         AND pa.eff_from_date > current_date()
         AND date_diff(pa.eff_from_date, current_date(), DAY) <= 30
  ;
  DROP TABLE {{ params.param_hr_stage_dataset_name }}.glint_hca_employee_work2;
  CREATE TABLE {{ params.param_hr_stage_dataset_name }}.glint_hca_employee_work2
    AS
      SELECT DISTINCT
          trim('ACTIVE    ') AS status,
          ep.employee_num AS employee_num,
          -- CASE
          --   WHEN coalesce(trim(ada.ad_account_email_addr_txt), trim(epers.email_text)) = 'email@invalid.com'
          --    OR coalesce(trim(ada.ad_account_email_addr_txt), trim(epers.email_text)) = ''
          --    OR coalesce(trim(ada.ad_account_email_addr_txt), trim(epers.email_text)) IS NULL THEN 
             concat(trim(ee.employee_34_login_code), '@hca.corpad.net')
          --   ELSE coalesce(trim(ada.ad_account_email_addr_txt), trim(epers.email_text))
          -- END 
          AS email_address,
          trim(epers.employee_first_name) AS first_name,
          trim(epers.employee_last_name) AS last_name,
          trim(epers.employee_middle_name) AS middle_name,
          trim(sup.supervisor_code) AS supervisor_cd,
          er.supervisor_employee_id AS supervisor_employee_id,
          er.supervisor_name AS supervisor_name,
          CASE
            WHEN sup.employee_sid = 0 THEN 'Vacant'
            ELSE 'Non-Vacant'
          END AS immediate_supervisor_vacancy_flag,
          concat(xwalk.coid, '-', xwalk.dept_num) AS coid_dept_main,
          trim(ee.employee_34_login_code) AS employee_3_4_id,
          ee.hire_date AS date_hired,
          NULL AS termination_date,
          ee.adjusted_hire_date AS adjusted_hire_date,
          ee.anniversary_date AS anniversary_date,
          epers.birth_date AS birthdate,
          trim(CAST(floor(date_diff(current_date(), epers.birth_date, DAY) / NUMERIC '365.25') as STRING)) AS age,
          concat(trim(jcl.job_class_code), '-', jcl.job_class_desc) AS job_class,
          trim(epers.gender_code) AS gender,
          trim(empstat.status_desc) AS employee_status_desc,
          trim(jes.status_code) AS auxiliary_status,
          ee.remote_sw AS remote_flag,
          trim(epers.ethnic_origin_code) AS eeo_class,
          coalesce(hrcun.hr_code_desc, 'N/A') AS union_cd_desc,
          concat(trim(jcd.job_code), '-', jcd.job_code_desc) AS job_code,
          trim(jp.position_code) AS position_code,
          trim(substr(trim(jp.position_code_desc), 1, 30)) AS position_code_desc,
          ed.detail_value_date AS rn_experience,
          trim(fsfd.functional_dept_desc) AS function,
          trim(fsfd.sub_functional_dept_desc) AS sub_function,
          'HCA' AS hca_flag,
          trim(ff.lob_name) AS lob,
          trim(ff.sub_lob_name) AS sub_lob,
          concat(ff.group_code, '-', ff.group_name) AS group_num,
          concat(ff.division_code, '-', ff.division_name) AS division_num,
          concat(ff.market_code, '-', ff.market_name) AS market_num,
          ep.lawson_company_num AS hr_company_curr,
          trim(xwalk.coid) AS coid,
          trim(rchc.hospital_level_code) AS facility_level,
          concat(pl.process_level_code, ': ', d.dept_code, '-', d.dept_name) AS pl_department,
          pl.process_level_code AS process_level_num,
          concat(pl.process_level_code, '-', pl.process_level_name) AS process_level_home_curr,
          concat(legxwalk.prcs_lvl, '-', legpl.process_level_name) AS process_level_hca,
          concat(d.dept_code, '-', d.dept_name) AS dept_num_home_curr,
          concat(trim(ep.working_location_code), '-', trim(rl.location_desc)) AS location_cd,
          trim(kt.key_talent_group_text) AS key_talent_group,
          trim(ilob.category_desc) AS ilob_category,
          trim(ilob.sub_category_desc) AS ilob_sub_category,
          trim(hro.business_unit_name) AS business_unit,
          trim(hro.business_unit_segment_name) AS business_segment,
          CASE
            WHEN upper(fac.time_zone_actual) = 'L' THEN 'America/Anchorage'
            WHEN upper(fac.time_zone_actual) = 'C' THEN 'America/Chicago'
            WHEN upper(fac.time_zone_actual) = 'P' THEN 'America/Los_Angeles'
            WHEN upper(fac.time_zone_actual) = 'E' THEN 'America/New_York'
            WHEN upper(fac.time_zone_actual) = 'M' THEN 'America/Phoenix'
            ELSE 'America/Chicago'
          END AS time_zone,
          CASE
            WHEN ecd.employee_code IS NOT NULL THEN 'RN Flag'
            ELSE CAST(NULL as STRING)
          END AS employee_personnel_cd,
          'HCA Healthcare' AS organization,
          trim(ft.action_code) AS action_code,
          trim(ft.action_reason_text) AS action_reason,
          ft.eff_from_date AS action_eff_date
        FROM
          {{ params.param_hr_base_views_dataset_name }}.employee_position AS ep
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON ep.position_sid = jp.position_sid
           AND jp.valid_to_date = DATETIME("9999-12-31 23:59:59") 
           AND ep.eff_from_date BETWEEN jp.eff_from_date AND jp.eff_to_date
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_code AS jcd ON jp.job_code_sid = jcd.job_code_sid
           AND jcd.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jcl ON jcd.job_class_sid = jcl.job_class_sid
           AND jcl.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.supervisor AS sup ON jp.supervisor_sid = sup.supervisor_sid
           AND sup.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS eperssup ON sup.employee_sid = eperssup.employee_sid
           AND eperssup.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS ee ON ep.employee_sid = ee.employee_sid
           AND ee.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.junc_employee_status AS jes ON ep.employee_sid = jes.employee_sid
           AND upper(jes.status_type_code) = 'AUX'
           AND jes.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          -- LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.ad_account AS ada ON ee.employee_34_login_code = ada.ad_account_user_id
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS epers ON ep.employee_sid = epers.employee_sid
           AND epers.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk ON ep.account_unit_num = xwalk.account_unit_num
           AND ep.gl_company_num = xwalk.gl_company_num
           AND xwalk.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON xwalk.coid = ff.coid
           AND xwalk.company_code = ff.company_code
          LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_sub_functional_department AS fsfd ON xwalk.coid = fsfd.coid
           AND xwalk.company_code = fsfd.company_code
           AND xwalk.dept_num = fsfd.dept_num
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_coid_hospital_category AS rchc ON xwalk.coid = rchc.coid
           AND xwalk.company_code = rchc.company_code
           AND rchc.year_num = extract(YEAR from current_date())
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_location AS rl ON ep.working_location_code = rl.location_code
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS d ON ep.dept_sid = d.dept_sid
           AND d.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.facility AS fac ON xwalk.coid = fac.coid
           AND xwalk.company_code = fac.company_code
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_code_detail AS ecd ON ep.employee_sid = ecd.employee_sid
           AND ecd.valid_to_date = DATETIME("9999-12-31 23:59:59") 
           AND upper(ecd.employee_code) = 'NURLIC080'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS pl ON d.process_level_sid = pl.process_level_sid
           AND pl.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS hrcun ON jp.union_code = hrcun.hr_code
           AND upper(hrcun.hr_type_code) = 'UN'
           AND upper(hrcun.active_ind) = 'A'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_detail AS ed ON ep.employee_sid = ed.employee_sid
           AND ed.valid_to_date = DATETIME("9999-12-31 23:59:59") 
           AND upper(ed.employee_detail_code) = '59'
          LEFT OUTER JOIN {{ params.param_hr_stage_dataset_name }}.legacy_to_lawson_crosswalk AS legxwalk ON xwalk.account_unit_num = legxwalk.acct_unit
           AND xwalk.gl_company_num = legxwalk.cmpy
           AND CAST(xwalk.dept_num AS INT64) = legxwalk.hca_dept
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS legpl ON legxwalk.prcs_lvl = legpl.process_level_code
           AND legpl.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.junc_employee_status AS emplstts ON ep.employee_sid = emplstts.employee_sid
           AND emplstts.valid_to_date = DATETIME("9999-12-31 23:59:59") 
           AND upper(emplstts.status_type_code) = 'EMP'
           AND emplstts.status_code IN(
            '01', '02', '03', '04', '05', '99'
          )
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS empstat ON emplstts.status_sid = empstat.status_sid
           AND empstat.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_roster AS er ON ep.employee_sid = er.employee_sid
           AND ep.position_sid = er.position_sid
           AND upper(er.active_dw_ind) = 'Y'
          LEFT OUTER JOIN {{ params.param_pub_views_dataset_name }}.functional_department AS df ON fsfd.functional_dept_num = df.functional_dept_num
          LEFT OUTER JOIN -- --Used for below ILOB Joins
          -- ILOB Start--
          {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat1 ON pl.process_level_code = mat1.process_level_code
           AND d.dept_code = mat1.dept_code
           AND mat1.match_level_num = 1
          LEFT OUTER JOIN --  Process Level AND Dept Num
          {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat2 ON ff.lob_code = mat2.lob_code
           AND ff.sub_lob_code = mat2.sub_lob_code
           AND mat2.match_level_num = 2
          LEFT OUTER JOIN --  LOB and Sub LOB
          {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat3 ON df.functional_dept_desc = mat3.functional_dept_desc
           AND fsfd.sub_functional_dept_desc = mat3.sub_functional_dept_desc
           AND mat3.match_level_num = 3
          LEFT OUTER JOIN --  Function and Sub Function
          {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS mat4 ON pl.process_level_code = mat4.process_level_code
           AND mat4.match_level_num = 4
          LEFT OUTER JOIN --  Process Level
          {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS ilob ON coalesce(mat1.integrated_lob_id, mat4.integrated_lob_id, mat3.integrated_lob_id, mat2.integrated_lob_id) = ilob.integrated_lob_id
          LEFT OUTER JOIN -- KeyTalent Mapping Start--
          {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt1 ON rkeyt1.match_level_num = 1
           AND jcd.job_code = rkeyt1.job_code
           AND jcd.job_code_desc = rkeyt1.job_code_desc
           AND upper(jp.position_code_desc) LIKE 'ACMO%'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt2 ON rkeyt2.match_level_num = 2
           AND jcd.job_code = rkeyt2.job_code
           AND ff.lob_code = rkeyt2.lob_code
           AND jcd.job_code_desc = rkeyt2.job_code_desc
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt3 ON rkeyt3.match_level_num = 3
           AND jcd.job_code = rkeyt3.job_code
           AND jcd.job_code_desc = rkeyt3.job_code_desc
           AND jp.position_code_desc = rkeyt3.job_title_text
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt4 ON rkeyt4.match_level_num = 4
           AND jcd.job_code = rkeyt4.job_code
           AND jp.position_code_desc = rkeyt4.job_title_text
           AND pl.process_level_code = rkeyt4.process_level_code
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt5 ON rkeyt5.match_level_num = 5
           AND jcd.job_code = rkeyt5.job_code
           AND pl.process_level_code = rkeyt5.process_level_code
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt6 ON rkeyt6.match_level_num = 6
           AND jcd.job_code = rkeyt6.job_code
           AND pl.process_level_code = rkeyt6.process_level_code
           AND upper(jp.position_code_desc) LIKE 'DIR PRGM%'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt7 ON rkeyt6.match_level_num = 7
           AND jcd.job_code = rkeyt7.job_code
           AND pl.process_level_code = rkeyt7.process_level_code
           AND d.dept_code BETWEEN '70000' AND '79999'
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS rkeyt8 ON rkeyt8.match_level_num = 8
           AND jcd.job_code = rkeyt8.job_code
           AND jcd.job_code_desc = rkeyt8.job_code_desc
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_bi_key_talent AS kt ON coalesce(rkeyt1.key_talent_id, rkeyt2.key_talent_id, rkeyt3.key_talent_id, rkeyt4.key_talent_id, rkeyt5.key_talent_id, rkeyt6.key_talent_id, rkeyt7.key_talent_id, rkeyt8.key_talent_id) = kt.key_talent_id
          LEFT OUTER JOIN -- HR Ops Rollup Mapping Start-
          {{ params.param_hr_base_views_dataset_name }}.ref_hr_operations AS hro ON pl.process_level_code = hro.process_level_code
          LEFT OUTER JOIN -- Future Term Mapping Start--
          future_term AS ft ON ep.employee_num = ft.employee_num
        WHERE ep.valid_to_date = DATETIME("9999-12-31 23:59:59") 
         AND ep.eff_to_date = '9999-12-31'
         AND ep.position_level_sequence_num = 1
         AND ep.lawson_company_num <> 300
        QUALIFY row_number() OVER (PARTITION BY ee.employee_34_login_code ORDER BY CASE
          WHEN upper(emplstts.status_code) = '01' THEN 1
          WHEN upper(emplstts.status_code) = '02' THEN 2
          WHEN upper(emplstts.status_code) = '03' THEN 3
          WHEN upper(emplstts.status_code) = '04' THEN 4
          WHEN upper(emplstts.status_code) = '05' THEN 5
          WHEN upper(emplstts.status_code) = '99' THEN 6
        END) = 1
  ;
DROP TABLE future_term;