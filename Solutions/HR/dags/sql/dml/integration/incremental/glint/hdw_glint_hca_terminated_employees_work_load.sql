  DROP TABLE {{ params.param_hr_stage_dataset_name }}.glint_hca_terminated_employee_work;
  CREATE TABLE {{ params.param_hr_stage_dataset_name }}.glint_hca_terminated_employee_work
    AS
      SELECT
          CASE
            WHEN upper(fhrm.action_reason_text) = 'TI-DIVEST' THEN 'INACTIVE'
            WHEN date_diff(current_date(), fhrm.date_id, DAY) BETWEEN 1 AND 14 THEN 'ACTIVE'
            ELSE 'INACTIVE'
          END AS status,
          jes.status_code AS employee_status,
          status.status_desc AS employee_status_desc,
          concat(ff.group_code, '-', ff.group_name) AS group_num,
          concat(ff.division_code, '-', ff.division_name) AS division_num,
          concat(ff.market_code, '-', ff.market_name) AS market_num,
          fhrm.lawson_company_num AS hr_company_curr,
          fhrm.coid AS coid,
          concat(fhrm.process_level_code, '-', pl.process_level_name) AS process_level_home_curr,
          concat(dept.dept_code, '-', dept.dept_name) AS dept_num_home_curr,
          ff.lob_name AS lob,
          ff.sub_lob_name AS sub_lob,
          trim(epers.employee_first_name) AS first_name,
          trim(epers.employee_last_name) AS last_name,
          fhrm.employee_num AS employee_num,
          ee.employee_34_login_code AS employee_3_4_id,
          concat(ee.employee_34_login_code, '@hca.corpad.net') AS email_address,
          ee.anniversary_date AS anniversary_date,
          ed.detail_value_date AS rn_experience,
          epers.birth_date AS birthdate,
          ee.hire_date AS date_hired,
          fhrm.action_code AS action_code,
          fhrm.action_reason_text AS action_reason,
          fhrm.date_id AS action_eff_date,
          CAST(pemail.lawson_element_value as INT64) AS personal_email
        FROM
          {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS fhrm
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.junc_employee_status AS jes ON fhrm.employee_sid = jes.employee_sid
           AND upper(jes.status_type_code) = 'EMP'
           AND jes.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.status ON jes.status_sid = status.status_sid
           AND status.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person AS epers ON fhrm.employee_sid = epers.employee_sid
           AND epers.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS ee ON fhrm.employee_sid = ee.employee_sid
           AND ee.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_position AS jp ON fhrm.position_sid = jp.position_sid
           AND jp.valid_to_date = DATETIME("9999-12-31 23:59:59") 
           AND fhrm.date_id BETWEEN jp.eff_from_date AND jp.eff_to_date
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS xwalk ON jp.account_unit_num = xwalk.account_unit_num
           AND jp.gl_company_num = xwalk.gl_company_num
           AND xwalk.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          INNER JOIN {{ params.param_pub_views_dataset_name }}.fact_facility AS ff ON fhrm.coid = ff.coid
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON fhrm.dept_sid = dept.dept_sid
           AND dept.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS pl ON dept.process_level_sid = pl.process_level_sid
           AND pl.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_detail AS ed ON fhrm.employee_sid = ed.employee_sid
           AND ed.valid_to_date = DATETIME("9999-12-31 23:59:59") 
           AND ed.employee_detail_code = '59'
          LEFT OUTER JOIN (
            SELECT
                b.employee_sid,
                b.employee_num,
                b.eff_from,
                b.eff_to,
                b.lawson_element_num,
                b.lawson_element_desc,
                b.lawson_element_value,
                row_number() OVER (PARTITION BY b.employee_num ORDER BY b.employee_num, b.lawson_element_value DESC) AS total
              FROM
                (
                  SELECT
                      a.employee_sid,
                      a.employee_num,
                      a.eff_from,
                      CASE
                        WHEN lead(a.eff_from, 1) OVER (PARTITION BY a.employee_num, a.lawson_element_num ORDER BY a.lawson_element_num, a.eff_from) - 1 IS NULL THEN parse_date('%Y-%m-%d', '9999-12-31')
                        ELSE lead(a.eff_from, 1) OVER (PARTITION BY a.employee_num, a.lawson_element_num ORDER BY a.lawson_element_num, a.eff_from) - 1
                      END AS eff_to,
                      a.lawson_element_num,
                      a.lawson_element_desc,
                      a.lawson_element_value,
                      row_number() OVER (PARTITION BY a.employee_sid ORDER BY a.employee_num, a.eff_from) AS email_count
                    FROM
                      (
                        SELECT
                            hreh.employee_sid,
                            hreh.employee_num,
                            hreh.hr_employee_element_date AS eff_from,
                            hreh.lawson_element_num,
                            rle.lawson_element_desc,
                            CASE
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@HCA%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@PARAL%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@SARAHC%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@HEALTHT%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%.MEDCITY.%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@HEALTHON%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@MEDICALCI%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@MHSHEAL%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@MOUNTAINST%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@STDAVI%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@WESLEY%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@MSJ.ORG%' THEN NULL
                              ELSE hreh.hr_employee_value_alphanumeric_text
                            END AS lawson_element_value,
                            hreh.sequence_num,
                            row_number() OVER (PARTITION BY hreh.employee_num, hreh.lawson_element_num, hreh.hr_employee_element_date ORDER BY hreh.hr_employee_element_date, hreh.sequence_num DESC) AS row_count
                          FROM
                            {{ params.param_hr_base_views_dataset_name }}.hr_employee_history AS hreh
                            INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_lawson_element AS rle ON hreh.lawson_element_num = rle.lawson_element_num
                          WHERE hreh.valid_to_date = DATETIME("9999-12-31 23:59:59") 
                           AND hreh.lawson_element_num = 166
                      ) AS a
                    QUALIFY email_count = 1
                ) AS b
              QUALIFY total = 1
          ) AS pemail ON fhrm.employee_num = pemail.employee_num
        WHERE fhrm.analytics_msr_sid = 80300
         AND (upper(fhrm.action_code) = '1TERMPEND'
         AND fhrm.action_reason_text NOT IN(
          'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR'
        )
         OR upper(fhrm.action_reason_text) = 'TI-DIVEST')
         AND fhrm.date_id BETWEEN date_sub(current_date(), interval 21 DAY) AND date_sub(current_date(), interval 1 DAY)
         AND concat(fhrm.employee_num, fhrm.position_sid) IN(
          SELECT
              concat(a.employee_num, a.position_sid) AS emp_uid
            FROM
              (
                SELECT
                    fhrm_0.date_id,
                    fhrm_0.lawson_company_num,
                    fhrm_0.employee_num,
                    ee_0.hire_date,
                    fhrm_0.position_sid,
                    ep.position_level_sequence_num,
                    auxstat.status_code
                  FROM
                    {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS fhrm_0
                    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_position AS ep ON fhrm_0.employee_sid = ep.employee_sid
                     AND ep.valid_to_date = DATETIME("9999-12-31 23:59:59") 
                     AND fhrm_0.date_id BETWEEN ep.eff_from_date AND ep.eff_to_date
                    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS ee_0 ON fhrm_0.employee_sid = ee_0.employee_sid
                     AND ee_0.valid_to_date = DATETIME("9999-12-31 23:59:59") 
                    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS auxstat ON fhrm_0.auxiliary_status_sid = auxstat.status_sid
                     AND auxstat.valid_to_date = DATETIME("9999-12-31 23:59:59") 
                  WHERE fhrm_0.analytics_msr_sid = 80100
                   AND fhrm_0.date_id BETWEEN date_sub(current_date(), interval 30 DAY) AND date_sub(current_date(), interval 1 DAY)
                  QUALIFY row_number() OVER (PARTITION BY fhrm_0.employee_num, fhrm_0.date_id ORDER BY fhrm_0.date_id, auxstat.status_code, ee_0.hire_date, ep.position_level_sequence_num, CASE
                    WHEN upper(auxstat.status_code) = 'FT' THEN 1
                    WHEN upper(auxstat.status_code) = 'PT' THEN 2
                    WHEN upper(auxstat.status_code) = 'PRN' THEN 3
                    WHEN upper(auxstat.status_code) = 'TEMP' THEN 4
                  END) = 1
              ) AS a
        )
        QUALIFY row_number() OVER (PARTITION BY employee_num ORDER BY employee_num, fhrm.date_id DESC) = 1
      UNION ALL
      SELECT
          'ACTIVE' AS status,
          er.employee_status_code AS employee_status,
          er.employee_status_desc AS employee_status_desc,
          concat(trim(er.group_code), '-', trim(er.group_name)) AS group_num,
          concat(trim(er.division_code), '-', trim(er.division_name)) AS division_num,
          concat(trim(er.market_code), '-', trim(er.market_name)) AS market_num,
          er.lawson_company_num AS hr_company___curr,
          er.coid AS coid,
          concat(er.process_level_code, '-', er.process_level_name) AS process_level_home_curr,
          concat(er.dept_code, '-', er.dept_name) AS dept_num_home_curr,
          er.lob_name AS lob,
          er.sub_lob_name AS sub_lob,
          trim(er.employee_first_name) AS first_name,
          trim(er.employee_last_name) AS last_name,
          er.employee_num AS employee_num,
          er.employee_34_login_code AS employee_3_4_id,
          concat(er.employee_34_login_code, '@hca.corpad.net') AS email_address,
          er.anniversary_date AS anniversary_date,
          er.rn_experience_date AS rn_experience,
          er.birth_date AS birthdate,
          er.hire_date AS date_hired,
          pa.action_code AS action_code,
          pa.action_reason_text AS action_reason,
          pa.eff_from_date AS action_eff_date,
          CAST(pemail.lawson_element_value as INT64) AS personal_email
        FROM
          {{ params.param_hr_base_views_dataset_name }}.employee_roster AS er
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.person_action AS pa ON er.employee_sid = pa.employee_sid
           AND pa.valid_to_date = DATETIME("9999-12-31 23:59:59") 
           AND upper(pa.action_code) = '1TERMPEND'
           AND pa.action_reason_text NOT IN(
            'TV-FCLTRNS', 'TV-RELOHCA', 'TV-PLCHG', 'TV-SPCLXFR'
          )
           AND pa.eff_from_date > current_date()
           AND date_diff(pa.eff_from_date, current_date(), DAY) <= 30
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.junc_employee_status AS jes ON er.employee_sid = jes.employee_sid
           AND jes.valid_to_date = DATETIME("9999-12-31 23:59:59") 
           AND upper(jes.status_type_code) = 'EMP'
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.status AS empstat ON jes.status_sid = empstat.status_sid
           AND empstat.valid_to_date = DATETIME("9999-12-31 23:59:59") 
          LEFT OUTER JOIN (
            SELECT
                b.employee_sid,
                b.employee_num,
                b.eff_from,
                b.eff_to,
                b.lawson_element_num,
                b.lawson_element_desc,
                b.lawson_element_value,
                row_number() OVER (PARTITION BY b.employee_num ORDER BY b.employee_num, b.lawson_element_value DESC) AS total
              FROM
                (
                  SELECT
                      a.employee_sid,
                      a.employee_num,
                      a.eff_from,
                      CASE
                        WHEN lead(a.eff_from, 1) OVER (PARTITION BY a.employee_num, a.lawson_element_num ORDER BY a.lawson_element_num, a.eff_from) - 1 IS NULL THEN parse_date('%Y-%m-%d', '9999-12-31')
                        ELSE lead(a.eff_from, 1) OVER (PARTITION BY a.employee_num, a.lawson_element_num ORDER BY a.lawson_element_num, a.eff_from) - 1
                      END AS eff_to,
                      a.lawson_element_num,
                      a.lawson_element_desc,
                      a.lawson_element_value,
                      row_number() OVER (PARTITION BY a.employee_sid ORDER BY a.employee_num, a.eff_from) AS email_count
                    FROM
                      (
                        SELECT
                            hreh.employee_sid,
                            hreh.employee_num,
                            hreh.hr_employee_element_date AS eff_from,
                            hreh.lawson_element_num,
                            rle.lawson_element_desc,
                            CASE
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@HCA%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@PARAL%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@SARAHC%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@HEALTHT%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%.MEDCITY.%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@HEALTHON%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@MEDICALCI%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@MHSHEAL%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@MOUNTAINST%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@STDAVI%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@WESLEY%' THEN NULL
                              WHEN upper(hreh.hr_employee_value_alphanumeric_text) LIKE '%@MSJ.ORG%' THEN NULL
                              ELSE hreh.hr_employee_value_alphanumeric_text
                            END AS lawson_element_value,
                            hreh.sequence_num,
                            row_number() OVER (PARTITION BY hreh.employee_num, hreh.lawson_element_num, hreh.hr_employee_element_date ORDER BY hreh.hr_employee_element_date, hreh.sequence_num DESC) AS row_count
                          FROM
                            {{ params.param_hr_base_views_dataset_name }}.hr_employee_history AS hreh
                            INNER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_lawson_element AS rle ON hreh.lawson_element_num = rle.lawson_element_num
                          WHERE hreh.valid_to_date = DATETIME("9999-12-31 23:59:59") 
                           AND hreh.lawson_element_num = 166
                      ) AS a
                    QUALIFY email_count = 1
                ) AS b
              QUALIFY total = 1
          ) AS pemail ON er.employee_num = pemail.employee_num
        WHERE upper(er.active_dw_ind) = 'Y'
  ;