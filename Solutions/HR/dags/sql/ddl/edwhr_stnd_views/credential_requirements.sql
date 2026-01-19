-- Translation time: 2023-04-12T01:17:05.938700Z
-- Translation job ID: 62432306-4f81-40cb-aaa3-b1e8779e3176
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/EDWHR_STND_VIEWS/credential_requirements.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.credential_requirements AS WITH comp_sum AS (
  SELECT
      er.employee_sid,
      coalesce(jpc.weight_amt, 0) AS weight,
      sum(CASE
        WHEN ecd.employee_num IS NOT NULL
         AND CASE
          WHEN upper(coalesce(ecd.skill_source_code, 'Null')) NOT IN(
            'GRANDFATHR', 'LIFETIME'
          ) THEN CASE
            WHEN ecd.certification_renew_date = parse_date('%m/%d/%Y', '01/01/1800') THEN ecd.renew_date
            ELSE ecd.certification_renew_date
          END
          ELSE parse_date('%m/%d/%Y', '12/31/9999')
        END >= current_date() THEN 1
        ELSE 0
      END) AS compliance_sum
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee_roster AS er
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_personnel_code AS jpc ON er.position_sid = jpc.position_sid
       AND date(jpc.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_code_detail AS ecd ON er.employee_sid = ecd.employee_sid
       AND jpc.personnel_type_code = ecd.employee_type_code
       AND jpc.personnel_code = ecd.employee_code
       AND date(ecd.valid_to_date) = '9999-12-31'
    WHERE upper(er.active_dw_ind) = 'Y'
     AND jpc.personnel_type_code IN(
      'KN', 'CE'
    )
    GROUP BY 1, 2
)
SELECT
    er.group_code,
    er.group_name,
    er.division_code,
    er.division_name,
    er.market_code,
    er.market_name,
    er.lob_code,
    er.sub_lob_code,
    er.functional_dept_num,
    er.functional_dept_desc,
    er.sub_functional_dept_num,
    er.sub_functional_dept_desc,
    er.lawson_company_num,
    er.company_name,
    er.coid,
    er.coid_name,
    er.process_level_code,
    er.process_level_name,
    er.dept_num AS cost_center,
    er.dept_code,
    er.dept_name,
    er.job_class_code,
    er.job_class_desc,
    er.job_code,
    er.job_code_desc,
    er.position_code,
    er.position_code_desc,
    er.union_code,
    er.union_desc,
    concat(trim(er.employee_last_name), ', ', trim(er.employee_first_name), ' ', trim(er.employee_middle_initial_text)) AS employee_name,
    er.employee_num,
    er.employee_34_login_code,
    er.auxiliary_status_code,
    er.employee_status_code,
    er.employee_status_desc,
    er.hire_date,
    er.supervisor_code,
    er.supervisor_name,
    er.supervisor_employee_id,
    er.supervisor_34_id,
    er.supervisor_position,
    er.supervisor_email_text,
    jpc.personnel_type_code,
    jpc.personnel_code,
    hrc_cred.hr_code_desc AS personnel_code_desc,
    jpc.required_flag_ind,
    jpc.weight_amt,
    CASE
      WHEN ecd.employee_type_code IS NULL THEN 'Missing Credential'
      ELSE 'Fulfilled'
    END AS credential_status,
    CASE
      WHEN ecd.employee_num IS NOT NULL
       AND CASE
        WHEN upper(coalesce(ecd.skill_source_code, 'Null')) NOT IN(
          'GRANDFATHR', 'LIFETIME'
        ) THEN CASE
          WHEN ecd.certification_renew_date = parse_date('%m/%d/%Y', '01/01/1800') THEN ecd.renew_date
          ELSE ecd.certification_renew_date
        END
        ELSE parse_date('%m/%d/%Y', '12/31/9999')
      END >= current_date() THEN 'In Compliance'
      ELSE 'Out of Compliance'
    END AS compliance_status,
    ecd.license_num_text,
    CASE
      WHEN date(ecd.acquired_date) = '1800-01-01' THEN NULL
      ELSE ecd.acquired_date
    END AS acquired_date,
    CASE
      WHEN upper(ecd.employee_type_code) = 'ED' THEN NULL
      WHEN date(ecd.certification_renew_date) = '1800-01-01'
       AND date(ecd.renew_date) = '1800-01-01' THEN NULL
      WHEN date(ecd.certification_renew_date) = '1800-01-01' THEN ecd.renew_date
      ELSE ecd.certification_renew_date
    END AS expiration_date,
    CASE
      WHEN ceil(date_diff(CASE
        WHEN upper(ecd.employee_type_code) = 'ED' THEN NULL
        WHEN date(ecd.certification_renew_date) = '1800-01-01'
         AND date(ecd.renew_date) = '1800-01-01' THEN NULL
        WHEN date(ecd.certification_renew_date) = '1800-01-01' THEN ecd.renew_date
        ELSE ecd.certification_renew_date
      END , current_date('US/Central'),day)) BETWEEN 0 AND 90 THEN ceil(date_diff(CASE
        WHEN upper(ecd.employee_type_code) = 'ED' THEN NULL
        WHEN date(ecd.certification_renew_date) = '1800-01-01'
         AND date(ecd.renew_date) = '1800-01-01' THEN NULL
        WHEN date(ecd.certification_renew_date) = '1800-01-01' THEN ecd.renew_date
        ELSE ecd.certification_renew_date
      END , current_date(),day))
      ELSE CAST(NULL as BIGNUMERIC)
    END AS days_until_expired,
    CASE
      WHEN CASE
        WHEN ceil(date_diff(CASE
          WHEN upper(ecd.employee_type_code) = 'ED' THEN NULL
          WHEN date(ecd.certification_renew_date) = '1800-01-01'
           AND date(ecd.renew_date) = '1800-01-01' THEN NULL
          WHEN date(ecd.certification_renew_date) = '1800-01-01' THEN ecd.renew_date
          ELSE ecd.certification_renew_date
        END , current_date('US/Central'),day)) < 0 THEN CAST(NULL as BIGNUMERIC)
        ELSE ceil(date_diff(CASE
          WHEN upper(ecd.employee_type_code) = 'ED' THEN NULL
          WHEN date(ecd.certification_renew_date) = '1800-01-01'
           AND date(ecd.renew_date) = '1800-01-01' THEN NULL
          WHEN date(ecd.certification_renew_date) = '1800-01-01' THEN ecd.renew_date
          ELSE ecd.certification_renew_date
        END , current_date('US/Central'),day))
      END <= 30 THEN '30 Days or Less'
      WHEN CASE
        WHEN ceil(date_diff(CASE
          WHEN upper(ecd.employee_type_code) = 'ED' THEN NULL
          WHEN date(ecd.certification_renew_date) = '1800-01-01'
           AND date(ecd.renew_date) = '1800-01-01' THEN NULL
          WHEN date(ecd.certification_renew_date) = '1800-01-01' THEN ecd.renew_date
          ELSE ecd.certification_renew_date
        END , current_date('US/Central'),day)) < 0 THEN CAST(NULL as BIGNUMERIC)
        ELSE ceil(date_diff(CASE
          WHEN upper(ecd.employee_type_code) = 'ED' THEN NULL
          WHEN date(ecd.certification_renew_date) = '1800-01-01'
           AND date(ecd.renew_date) = '1800-01-01' THEN NULL
          WHEN date(ecd.certification_renew_date) = '1800-01-01' THEN ecd.renew_date
          ELSE ecd.certification_renew_date
        END , current_date('US/Central'),day))
      END BETWEEN 31 AND 60 THEN '31 to 60 Days'
      WHEN CASE
        WHEN ceil(date_diff(CASE
          WHEN upper(ecd.employee_type_code) = 'ED' THEN NULL
          WHEN date(ecd.certification_renew_date) = '1800-01-01'
           AND date(ecd.renew_date) = '1800-01-01' THEN NULL
          WHEN date(ecd.certification_renew_date) = '1800-01-01' THEN ecd.renew_date
          ELSE ecd.certification_renew_date
        END ,current_date('US/Central'),day)) < 0 THEN CAST(NULL as BIGNUMERIC)
        ELSE ceil(date_diff(CASE
          WHEN upper(ecd.employee_type_code) = 'ED' THEN NULL
          WHEN date(ecd.certification_renew_date) = '1800-01-01'
           AND date(ecd.renew_date) = '1800-01-01' THEN NULL
          WHEN date(ecd.certification_renew_date) = '1800-01-01' THEN ecd.renew_date
          ELSE ecd.certification_renew_date
        END , current_date('US/Central'),day))
      END BETWEEN 61 AND 90 THEN '61 to 90 Days'
      ELSE CAST(NULL as STRING)
    END AS expiring_in,
    CASE
      WHEN CASE
        WHEN upper(ecd.employee_type_code) = 'ED' THEN NULL
        WHEN date(ecd.certification_renew_date) = '1800-01-01'
         AND date(ecd.renew_date) = '1800-01-01' THEN NULL
        WHEN date(ecd.certification_renew_date) = '1800-01-01' THEN ecd.renew_date
        ELSE ecd.certification_renew_date
      END IS NULL
       AND ecd.employee_type_code IS NOT NULL THEN 'No Date in Lawson'
      WHEN CAST(CASE
        WHEN upper(ecd.employee_type_code) = 'ED' THEN NULL
        WHEN date(ecd.certification_renew_date) = '1800-01-01'
         AND date(ecd.renew_date) = '1800-01-01' THEN NULL
        WHEN date(ecd.certification_renew_date) = '1800-01-01' THEN ecd.renew_date
        ELSE ecd.certification_renew_date
      END as DATE) < current_date() THEN 'Expired'
      ELSE CAST(NULL as STRING)
    END AS expired_status,
    CASE
      WHEN floor(date_diff(current_date('US/Central') , CASE
        WHEN upper(ecd.employee_type_code) = 'ED' THEN NULL
        WHEN date(ecd.certification_renew_date) = '1800-01-01'
         AND date(ecd.renew_date) = '1800-01-01' THEN NULL
        WHEN date(ecd.certification_renew_date) = '1800-01-01' THEN ecd.renew_date
        ELSE ecd.certification_renew_date
      END,day)) < 0 THEN CAST(NULL as BIGNUMERIC)
      ELSE floor(date_diff(current_date('US/Central') , CASE
        WHEN upper(ecd.employee_type_code) = 'ED' THEN NULL
        WHEN date(ecd.certification_renew_date) = '1800-01-01'
         AND date(ecd.renew_date) = '1800-01-01' THEN NULL
        WHEN date(ecd.certification_renew_date) = '1800-01-01' THEN ecd.renew_date
        ELSE ecd.certification_renew_date
      END,day))
    END AS days_past_due,
    ecd.skill_source_code,
    hrc.hr_code_desc AS skill_source_code_desc,
    ecd.state_code,
    CASE
      WHEN upper(ecd.state_code) = 'US' THEN 'National Credential'
      ELSE CAST(NULL as STRING)
    END AS national_cert,
    ecd.company_sponsored_ind,
    -- Stop Email Notification aka Company Sponsored Flag--
    ecd.verified_ind
  FROM
    -- No Longer in Force aka Verified Flag--
    {{ params.param_hr_base_views_dataset_name }}.employee_roster AS er
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.job_personnel_code AS jpc ON er.position_sid = jpc.position_sid
     AND date(jpc.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_code_detail AS ecd ON jpc.personnel_type_code = ecd.employee_type_code
     AND jpc.personnel_code = ecd.employee_code
     AND er.employee_sid = ecd.employee_sid
     AND ecd.employee_type_code IN(
      'KN', 'CE'
    )
     AND ecd.employee_sw = 0
     AND upper(ecd.active_dw_ind) = 'Y'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS hrc ON trim(ecd.skill_source_code) = trim(hrc.hr_code)
     AND upper(hrc.hr_type_code) = 'SS'
     AND upper(hrc.active_ind) = 'A'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_hr_code AS hrc_cred ON jpc.personnel_code = hrc_cred.hr_code
     AND jpc.personnel_type_code = hrc_cred.hr_type_code
     AND upper(hrc_cred.active_ind) = 'A'
    INNER JOIN comp_sum ON er.employee_sid = comp_sum.employee_sid
     AND jpc.weight_amt = comp_sum.weight
    INNER JOIN -- Showing whether or not a weight has any compliance at all
    {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON er.process_level_code = c.process_level_code
     AND er.lawson_company_num = c.lawson_company_num
     AND c.user_id = session_user()
  WHERE upper(er.active_dw_ind) = 'Y'
   AND jpc.personnel_type_code IN(
    'KN', 'CE'
  )
   AND CASE
    WHEN coalesce(jpc.weight_amt, 0) = 0 THEN 1
    WHEN coalesce(jpc.weight_amt, 0) <> 0
     AND comp_sum.compliance_sum = 0 THEN 1
    WHEN coalesce(jpc.weight_amt, 0) <> 0
     AND comp_sum.compliance_sum > 0
     AND CASE
      WHEN ecd.employee_num IS NOT NULL
       AND CASE
        WHEN upper(coalesce(ecd.skill_source_code, 'Null')) NOT IN(
          -- The following case takes care of all three scenarios (see comments for details) to avoid the need to write three separate queries
          -- All 0 weight class rows (all rows should show)
          -- All weight classes with no compliance at all (all rows should show)
          'GRANDFATHR', 'LIFETIME'
        ) THEN CASE
          WHEN ecd.certification_renew_date = parse_date('%m/%d/%Y', '01/01/1800') THEN ecd.renew_date
          ELSE ecd.certification_renew_date
        END
        ELSE parse_date('%m/%d/%Y', '12/31/9999')
      END >= current_date() THEN 1
      ELSE 0
    END = 1 THEN 1
    ELSE 0
  END = 1
;
-- Non-zero weight classes with compliance (only in compliance rows should show)
