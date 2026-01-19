
  CREATE VIEW {{ params.param_hr_views_dataset_name }}.sup_bc2 AS SELECT DISTINCT
      scd.lawson_company_num,
      scd.supervisor_sid,
      scd.supervisor_code,
      CASE
        WHEN scd.employee_num = 0 THEN NULL
        ELSE scd.employee_num
      END AS sup_eid,
      CASE
        WHEN scd.employee_num = 0 THEN NULL
        ELSE see.employee_34_login_code
      END AS sup_34_id,
      concat(trim(cse.employee_last_name), ', ', trim(cse.employee_first_name)) AS sup_name,
      scd.supervisor_desc,
      cse.email_text,
      corp_sup.reporting_supervisor_sid AS rpt_to_5950,
      scd.reporting_supervisor_sid AS rpt_to_other,
      coalesce(corp_sup.reporting_supervisor_sid, scd.reporting_supervisor_sid) AS reporting_supervisor_sid,
      scd.valid_from_date,
      scd.valid_to_date
    FROM
      {{ params.param_hr_base_views_dataset_name }}.supervisor AS scd
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS see ON scd.employee_sid = see.employee_sid
       AND upper(see.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_person_bc AS cse ON scd.employee_sid = cse.employee_sid
       AND upper(cse.valid_to_date) = '9999-12-31'
      LEFT OUTER JOIN (
        SELECT DISTINCT
            scd_0.employee_num,
            scd_0.reporting_supervisor_sid
          FROM
            {{ params.param_hr_base_views_dataset_name }}.supervisor AS scd_0
            INNER JOIN (
              SELECT DISTINCT
                  fscd.employee_num
                FROM
                  -- 139434
                  {{ params.param_hr_base_views_dataset_name }}.supervisor AS fscd
                WHERE fscd.lawson_company_num <> 5950
                 AND fscd.employee_num <> 0
            ) AS x ON scd_0.employee_num = x.employee_num
          WHERE scd_0.lawson_company_num = 5950
           AND scd_0.employee_num <> 0
      ) AS corp_sup ON scd.employee_num = corp_sup.employee_num
  ;

