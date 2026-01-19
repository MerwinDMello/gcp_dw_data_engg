
  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.span_code_override AS WITH userdata AS (
    SELECT
        /*WITH DATA PRIMARY INDEX (Hca_Span_Code, Span_Override, Process_Level_Code)
        ON COMMIT PRESERVE ROWS;*/
        zv.employee_34_login_code,
        zn.access_role_code,
        zn.span_code,
        substr(zn.span_code, 1, 2) AS span_override,
        zn.view_only_span_code,
        substr(zn.view_only_span_code, 1, 2) AS inq_span_override,
        'ZS32' AS access_origin
      FROM
        {{ params.param_hr_base_views_dataset_name }}.employee_position_security_role AS zv
        INNER JOIN {{ params.param_hr_base_views_dataset_name }}.security_role_detail AS zn ON zv.lawson_company_num = zn.lawson_company_num
         AND TRIM(UPPER(zv.process_level_code)) = TRIM(UPPER(zn.process_level_code))
         AND TRIM(UPPER(zv.span_code)) = TRIM(UPPER(zn.span_code))
         AND TRIM(UPPER(zv.access_role_code)) = TRIM(UPPER(zn.access_role_code))
         AND TRIM(UPPER(zv.dept_code)) = TRIM(UPPER(zn.dept_code))
    UNION DISTINCT
    SELECT
        esr.employee_34_login_code,
        esr.access_role_code,
        esr.span_code,
        substr(esr.span_code, 1, 2) AS span_override,
        ' ' AS view_only_span_code,
        ' ' AS inq_span_override,
        'ZS37' AS access_origin
      FROM
        {{ params.param_hr_base_views_dataset_name }}.employee_security_role AS esr
      WHERE TRIM(upper(esr.active_ind)) = 'A'
       AND TRIM(UPPER(esr.system_code)) IN(
        'HR', 'PR'
      )
  ), overrideaccess AS (
    SELECT
        'AZ001' AS hca_span_code,
        'AZ' AS span_override,
        pr.*
      FROM
        (
          SELECT DISTINCT
              gl_lawson_dept_crosswalk.valid_to_date,
              gl_lawson_dept_crosswalk.coid,
              gl_lawson_dept_crosswalk.process_level_code,
              gl_lawson_dept_crosswalk.lawson_company_num,
              gl_lawson_dept_crosswalk.company_code,
              gl_lawson_dept_crosswalk.source_system_code
            FROM
              {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk
            WHERE date(gl_lawson_dept_crosswalk.valid_to_date) = '9999-12-31'
             AND gl_lawson_dept_crosswalk.lawson_company_num <> 0
        ) AS pr
      WHERE TRIM(upper(pr.process_level_code)) <> ' '
       AND date(pr.valid_to_date) = '9999-12-31'
    UNION DISTINCT
    SELECT
        'CZ001' AS hca_span_code,
        'CZ' AS span_override,
        pr.*
      FROM
        (
          SELECT DISTINCT
              gl_lawson_dept_crosswalk.valid_to_date,
              gl_lawson_dept_crosswalk.coid,
              gl_lawson_dept_crosswalk.process_level_code,
              gl_lawson_dept_crosswalk.lawson_company_num,
              gl_lawson_dept_crosswalk.company_code,
              gl_lawson_dept_crosswalk.source_system_code
            FROM
              {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk
            WHERE date(gl_lawson_dept_crosswalk.valid_to_date) = '9999-12-31'
             AND gl_lawson_dept_crosswalk.lawson_company_num <> 0
        ) AS pr
      WHERE pr.lawson_company_num IN(
        5950
      )
       AND TRIM(upper(pr.process_level_code)) <> ' '
       AND date(pr.valid_to_date) = '9999-12-31'
    UNION DISTINCT
    SELECT
        'FZ001' AS hca_span_code,
        'FZ' AS span_override,
        pr.*
      FROM
        (
          SELECT DISTINCT
              gl_lawson_dept_crosswalk.valid_to_date,
              gl_lawson_dept_crosswalk.coid,
              gl_lawson_dept_crosswalk.process_level_code,
              gl_lawson_dept_crosswalk.lawson_company_num,
              gl_lawson_dept_crosswalk.company_code,
              gl_lawson_dept_crosswalk.source_system_code
            FROM
              {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk
            WHERE date(gl_lawson_dept_crosswalk.valid_to_date) = '9999-12-31'
             AND gl_lawson_dept_crosswalk.lawson_company_num <> 0
        ) AS pr
      WHERE pr.lawson_company_num NOT IN(
        5950
      )
       AND TRIM(upper(pr.process_level_code)) <> ' '
       AND date(pr.valid_to_date) = '9999-12-31'
    UNION DISTINCT
    SELECT
        'HZ001' AS hca_span_code,
        'HZ' AS span_override,
        pr.*
      FROM
        (
          SELECT DISTINCT
              gl_lawson_dept_crosswalk.valid_to_date,
              gl_lawson_dept_crosswalk.coid,
              gl_lawson_dept_crosswalk.process_level_code,
              gl_lawson_dept_crosswalk.lawson_company_num,
              gl_lawson_dept_crosswalk.company_code,
              gl_lawson_dept_crosswalk.source_system_code
            FROM
              {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk
            WHERE date(gl_lawson_dept_crosswalk.valid_to_date) = '9999-12-31'
             AND gl_lawson_dept_crosswalk.lawson_company_num <> 0
        ) AS pr
      WHERE pr.lawson_company_num NOT IN(
        5902, 5903, 5907, 5910, 5920, 5950, 5959
      )
       AND TRIM(upper(pr.process_level_code)) <> ' '
       AND date(pr.valid_to_date) = '9999-12-31'
    UNION DISTINCT
    SELECT
        'RZ001' AS hca_span_code,
        'RZ' AS span_override,
        pr.*
      FROM
        (
          SELECT DISTINCT
              gl_lawson_dept_crosswalk.valid_to_date,
              gl_lawson_dept_crosswalk.coid,
              gl_lawson_dept_crosswalk.process_level_code,
              gl_lawson_dept_crosswalk.lawson_company_num,
              gl_lawson_dept_crosswalk.company_code,
              gl_lawson_dept_crosswalk.source_system_code
            FROM
              {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk
            WHERE date(gl_lawson_dept_crosswalk.valid_to_date) = '9999-12-31'
             AND gl_lawson_dept_crosswalk.lawson_company_num <> 0
        ) AS pr
      WHERE pr.lawson_company_num NOT IN(
        5902, 5903, 5907, 5910, 5950, 5959
      )
       AND TRIM(upper(pr.process_level_code)) <> ' '
       AND date(pr.valid_to_date) = '9999-12-31'
  )
  SELECT DISTINCT
      /*WITH DATA PRIMARY INDEX (Employee_34_Login_Code, Access_Role_Code, Span_Code)
      ON COMMIT PRESERVE ROWS;*/
      a.lawson_company_num,
      trim(a.employee_34_login_code) AS employee_34_login_code,
      trim(a.process_level_code) AS process_level_code,
      trim(a.coid) AS coid
    FROM
      -- Correlate update override access to user
      (
        SELECT
            usr.employee_34_login_code,
            usr.access_role_code,
            usr.span_code,
            ora.coid,
            ora.lawson_company_num,
            ora.process_level_code
          FROM
            userdata AS usr
            INNER JOIN overrideaccess AS ora ON TRIM(UPPER(usr.span_override)) = TRIM(UPPER(ora.span_override))
          WHERE TRIM(upper(usr.access_role_code)) <> 'PRMOBIUS'
        UNION DISTINCT
        SELECT
            -- Correlate update span code access to user
            usr.employee_34_login_code,
            usr.access_role_code,
            usr.span_code,
            zs51.coid,
            zs51.lawson_company_num,
            zs51.process_level_code
          FROM
            userdata AS usr
            INNER JOIN {{ params.param_hr_base_views_dataset_name }}.span_code_detail AS zs51 ON TRIM(UPPER(usr.span_code)) = TRIM(UPPER(zs51.span_code))
             AND TRIM(UPPER(zs51.system_code)) NOT IN(
              'AP', 'GL', 'LP', 'PO'
            )
             AND TRIM(UPPER(zs51.system_code)) IN(
              ' ', 'HR', 'PR'
            )
          WHERE TRIM(upper(usr.access_role_code)) <> 'PRMOBIUS'
        UNION DISTINCT
        SELECT
            -- Correlate inquire only override access to user
            usr.employee_34_login_code,
            usr.access_role_code,
            usr.view_only_span_code AS span_code,
            osr.coid,
            osr.lawson_company_num,
            osr.process_level_code
          FROM
            userdata AS usr
            INNER JOIN overrideaccess AS osr ON TRIM(UPPER(usr.inq_span_override)) = TRIM(UPPER(osr.span_override))
          WHERE TRIM(upper(usr.access_role_code)) <> 'PRMOBIUS'
        UNION DISTINCT
        SELECT
            -- Correlate inquiry span code access to user
            usr.employee_34_login_code,
            usr.access_role_code,
            usr.view_only_span_code AS span_code,
            zs51.coid,
            zs51.lawson_company_num,
            zs51.process_level_code
          FROM
            userdata AS usr
            INNER JOIN {{ params.param_hr_base_views_dataset_name }}.span_code_detail AS zs51 ON TRIM(UPPER(usr.view_only_span_code)) = TRIM(UPPER(zs51.span_code))
             AND TRIM(UPPER(zs51.system_code)) NOT IN(
              'AP', 'GL', 'LP', 'PO'
            )
             AND TRIM(UPPER(zs51.system_code)) IN(
              ' ', 'HR', 'PR'
            )
          WHERE trim(upper(usr.access_role_code)) <> 'PRMOBIUS'
      ) AS a
  ;

