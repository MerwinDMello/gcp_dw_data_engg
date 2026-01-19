create or replace view {{ params.param_hr_bi_views_dataset_name }}.dimemployee as  SELECT DISTINCT
    emppers.employee_sid,
    emppers.employee_num,
    emp.employee_34_login_code,
    emppers.employee_last_name,
    emppers.employee_first_name,
    emppers.ethnic_origin_code,
    CASE
      WHEN upper(emppers.ethnic_origin_code) = 'A' THEN 'Asian'
      WHEN upper(emppers.ethnic_origin_code) = 'B' THEN 'Black'
      WHEN upper(emppers.ethnic_origin_code) = 'H' THEN 'Hispanic'
      WHEN upper(emppers.ethnic_origin_code) = 'I' THEN 'American Indian or Native Alaskan'
      WHEN upper(emppers.ethnic_origin_code) = 'P' THEN 'Pacific Islander or Native Hawaiian'
      WHEN upper(emppers.ethnic_origin_code) = 'T' THEN 'Two or more Races'
      WHEN upper(emppers.ethnic_origin_code) = 'W' THEN 'White'
      ELSE 'Other'
    END AS ethnicity,
    emppers.gender_code,
    CASE
      WHEN upper(emppers.gender_code) = 'F' THEN 'Female'
      WHEN upper(emppers.gender_code) = 'M' THEN 'Male'
      ELSE CAST(NULL as STRING)
    END AS gender,
    emp.union_code,
    CASE
      WHEN emp.remote_sw = 0 THEN 'Non WFH'
      WHEN emp.remote_sw = 1 THEN 'WFH'
      ELSE CAST(NULL as STRING)
    END AS wfh_flag,
    emppers.veteran_ind,
    coalesce(mult_prog.mult_prog_flag, prog.prog_flag, 'No Program') AS gn_flag,
    nursing_school.nursing_school_name,
    rnexp_hire.rn_xp_at_hire,
    rnexp_hire.rn_xp_bucket_at_hire,
    rnexp_hire.rn_xp_at_hire AS rn_xp_at_first_hire,
    rnexp_hire.rn_xp_bucket_at_hire AS rn_xp_bucket_at_first_hire,
    emppers.birth_date,
    CASE
      WHEN extract(YEAR from CAST(emppers.birth_date as DATE)) BETWEEN NUMERIC '1909' AND NUMERIC '1945' THEN 'Traditionalists'
      WHEN extract(YEAR from CAST(emppers.birth_date as DATE)) BETWEEN NUMERIC '1946' AND NUMERIC '1964' THEN 'Baby Boomers'
      WHEN extract(YEAR from CAST(emppers.birth_date as DATE)) BETWEEN NUMERIC '1965' AND NUMERIC '1976' THEN 'Gen X'
      WHEN extract(YEAR from CAST(emppers.birth_date as DATE)) BETWEEN NUMERIC '1977' AND NUMERIC '1995' THEN 'Gen Y'
      WHEN extract(YEAR from CAST(emppers.birth_date as DATE)) >= 1996.0 THEN 'Gen Z'
      ELSE CAST(NULL as STRING)
    END AS generation_desc
  FROM
    -- 5/19/2022 Added by Stephen for DEI Dashboard
    {{ params.param_hr_base_views_dataset_name }}.employee_person AS emppers
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS emp ON emppers.employee_sid = emp.employee_sid
     AND DATE(emp.valid_to_date) = DATE('9999-12-31')
    LEFT OUTER JOIN --  employee_sid list that have more than one program
    (
      SELECT
          employee_code_detail.employee_sid,
          'Multiple Programs' AS mult_prog_flag
        FROM
          {{ params.param_hr_base_views_dataset_name }}.employee_code_detail
        WHERE employee_code_detail.employee_code IN(
          'O-RNSTARN', 'O-RNFACPRG', 'O-RNNOPROG', 'O-RNDIVPRG'
        )
         AND DATE(employee_code_detail.valid_to_date) = DATE('9999-12-31')
        GROUP BY 1
        HAVING count(*) > 1
    ) AS mult_prog ON mult_prog.employee_sid = emppers.employee_sid
    LEFT OUTER JOIN --  all employees with programs we are interested in
    (
      SELECT DISTINCT
          employee_code_detail.employee_sid,
          employee_code_detail.employee_code AS prog_flag
        FROM
          {{ params.param_hr_base_views_dataset_name }}.employee_code_detail
        WHERE employee_code_detail.employee_code IN(
          'O-RNSTARN', 'O-RNFACPRG', 'O-RNNOPROG', 'O-RNDIVPRG'
        )
         AND DATE(employee_code_detail.valid_to_date) = DATE('9999-12-31')
    ) AS prog ON prog.employee_sid = emppers.employee_sid
     AND mult_prog.employee_sid IS NULL
    LEFT OUTER JOIN --  for each employee get their RN experience at time of hire for their earliest RN hire record
    (
      SELECT
          hrmet.employee_sid,
          date_diff(hrmet.date_id, empdet.acute_experience_start_date, day) AS rn_xp_at_hire,
          CASE
            WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, day) <= 150 THEN 'New Grad'
            WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, day) <= 365 THEN 'Less than 1 Year'
            WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, day) <= 731 THEN '1-2 Years'
            WHEN date_diff(hrmet.date_id, empdet.acute_experience_start_date, day) > 731 THEN '2+ Years'
            ELSE 'Unknown'
          END AS rn_xp_bucket_at_hire
        FROM
          {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot AS hrmet
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.bi_employee_detail AS empdet ON empdet.employee_sid = hrmet.employee_sid
          INNER JOIN {{ params.param_hr_base_views_dataset_name }}.job_class AS jobcl ON hrmet.job_class_sid = jobcl.job_class_sid
           AND DATE(jobcl.valid_to_date) = DATE('9999-12-31')
        WHERE hrmet.analytics_msr_sid = 80200
         AND hrmet.action_code IN(
          --  hires
          '1HIREAPPL', '1REHIRE', '1XFERPOS', '1XFEREIN-S', '1XFERINT'
        )
         AND upper(jobcl.job_class_code) = '103'
        QUALIFY row_number() OVER (PARTITION BY hrmet.employee_sid ORDER BY hrmet.date_id) = 1
    ) AS rnexp_hire ON emppers.employee_sid = rnexp_hire.employee_sid
    LEFT OUTER JOIN -- Returns employees that went through Nursing School Program (Galen is only option as of 2.15.22)
    (
      SELECT DISTINCT
          ns.employee_num,
          rns.nursing_school_name
        FROM
          {{ params.param_hr_base_views_dataset_name }}.nursing_student AS ns
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.student_program_graduation AS spg ON ns.student_sid = spg.student_sid
           AND DATE(spg.valid_to_date) = DATE('9999-12-31')
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_school_campus AS rnsc ON spg.campus_id = rnsc.campus_id
          LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_nursing_school AS rns ON rnsc.nursing_school_id = rns.nursing_school_id
        WHERE DATE(ns.valid_to_date) = DATE('9999-12-31')
         AND ns.employee_num IS NOT NULL
    ) AS nursing_school ON emppers.employee_num = nursing_school.employee_num
  WHERE DATE(emppers.valid_to_date) = DATE('9999-12-31')
   AND emppers.employee_sid IN(
    SELECT DISTINCT
        hrmet.employee_sid
      FROM
        {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric AS hrmet
  )
