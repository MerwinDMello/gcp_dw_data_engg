-- Translation time: 2023-04-25T18:33:50.275152Z
-- Translation job ID: 8b921d45-66b0-461e-880c-7106e48c9005
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/SARANYADEVI/missing_views/EDWHR_BI_Views/dimrecruiter.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.dimrecruiter AS SELECT
    er.date_id,
    er.coid,
    er.group_code,
    er.group_name,
    er.division_code,
    er.division_name,
    er.market_code,
    er.market_name,
    er.lob_code,
    er.lob_name,
    er.sub_lob_code,
    er.sub_lob_name,
    er.lawson_company_num AS hr_company,
    er.process_level_code,
    er.process_level_name,
    er.location_code,
    er.location_desc,
    er.dept_num AS cost_center,
    er.dept_code,
    er.dept_name,
    er.employee_sid,
    er.employee_num,
    ru.employee_34_login_code,
    ru.recruitment_user_sid,
    ru.recruitment_user_num,
    ru.last_name AS recruiter_last_name,
    ru.first_name AS recruiter_first_name,
    concat(trim(ru.last_name), ', ', trim(ru.first_name)) AS recruiter_full_name,
    ru.source_system_code,
    er.job_class_code,
    er.job_class_desc,
    er.job_code,
    er.job_code_desc,
    er.position_code,
    er.position_code_desc,
    er.fte_percent,
    er.remote_flag,
    er.pay_grade_code,
    er.supervisor_sid,
    er.supervisor_code,
    er.supervisor_name,
    er.supervisor_employee_id,
    er.supervisor_34_id,
    er.supervisor_position,
    p.pfte_value_num AS pfte
  FROM
    {{ params.param_hr_base_views_dataset_name }}.employee_roster AS er
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.junc_employee_recruitment_user AS jeru ON er.employee_sid = jeru.employee_sid
     AND date(jeru.valid_to_date) = '9999-12-31'
    INNER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS ru ON jeru.recruitment_user_sid = ru.recruitment_user_sid
     AND date(ru.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.ref_position_pfte AS p ON er.position_code_desc = p.position_code_desc
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45
;
