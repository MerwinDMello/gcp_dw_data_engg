CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.gl_summary AS SELECT
    a.company_code,
    a.coid,
    a.gl_dept_num,
    a.gl_sub_account_num,
    a.source_system_code,
    a.pe_date,
    a.gl_cm_actual,
    a.gl_cm_budget,
    a.gl_cm_prior_year,
    a.gl_qtd_actual,
    a.gl_qtd_budget,
    a.gl_qtd_prior_year,
    a.gl_ytd_actual,
    a.gl_ytd_budget,
    a.gl_ytd_prior_year,
    a.data_source_code
  FROM
    {{ params.param_fs_base_views_dataset_name }}.gl_summary AS a
  WHERE upper(a.company_code) = 'H'

