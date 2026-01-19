
  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.stats AS SELECT
      a.company_code,
      a.coid,
      a.dept_num,
      a.stat_code,
      a.pe_date,
      a.stat_cm_actual,
      a.stat_cm_budget,
      a.stat_cm_prior_year,
      a.stat_qtd_actual,
      a.stat_qtd_budget,
      a.stat_qtd_prior_year,
      a.stat_ytd_actual,
      a.stat_ytd_budget,
      a.stat_ytd_prior_year,
      a.source_system_code,
      a.data_source_code,
      a.eff_from_date,
      a.eff_to_date
    FROM
      {{ params.param_hr_base_views_dataset_name }}.stats AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.secref_facility AS b ON a.company_code = b.company_code
       AND a.coid = b.co_id
       AND b.user_id = session_user()
  ;

