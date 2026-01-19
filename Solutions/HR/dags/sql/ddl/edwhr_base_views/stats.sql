CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.stats AS SELECT
    stats.company_code,
    stats.coid,
    stats.dept_num,
    stats.stat_code,
    stats.pe_date,
    stats.stat_cm_actual,
    stats.stat_cm_budget,
    stats.stat_cm_prior_year,
    stats.stat_qtd_actual,
    stats.stat_qtd_budget,
    stats.stat_qtd_prior_year,
    stats.stat_ytd_actual,
    stats.stat_ytd_budget,
    stats.stat_ytd_prior_year,
    stats.source_system_code,
    stats.data_source_code,
    stats.eff_from_date,
    stats.eff_to_date
  FROM
    {{ params.param_fs_base_views_dataset_name }}.stats
  WHERE upper(stats.company_code) = 'H'
;
