-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/stats.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.stats AS SELECT
    a.company_code,
    a.coid,
    a.dept_num,
    a.stat_code,
    a.pe_date,
    ROUND(a.stat_cm_actual, 4, 'ROUND_HALF_EVEN') AS stat_cm_actual,
    ROUND(a.stat_cm_budget, 4, 'ROUND_HALF_EVEN') AS stat_cm_budget,
    ROUND(a.stat_cm_prior_year, 4, 'ROUND_HALF_EVEN') AS stat_cm_prior_year,
    ROUND(a.stat_qtd_actual, 4, 'ROUND_HALF_EVEN') AS stat_qtd_actual,
    ROUND(a.stat_qtd_budget, 4, 'ROUND_HALF_EVEN') AS stat_qtd_budget,
    ROUND(a.stat_qtd_prior_year, 4, 'ROUND_HALF_EVEN') AS stat_qtd_prior_year,
    ROUND(a.stat_ytd_actual, 4, 'ROUND_HALF_EVEN') AS stat_ytd_actual,
    ROUND(a.stat_ytd_budget, 4, 'ROUND_HALF_EVEN') AS stat_ytd_budget,
    ROUND(a.stat_ytd_prior_year, 4, 'ROUND_HALF_EVEN') AS stat_ytd_prior_year,
    a.source_system_code,
    a.data_source_code,
    a.eff_from_date,
    a.eff_to_date
  FROM
    `hca-hin-dev-cur-parallon`.edwfs_base_views.stats AS a
;
