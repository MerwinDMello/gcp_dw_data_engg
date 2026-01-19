-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/stats.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.stats AS SELECT
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
    `hca-hin-dev-cur-parallon`.edwfs_base_views.stats AS a
;
