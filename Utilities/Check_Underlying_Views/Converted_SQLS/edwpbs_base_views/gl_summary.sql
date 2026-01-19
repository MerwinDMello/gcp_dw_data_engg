-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/gl_summary.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.gl_summary AS SELECT
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
    `hca-hin-dev-cur-parallon`.edwfs_base_views.gl_summary AS a
;
