-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/gl_summary_cm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.gl_summary_cm AS SELECT
    gl_summary_cm.coid,
    gl_summary_cm.company_code,
    gl_summary_cm.gl_cm_actual,
    gl_summary_cm.gl_cm_budget,
    gl_summary_cm.gl_cm_prior_year,
    gl_summary_cm.gl_dept_num,
    gl_summary_cm.gl_qtd_actual,
    gl_summary_cm.gl_qtd_budget,
    gl_summary_cm.gl_qtd_prior_year,
    gl_summary_cm.gl_sub_account_num,
    gl_summary_cm.gl_ytd_actual,
    gl_summary_cm.gl_ytd_budget,
    gl_summary_cm.gl_ytd_prior_year,
    gl_summary_cm.pe_date,
    gl_summary_cm.source_system_code,
    gl_summary_cm.data_source_code
  FROM
    `hca-hin-dev-cur-parallon`.edwfs_base_views.gl_summary_cm
;
