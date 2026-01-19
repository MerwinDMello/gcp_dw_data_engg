-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/gl_allocated_summary.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.gl_allocated_summary AS SELECT
    gl_allocated_summary.coid,
    gl_allocated_summary.company_code,
    gl_allocated_summary.financial_service_code,
    gl_allocated_summary.pe_date,
    gl_allocated_summary.gl_dept_num,
    gl_allocated_summary.allocation_type_id,
    gl_allocated_summary.gl_cm_actual,
    gl_allocated_summary.gl_cm_budget,
    gl_allocated_summary.gl_cm_prior_year,
    gl_allocated_summary.gl_qtd_actual,
    gl_allocated_summary.gl_qtd_budget,
    gl_allocated_summary.gl_qtd_prior_year,
    gl_allocated_summary.gl_ytd_actual,
    gl_allocated_summary.gl_ytd_budget,
    gl_allocated_summary.gl_ytd_prior_year,
    gl_allocated_summary.data_source_code,
    gl_allocated_summary.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.gl_allocated_summary
;
