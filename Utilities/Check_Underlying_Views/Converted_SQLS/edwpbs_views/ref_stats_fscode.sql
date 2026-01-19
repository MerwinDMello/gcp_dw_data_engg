-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_stats_fscode.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_stats_fscode AS SELECT
    ref_stats_fscode.financial_service_code,
    ref_stats_fscode.financial_service_name,
    ref_stats_fscode.report_format_mapping,
    ref_stats_fscode.formula,
    ref_stats_fscode.dept_7xx_ind
  FROM
    `hca-hin-dev-cur-parallon`.edwps_base_views.ref_stats_fscode
;
