-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
