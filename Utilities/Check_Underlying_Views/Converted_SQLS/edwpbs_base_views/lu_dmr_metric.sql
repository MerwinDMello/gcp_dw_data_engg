-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_dmr_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_dmr_metric AS SELECT
    lu_dmr_metric.dmr_metric_code,
    lu_dmr_metric.dmr_metric_desc,
    lu_dmr_metric.dmr_metric_short_desc,
    lu_dmr_metric.dmr_metric_uom,
    lu_dmr_metric.dmr_metric_help_txt,
    lu_dmr_metric.dw_last_update_date_time,
    lu_dmr_metric.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lu_dmr_metric
;
