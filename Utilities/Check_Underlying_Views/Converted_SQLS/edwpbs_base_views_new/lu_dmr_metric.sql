-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
