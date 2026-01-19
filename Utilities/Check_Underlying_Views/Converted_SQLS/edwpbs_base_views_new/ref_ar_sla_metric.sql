-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_ar_sla_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_ar_sla_metric AS SELECT
    ref_ar_sla_metric.metric_code,
    ref_ar_sla_metric.metric_description,
    ref_ar_sla_metric.source_system_code,
    ref_ar_sla_metric.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.ref_ar_sla_metric
;
