-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_sla_sub_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_sla_sub_metric AS SELECT
    lu_sla_sub_metric.corporate_code,
    lu_sla_sub_metric.metric_code,
    lu_sla_sub_metric.metric_sub_code,
    lu_sla_sub_metric.metric_sub_code_desc,
    lu_sla_sub_metric.metric_sub_code_detail,
    lu_sla_sub_metric.comments,
    lu_sla_sub_metric.metric_range_value,
    lu_sla_sub_metric.source_system_code,
    lu_sla_sub_metric.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lu_sla_sub_metric
;
