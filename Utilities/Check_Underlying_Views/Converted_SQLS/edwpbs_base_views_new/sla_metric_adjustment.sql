-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/sla_metric_adjustment.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.sla_metric_adjustment AS SELECT
    sla_metric_adjustment.month_id,
    sla_metric_adjustment.service_type_name,
    sla_metric_adjustment.fact_lvl_code,
    sla_metric_adjustment.corporate_code,
    sla_metric_adjustment.parent_code,
    sla_metric_adjustment.child_code,
    sla_metric_adjustment.metric_code,
    sla_metric_adjustment.metric_sub_code,
    sla_metric_adjustment.metric_value,
    sla_metric_adjustment.dw_last_update_date_time,
    sla_metric_adjustment.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.sla_metric_adjustment
;
