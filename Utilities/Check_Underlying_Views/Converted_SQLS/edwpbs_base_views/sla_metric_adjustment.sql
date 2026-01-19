-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
    ROUND(sla_metric_adjustment.metric_value, 4, 'ROUND_HALF_EVEN') AS metric_value,
    sla_metric_adjustment.dw_last_update_date_time,
    sla_metric_adjustment.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.sla_metric_adjustment
;
