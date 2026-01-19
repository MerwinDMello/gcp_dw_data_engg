-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_sla_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_sla_metric AS SELECT
    lu_sla_metric.start_date,
    lu_sla_metric.end_date,
    lu_sla_metric.month_id,
    lu_sla_metric.coid,
    lu_sla_metric.corporate_code,
    lu_sla_metric.metric_code,
    lu_sla_metric.metric_type_code,
    lu_sla_metric.type_change_date,
    lu_sla_metric.status_flag,
    lu_sla_metric.status_change_date,
    lu_sla_metric.benchmark_value,
    lu_sla_metric.incentive_target_value,
    lu_sla_metric.value_change_date,
    lu_sla_metric.user_id,
    lu_sla_metric.eff_change_date,
    lu_sla_metric.current_active_ind,
    lu_sla_metric.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_sla_metric
;
