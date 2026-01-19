-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_sla_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_sla_metric AS SELECT
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
    ROUND(lu_sla_metric.benchmark_value, 5, 'ROUND_HALF_EVEN') AS benchmark_value,
    ROUND(lu_sla_metric.incentive_target_value, 3, 'ROUND_HALF_EVEN') AS incentive_target_value,
    lu_sla_metric.value_change_date,
    lu_sla_metric.user_id,
    lu_sla_metric.current_active_ind,
    lu_sla_metric.eff_change_date,
    lu_sla_metric.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lu_sla_metric
;
