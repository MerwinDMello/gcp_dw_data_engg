-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_sla_sub_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_sla_sub_metric AS SELECT
    lu_sla_sub_metric.corporate_code,
    lu_sla_sub_metric.metric_code,
    lu_sla_sub_metric.metric_sub_code,
    lu_sla_sub_metric.metric_sub_code_desc,
    lu_sla_sub_metric.metric_sub_code_detail,
    lu_sla_sub_metric.comments,
    lu_sla_sub_metric.source_system_code,
    lu_sla_sub_metric.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_sla_sub_metric
;
