-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_ar_sla_metric.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_ar_sla_metric
   OPTIONS(description='This is description table for Account Receivable Measurements')
  AS SELECT
      a.metric_code,
      a.metric_description,
      CASE
        WHEN upper(a.metric_description) LIKE 'AR%' THEN 'HCA Goal'
        WHEN upper(a.metric_description) LIKE 'SLA%' THEN 'SLA Goal'
      END AS metric_group,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_ar_sla_metric AS a
  ;
