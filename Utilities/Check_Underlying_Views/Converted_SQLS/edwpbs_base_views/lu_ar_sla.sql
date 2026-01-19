-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_ar_sla.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_ar_sla
   OPTIONS(description='This table captures the benchmark value for each facility at financial class code, the AR Aging expected values.')
  AS SELECT
      lu_ar_sla.company_code,
      lu_ar_sla.coid,
      lu_ar_sla.metric_code,
      lu_ar_sla.financial_class_code,
      lu_ar_sla.month_id,
      ROUND(lu_ar_sla.metric_value_pct, 5, 'ROUND_HALF_EVEN') AS metric_value_pct,
      lu_ar_sla.source_system_code,
      lu_ar_sla.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.lu_ar_sla
  ;
