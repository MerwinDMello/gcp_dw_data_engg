-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_ar_sla.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_ar_sla AS SELECT
    lu_ar_sla.company_code,
    lu_ar_sla.coid,
    lu_ar_sla.metric_code,
    lu_ar_sla.financial_class_code,
    lu_ar_sla.month_id,
    lu_ar_sla.metric_value_pct,
    lu_ar_sla.source_system_code,
    lu_ar_sla.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lu_ar_sla
;
