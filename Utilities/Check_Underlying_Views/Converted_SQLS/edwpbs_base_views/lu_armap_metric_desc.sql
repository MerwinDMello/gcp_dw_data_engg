-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_armap_metric_desc.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_armap_metric_desc AS SELECT
    lu_armap_metric_desc.metric_code,
    lu_armap_metric_desc.metric_desc,
    lu_armap_metric_desc.dw_last_update_date_time,
    lu_armap_metric_desc.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lu_armap_metric_desc
;
