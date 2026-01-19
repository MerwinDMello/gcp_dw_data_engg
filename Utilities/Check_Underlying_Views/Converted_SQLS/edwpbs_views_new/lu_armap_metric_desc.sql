-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_armap_metric_desc.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_armap_metric_desc AS SELECT
    lu_armap_metric_desc.metric_code,
    lu_armap_metric_desc.metric_desc,
    lu_armap_metric_desc.dw_last_update_date_time,
    lu_armap_metric_desc.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_armap_metric_desc
;
