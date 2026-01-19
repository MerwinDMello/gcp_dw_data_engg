-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_metric_category.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_metric_category AS SELECT
    lu_metric_category.metric_cat_code,
    lu_metric_category.metric_cat_desc,
    lu_metric_category.dw_last_update_date_time,
    lu_metric_category.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lu_metric_category
;
