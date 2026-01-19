-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_reason_category.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_reason_category
   OPTIONS(description='Reference table to maitain the Review category of the Discrepancy')
  AS SELECT
      ref_reason_category.reason_category_id,
      ref_reason_category.reason_category_code,
      ref_reason_category.reason_category_desc,
      ref_reason_category.source_system_code,
      ref_reason_category.dw_last_update_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.ref_reason_category
  ;
