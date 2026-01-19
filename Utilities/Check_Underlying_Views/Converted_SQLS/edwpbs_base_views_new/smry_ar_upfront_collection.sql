-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_ar_upfront_collection.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_upfront_collection AS SELECT
    smry_ar_upfront_collection.coid,
    smry_ar_upfront_collection.month_id,
    smry_ar_upfront_collection.upfront_amt,
    smry_ar_upfront_collection.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.smry_ar_upfront_collection
;
