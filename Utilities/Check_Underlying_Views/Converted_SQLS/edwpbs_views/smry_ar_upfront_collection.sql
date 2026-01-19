-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_ar_upfront_collection.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_ar_upfront_collection AS SELECT
    smry_ar_upfront_collection.coid,
    smry_ar_upfront_collection.month_id,
    ROUND(smry_ar_upfront_collection.upfront_amt, 3, 'ROUND_HALF_EVEN') AS upfront_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_upfront_collection
;
