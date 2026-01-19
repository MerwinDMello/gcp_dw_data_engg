-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_payer_bal_threshold.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.collection_payer_bal_threshold AS SELECT
    collection_payer_bal_threshold.coid,
    collection_payer_bal_threshold.company_code,
    collection_payer_bal_threshold.iplan_id,
    collection_payer_bal_threshold.artiva_instance_code,
    collection_payer_bal_threshold.unit_num,
    collection_payer_bal_threshold.top_threshold_amt,
    collection_payer_bal_threshold.high_threshold_amt,
    collection_payer_bal_threshold.low_threshold_amt,
    collection_payer_bal_threshold.ultra_low_threshold_amt,
    collection_payer_bal_threshold.source_system_code,
    collection_payer_bal_threshold.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.collection_payer_bal_threshold
;
