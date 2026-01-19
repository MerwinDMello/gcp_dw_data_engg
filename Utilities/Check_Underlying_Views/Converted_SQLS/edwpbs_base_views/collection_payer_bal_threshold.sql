-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_payer_bal_threshold.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.collection_payer_bal_threshold
   OPTIONS(description='Artiva system information about  Payer Balance Threshold amounts')
  AS SELECT
      collection_payer_bal_threshold.coid,
      collection_payer_bal_threshold.company_code,
      collection_payer_bal_threshold.iplan_id,
      collection_payer_bal_threshold.artiva_instance_code,
      collection_payer_bal_threshold.unit_num,
      ROUND(collection_payer_bal_threshold.top_threshold_amt, 0, 'ROUND_HALF_EVEN') AS top_threshold_amt,
      ROUND(collection_payer_bal_threshold.high_threshold_amt, 0, 'ROUND_HALF_EVEN') AS high_threshold_amt,
      ROUND(collection_payer_bal_threshold.low_threshold_amt, 0, 'ROUND_HALF_EVEN') AS low_threshold_amt,
      ROUND(collection_payer_bal_threshold.ultra_low_threshold_amt, 0, 'ROUND_HALF_EVEN') AS ultra_low_threshold_amt,
      collection_payer_bal_threshold.source_system_code,
      collection_payer_bal_threshold.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.collection_payer_bal_threshold
  ;
