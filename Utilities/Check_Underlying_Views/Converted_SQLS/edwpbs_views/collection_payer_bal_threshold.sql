-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/collection_payer_bal_threshold.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.collection_payer_bal_threshold
   OPTIONS(description='Artiva system information about  Payer Balance Threshold amounts')
  AS SELECT
      a.coid,
      a.company_code,
      a.iplan_id,
      a.artiva_instance_code,
      a.unit_num,
      ROUND(a.top_threshold_amt, 0, 'ROUND_HALF_EVEN') AS top_threshold_amt,
      ROUND(a.high_threshold_amt, 0, 'ROUND_HALF_EVEN') AS high_threshold_amt,
      ROUND(a.low_threshold_amt, 0, 'ROUND_HALF_EVEN') AS low_threshold_amt,
      ROUND(a.ultra_low_threshold_amt, 0, 'ROUND_HALF_EVEN') AS ultra_low_threshold_amt,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.collection_payer_bal_threshold AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
