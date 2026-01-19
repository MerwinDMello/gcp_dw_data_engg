-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_rcm_cash_collection.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_rcm_cash_collection AS SELECT
    smry_rcm_cash_collection.age_sid,
    smry_rcm_cash_collection.rptg_period_id,
    smry_rcm_cash_collection.month_id,
    smry_rcm_cash_collection.payor_financial_class_sid,
    smry_rcm_cash_collection.scenario_sid,
    smry_rcm_cash_collection.patient_type_sid,
    smry_rcm_cash_collection.coid,
    smry_rcm_cash_collection.company_code,
    ROUND(smry_rcm_cash_collection.ar_balance_amt, 3, 'ROUND_HALF_EVEN') AS ar_balance_amt,
    ROUND(smry_rcm_cash_collection.total_insurance_amt, 3, 'ROUND_HALF_EVEN') AS total_insurance_amt,
    ROUND(smry_rcm_cash_collection.adjust_net_rev_amt, 3, 'ROUND_HALF_EVEN') AS adjust_net_rev_amt,
    ROUND(smry_rcm_cash_collection.cash_amt, 3, 'ROUND_HALF_EVEN') AS cash_amt,
    smry_rcm_cash_collection.dw_last_update_date_time,
    smry_rcm_cash_collection.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.smry_rcm_cash_collection
;
