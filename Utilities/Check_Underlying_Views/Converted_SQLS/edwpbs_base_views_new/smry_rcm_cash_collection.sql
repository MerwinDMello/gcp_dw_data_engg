-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
    smry_rcm_cash_collection.ar_balance_amt,
    smry_rcm_cash_collection.total_insurance_amt,
    smry_rcm_cash_collection.adjust_net_rev_amt,
    smry_rcm_cash_collection.cash_amt,
    smry_rcm_cash_collection.dw_last_update_date_time,
    smry_rcm_cash_collection.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.smry_rcm_cash_collection
;
