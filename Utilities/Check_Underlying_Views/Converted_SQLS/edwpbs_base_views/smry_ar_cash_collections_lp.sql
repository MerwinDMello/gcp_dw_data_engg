-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_ar_cash_collections_lp.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_cash_collections_lp AS SELECT
    smry_ar_cash_collections_lp.rptg_period,
    smry_ar_cash_collections_lp.month_num,
    smry_ar_cash_collections_lp.week_of_month,
    smry_ar_cash_collections_lp.ar_transaction_enter_date,
    smry_ar_cash_collections_lp.company_code,
    smry_ar_cash_collections_lp.coid,
    smry_ar_cash_collections_lp.unit_num,
    smry_ar_cash_collections_lp.derived_patient_type_code,
    ROUND(smry_ar_cash_collections_lp.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
    ROUND(smry_ar_cash_collections_lp.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    smry_ar_cash_collections_lp.parent_payor_name,
    smry_ar_cash_collections_lp.payor_id,
    smry_ar_cash_collections_lp.iplan_id,
    smry_ar_cash_collections_lp.source_system_code,
    ROUND(smry_ar_cash_collections_lp.cash_amt, 3, 'ROUND_HALF_EVEN') AS cash_amt,
    ROUND(smry_ar_cash_collections_lp.adjusted_net_revenue_amt, 3, 'ROUND_HALF_EVEN') AS adjusted_net_revenue_amt,
    smry_ar_cash_collections_lp.up_front_collection_ind
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.smry_ar_cash_collections_lp
;
