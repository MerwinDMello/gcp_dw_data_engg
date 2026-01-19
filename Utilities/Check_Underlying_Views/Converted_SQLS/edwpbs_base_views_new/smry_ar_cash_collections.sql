-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_ar_cash_collections.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_ar_cash_collections AS SELECT
    smry_ar_cash_collections.rptg_period,
    smry_ar_cash_collections.month_num,
    smry_ar_cash_collections.week_of_month,
    smry_ar_cash_collections.ar_transaction_enter_date,
    smry_ar_cash_collections.company_code,
    smry_ar_cash_collections.coid,
    smry_ar_cash_collections.unit_num,
    smry_ar_cash_collections.derived_patient_type_code,
    smry_ar_cash_collections.financial_class_code,
    smry_ar_cash_collections.payor_dw_id,
    smry_ar_cash_collections.parent_payor_name,
    smry_ar_cash_collections.payor_id,
    smry_ar_cash_collections.iplan_id,
    smry_ar_cash_collections.up_front_collection_ind,
    smry_ar_cash_collections.source_system_code,
    smry_ar_cash_collections.cash_amt,
    smry_ar_cash_collections.adjusted_net_revenue_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.smry_ar_cash_collections
;
