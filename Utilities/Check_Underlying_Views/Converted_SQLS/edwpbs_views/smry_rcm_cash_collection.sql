-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_rcm_cash_collection.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_rcm_cash_collection AS SELECT
    a.age_sid,
    a.rptg_period_id,
    a.month_id,
    a.payor_financial_class_sid,
    a.scenario_sid,
    a.patient_type_sid,
    a.coid,
    a.company_code,
    ROUND(a.ar_balance_amt, 3, 'ROUND_HALF_EVEN') AS ar_balance_amt,
    ROUND(a.total_insurance_amt, 3, 'ROUND_HALF_EVEN') AS total_insurance_amt,
    ROUND(a.adjust_net_rev_amt, 3, 'ROUND_HALF_EVEN') AS adjust_net_rev_amt,
    ROUND(a.cash_amt, 3, 'ROUND_HALF_EVEN') AS cash_amt,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_rcm_cash_collection AS a
    CROSS JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b
  WHERE upper(b.co_id) = upper(a.coid)
   AND upper(b.company_code) = upper(a.company_code)
   AND b.user_id = session_user()
;
