-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
    a.ar_balance_amt,
    a.total_insurance_amt,
    a.adjust_net_rev_amt,
    a.cash_amt,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_rcm_cash_collection AS a
    CROSS JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b
  WHERE b.co_id = a.coid
   AND b.company_code = a.company_code
   AND b.user_id = session_user()
;
