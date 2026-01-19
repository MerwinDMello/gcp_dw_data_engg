-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_up_front_collection.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_up_front_collection AS SELECT
    a.unit_num_sid,
    a.patient_type_sid,
    a.patient_financial_class_sid,
    a.payor_sid,
    a.account_status_sid,
    a.time_id,
    a.up_front_msr_sid,
    a.coid,
    a.pe_date,
    a.company_code,
    ROUND(a.transaction_amt, 3, 'ROUND_HALF_EVEN') AS transaction_amt,
    ROUND(a.patient_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS patient_total_charge_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_up_front_collection AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b ON upper(a.coid) = upper(b.co_id)
     AND upper(a.company_code) = upper(b.company_code)
     AND b.user_id = session_user()
;
