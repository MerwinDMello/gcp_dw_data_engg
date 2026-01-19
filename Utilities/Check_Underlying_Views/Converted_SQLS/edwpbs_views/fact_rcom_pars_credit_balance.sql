-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_rcom_pars_credit_balance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_rcom_pars_credit_balance
   OPTIONS(description='This table contains Credit Balance related details of all patient accounts')
  AS SELECT
      ROUND(a.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
      a.unit_num_sid,
      a.account_status_sid,
      a.patient_financial_class_sid,
      a.patient_type_sid,
      a.payor_sid,
      a.payor_sequence_sid,
      a.credit_status_sid,
      a.cr_bal_orig_age_month_sid,
      a.date_sid,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.company_code,
      a.coid,
      ROUND(a.credit_balance_amt, 3, 'ROUND_HALF_EVEN') AS credit_balance_amt,
      a.credit_balance_count,
      a.discharge_date,
      a.credit_balance_date,
      a.created_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_credit_balance AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
