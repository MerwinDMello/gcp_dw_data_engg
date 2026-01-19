-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_rcom_pars_credit_refund.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.fact_rcom_pars_credit_refund
   OPTIONS(description='This table contains Credit Refunded patient accounts showing Refund amounts and other amounts.')
  AS SELECT
      ROUND(a.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
      a.unit_num_sid,
      a.account_status_sid,
      a.payor_financial_class_sid,
      a.patient_type_sid,
      a.payor_sid,
      a.refund_type_sid,
      a.cr_refund_age_month_sid,
      a.date_sid,
      a.credit_refund_id,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.company_code,
      a.coid,
      a.age_of_refund,
      a.take_back_ind,
      ROUND(a.refund_amount, 3, 'ROUND_HALF_EVEN') AS refund_amount,
      a.refund_count,
      ROUND(a.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      ROUND(a.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(a.total_cash_pay_amt, 3, 'ROUND_HALF_EVEN') AS total_cash_pay_amt,
      ROUND(a.total_allow_amt, 3, 'ROUND_HALF_EVEN') AS total_allow_amt,
      ROUND(a.total_policy_adj_amt, 3, 'ROUND_HALF_EVEN') AS total_policy_adj_amt,
      ROUND(a.total_write_off_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_amt,
      ROUND(a.take_back_amt, 3, 'ROUND_HALF_EVEN') AS take_back_amt,
      a.discharge_date,
      a.credit_balance_date,
      a.resolved_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.fact_rcom_pars_credit_refund AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
