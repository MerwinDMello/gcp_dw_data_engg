-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_pars_credit_refund.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_credit_refund
   OPTIONS(description='This table contains Credit Refunded patient accounts showing Refund amounts and other amounts.')
  AS SELECT
      ROUND(fact_rcom_pars_credit_refund.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
      fact_rcom_pars_credit_refund.unit_num_sid,
      fact_rcom_pars_credit_refund.account_status_sid,
      fact_rcom_pars_credit_refund.payor_financial_class_sid,
      fact_rcom_pars_credit_refund.patient_type_sid,
      fact_rcom_pars_credit_refund.payor_sid,
      fact_rcom_pars_credit_refund.refund_type_sid,
      fact_rcom_pars_credit_refund.cr_refund_age_month_sid,
      fact_rcom_pars_credit_refund.date_sid,
      fact_rcom_pars_credit_refund.credit_refund_id,
      ROUND(fact_rcom_pars_credit_refund.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      fact_rcom_pars_credit_refund.company_code,
      fact_rcom_pars_credit_refund.coid,
      fact_rcom_pars_credit_refund.age_of_refund,
      fact_rcom_pars_credit_refund.take_back_ind,
      ROUND(fact_rcom_pars_credit_refund.refund_amount, 3, 'ROUND_HALF_EVEN') AS refund_amount,
      fact_rcom_pars_credit_refund.refund_count,
      ROUND(fact_rcom_pars_credit_refund.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      ROUND(fact_rcom_pars_credit_refund.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(fact_rcom_pars_credit_refund.total_cash_pay_amt, 3, 'ROUND_HALF_EVEN') AS total_cash_pay_amt,
      ROUND(fact_rcom_pars_credit_refund.total_allow_amt, 3, 'ROUND_HALF_EVEN') AS total_allow_amt,
      ROUND(fact_rcom_pars_credit_refund.total_policy_adj_amt, 3, 'ROUND_HALF_EVEN') AS total_policy_adj_amt,
      ROUND(fact_rcom_pars_credit_refund.total_write_off_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_amt,
      ROUND(fact_rcom_pars_credit_refund.take_back_amt, 3, 'ROUND_HALF_EVEN') AS take_back_amt,
      fact_rcom_pars_credit_refund.discharge_date,
      fact_rcom_pars_credit_refund.credit_balance_date,
      fact_rcom_pars_credit_refund.resolved_date,
      fact_rcom_pars_credit_refund.source_system_code,
      fact_rcom_pars_credit_refund.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.fact_rcom_pars_credit_refund
  ;
