-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_pars_credit_refund.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_credit_refund AS SELECT
    fact_rcom_pars_credit_refund.patient_sid,
    fact_rcom_pars_credit_refund.unit_num_sid,
    fact_rcom_pars_credit_refund.account_status_sid,
    fact_rcom_pars_credit_refund.payor_financial_class_sid,
    fact_rcom_pars_credit_refund.patient_type_sid,
    fact_rcom_pars_credit_refund.payor_sid,
    fact_rcom_pars_credit_refund.refund_type_sid,
    fact_rcom_pars_credit_refund.cr_refund_age_month_sid,
    fact_rcom_pars_credit_refund.date_sid,
    fact_rcom_pars_credit_refund.credit_refund_id,
    fact_rcom_pars_credit_refund.patient_dw_id,
    fact_rcom_pars_credit_refund.company_code,
    fact_rcom_pars_credit_refund.coid,
    fact_rcom_pars_credit_refund.age_of_refund,
    fact_rcom_pars_credit_refund.take_back_ind,
    fact_rcom_pars_credit_refund.refund_amount,
    fact_rcom_pars_credit_refund.refund_count,
    fact_rcom_pars_credit_refund.total_account_balance_amt,
    fact_rcom_pars_credit_refund.total_charge_amt,
    fact_rcom_pars_credit_refund.total_cash_pay_amt,
    fact_rcom_pars_credit_refund.total_allow_amt,
    fact_rcom_pars_credit_refund.total_policy_adj_amt,
    fact_rcom_pars_credit_refund.total_write_off_amt,
    fact_rcom_pars_credit_refund.take_back_amt,
    fact_rcom_pars_credit_refund.discharge_date,
    fact_rcom_pars_credit_refund.credit_balance_date,
    fact_rcom_pars_credit_refund.resolved_date,
    fact_rcom_pars_credit_refund.source_system_code,
    fact_rcom_pars_credit_refund.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.fact_rcom_pars_credit_refund
;
