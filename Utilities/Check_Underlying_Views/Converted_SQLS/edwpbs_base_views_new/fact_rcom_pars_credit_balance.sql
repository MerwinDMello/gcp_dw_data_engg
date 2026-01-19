-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_pars_credit_balance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_credit_balance AS SELECT
    fact_rcom_pars_credit_balance.patient_sid,
    fact_rcom_pars_credit_balance.unit_num_sid,
    fact_rcom_pars_credit_balance.account_status_sid,
    fact_rcom_pars_credit_balance.patient_financial_class_sid,
    fact_rcom_pars_credit_balance.patient_type_sid,
    fact_rcom_pars_credit_balance.payor_sid,
    fact_rcom_pars_credit_balance.payor_sequence_sid,
    fact_rcom_pars_credit_balance.credit_status_sid,
    fact_rcom_pars_credit_balance.cr_bal_orig_age_month_sid,
    fact_rcom_pars_credit_balance.date_sid,
    fact_rcom_pars_credit_balance.patient_dw_id,
    fact_rcom_pars_credit_balance.company_code,
    fact_rcom_pars_credit_balance.coid,
    fact_rcom_pars_credit_balance.credit_balance_amt,
    fact_rcom_pars_credit_balance.credit_balance_count,
    fact_rcom_pars_credit_balance.discharge_date,
    fact_rcom_pars_credit_balance.credit_balance_date,
    fact_rcom_pars_credit_balance.created_date,
    fact_rcom_pars_credit_balance.source_system_code,
    fact_rcom_pars_credit_balance.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.fact_rcom_pars_credit_balance
;
