-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_pars_discrepancy.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_rcom_pars_discrepancy AS SELECT
    fact_rcom_pars_discrepancy.date_sid,
    ROUND(fact_rcom_pars_discrepancy.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(fact_rcom_pars_discrepancy.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    fact_rcom_pars_discrepancy.iplan_insurance_order_num,
    fact_rcom_pars_discrepancy.eor_log_date,
    fact_rcom_pars_discrepancy.log_id,
    fact_rcom_pars_discrepancy.ar_bill_thru_date,
    fact_rcom_pars_discrepancy.log_sequence_num,
    fact_rcom_pars_discrepancy.discrepancy_creation_date,
    CASE
       CASE
         substr(CAST(fact_rcom_pars_discrepancy.date_sid as STRING), 1, 4)
        WHEN '' THEN 0
        ELSE CAST(substr(CAST(fact_rcom_pars_discrepancy.date_sid as STRING), 1, 4) as INT64)
      END - CASE
         format_date('%Y', fact_rcom_pars_discrepancy.discrepancy_creation_date)
        WHEN '' THEN 0
        ELSE CAST(format_date('%Y', fact_rcom_pars_discrepancy.discrepancy_creation_date) as INT64)
      END
      WHEN 0 THEN 1
      WHEN 1 THEN 2
      WHEN 2 THEN 3
      WHEN 3 THEN 4
      ELSE 4
    END AS year_created_sid,
    ROUND(fact_rcom_pars_discrepancy.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
    fact_rcom_pars_discrepancy.payor_financial_class_sid,
    fact_rcom_pars_discrepancy.payor_sid,
    fact_rcom_pars_discrepancy.payor_sequence_sid,
    fact_rcom_pars_discrepancy.patient_type_sid,
    fact_rcom_pars_discrepancy.unit_num_sid,
    fact_rcom_pars_discrepancy.discrepancy_age_month_sid,
    fact_rcom_pars_discrepancy.discharge_age_month_sid,
    fact_rcom_pars_discrepancy.scenario_sid,
    ROUND(fact_rcom_pars_discrepancy.reason_code_1_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_1_sid,
    ROUND(fact_rcom_pars_discrepancy.reason_code_2_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_2_sid,
    ROUND(fact_rcom_pars_discrepancy.reason_code_3_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_3_sid,
    ROUND(fact_rcom_pars_discrepancy.reason_code_4_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_4_sid,
    fact_rcom_pars_discrepancy.gross_rbmt_dcrp_type_sid,
    fact_rcom_pars_discrepancy.cont_alw_dcrp_type_sid,
    fact_rcom_pars_discrepancy.payment_dcrp_type_sid,
    fact_rcom_pars_discrepancy.charge_dcrp_type_sid,
    fact_rcom_pars_discrepancy.gross_rbmt_new_strf_sid,
    fact_rcom_pars_discrepancy.cont_alw_new_strf_sid,
    fact_rcom_pars_discrepancy.payment_new_strf_sid,
    fact_rcom_pars_discrepancy.charge_new_strf_sid,
    fact_rcom_pars_discrepancy.gross_rbmt_end_strf_sid,
    fact_rcom_pars_discrepancy.cont_alw_end_strf_sid,
    fact_rcom_pars_discrepancy.payment_end_strf_sid,
    fact_rcom_pars_discrepancy.charge_end_strf_sid,
    fact_rcom_pars_discrepancy.source_sid,
    fact_rcom_pars_discrepancy.coid,
    fact_rcom_pars_discrepancy.company_code,
    fact_rcom_pars_discrepancy.resolve_reason_code,
    fact_rcom_pars_discrepancy.discharge_date,
    fact_rcom_pars_discrepancy.ra_remit_date,
    fact_rcom_pars_discrepancy.reason_assignment_date_1,
    fact_rcom_pars_discrepancy.reason_assignment_date_2,
    fact_rcom_pars_discrepancy.reason_assignment_date_3,
    fact_rcom_pars_discrepancy.reason_assignment_date_4,
    ROUND(fact_rcom_pars_discrepancy.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
    ROUND(fact_rcom_pars_discrepancy.var_gross_rbmt_beg_amt, 3, 'ROUND_HALF_EVEN') AS var_gross_rbmt_beg_amt,
    ROUND(fact_rcom_pars_discrepancy.var_gross_rbmt_new_amt, 3, 'ROUND_HALF_EVEN') AS var_gross_rbmt_new_amt,
    ROUND(fact_rcom_pars_discrepancy.var_gross_rbmt_resolved_amt, 3, 'ROUND_HALF_EVEN') AS var_gross_rbmt_resolved_amt,
    ROUND(fact_rcom_pars_discrepancy.var_gross_rbmt_othr_cor_amt, 3, 'ROUND_HALF_EVEN') AS var_gross_rbmt_othr_cor_amt,
    ROUND(fact_rcom_pars_discrepancy.var_gross_rbmt_end_amt, 3, 'ROUND_HALF_EVEN') AS var_gross_rbmt_end_amt,
    ROUND(fact_rcom_pars_discrepancy.exp_gross_rbmt_crnt_amt, 3, 'ROUND_HALF_EVEN') AS exp_gross_rbmt_crnt_amt,
    ROUND(fact_rcom_pars_discrepancy.exp_gross_rbmt_rate_amt, 3, 'ROUND_HALF_EVEN') AS exp_gross_rbmt_rate_amt,
    fact_rcom_pars_discrepancy.var_gross_rbmt_beg_cnt,
    fact_rcom_pars_discrepancy.var_gross_rbmt_new_cnt,
    fact_rcom_pars_discrepancy.var_gross_rbmt_resolved_cnt,
    fact_rcom_pars_discrepancy.var_gross_rbmt_othr_cor_cnt,
    fact_rcom_pars_discrepancy.var_gross_rbmt_end_cnt,
    fact_rcom_pars_discrepancy.exp_gross_rbmt_crnt_cnt,
    fact_rcom_pars_discrepancy.exp_gross_rbmt_rate_cnt,
    ROUND(fact_rcom_pars_discrepancy.var_cont_alw_beg_amt, 3, 'ROUND_HALF_EVEN') AS var_cont_alw_beg_amt,
    ROUND(fact_rcom_pars_discrepancy.var_cont_alw_new_amt, 3, 'ROUND_HALF_EVEN') AS var_cont_alw_new_amt,
    ROUND(fact_rcom_pars_discrepancy.var_cont_alw_resolved_amt, 3, 'ROUND_HALF_EVEN') AS var_cont_alw_resolved_amt,
    ROUND(fact_rcom_pars_discrepancy.var_cont_alw_othr_cor_amt, 3, 'ROUND_HALF_EVEN') AS var_cont_alw_othr_cor_amt,
    ROUND(fact_rcom_pars_discrepancy.var_cont_alw_end_amt, 3, 'ROUND_HALF_EVEN') AS var_cont_alw_end_amt,
    ROUND(fact_rcom_pars_discrepancy.exp_cont_alw_crnt_amt, 3, 'ROUND_HALF_EVEN') AS exp_cont_alw_crnt_amt,
    ROUND(fact_rcom_pars_discrepancy.exp_cont_alw_rate_amt, 3, 'ROUND_HALF_EVEN') AS exp_cont_alw_rate_amt,
    fact_rcom_pars_discrepancy.var_cont_alw_beg_cnt,
    fact_rcom_pars_discrepancy.var_cont_alw_new_cnt,
    fact_rcom_pars_discrepancy.var_cont_alw_resolved_cnt,
    fact_rcom_pars_discrepancy.var_cont_alw_othr_cor_cnt,
    fact_rcom_pars_discrepancy.var_cont_alw_end_cnt,
    fact_rcom_pars_discrepancy.exp_cont_alw_crnt_cnt,
    fact_rcom_pars_discrepancy.exp_cont_alw_rate_cnt,
    ROUND(fact_rcom_pars_discrepancy.var_payment_beg_amt, 3, 'ROUND_HALF_EVEN') AS var_payment_beg_amt,
    ROUND(fact_rcom_pars_discrepancy.var_payment_new_amt, 3, 'ROUND_HALF_EVEN') AS var_payment_new_amt,
    ROUND(fact_rcom_pars_discrepancy.var_payment_resolved_amt, 3, 'ROUND_HALF_EVEN') AS var_payment_resolved_amt,
    ROUND(fact_rcom_pars_discrepancy.var_payment_othr_cor_amt, 3, 'ROUND_HALF_EVEN') AS var_payment_othr_cor_amt,
    ROUND(fact_rcom_pars_discrepancy.var_payment_end_amt, 3, 'ROUND_HALF_EVEN') AS var_payment_end_amt,
    ROUND(fact_rcom_pars_discrepancy.exp_payment_crnt_amt, 3, 'ROUND_HALF_EVEN') AS exp_payment_crnt_amt,
    ROUND(fact_rcom_pars_discrepancy.exp_payment_rate_amt, 3, 'ROUND_HALF_EVEN') AS exp_payment_rate_amt,
    fact_rcom_pars_discrepancy.var_payment_beg_cnt,
    fact_rcom_pars_discrepancy.var_payment_new_cnt,
    fact_rcom_pars_discrepancy.var_payment_resolved_cnt,
    fact_rcom_pars_discrepancy.var_payment_othr_cor_cnt,
    fact_rcom_pars_discrepancy.var_payment_end_cnt,
    fact_rcom_pars_discrepancy.exp_payment_crnt_cnt,
    fact_rcom_pars_discrepancy.exp_payment_rate_cnt,
    ROUND(fact_rcom_pars_discrepancy.var_charge_beg_amt, 3, 'ROUND_HALF_EVEN') AS var_charge_beg_amt,
    ROUND(fact_rcom_pars_discrepancy.var_charge_new_amt, 3, 'ROUND_HALF_EVEN') AS var_charge_new_amt,
    ROUND(fact_rcom_pars_discrepancy.var_charge_resolved_amt, 3, 'ROUND_HALF_EVEN') AS var_charge_resolved_amt,
    ROUND(fact_rcom_pars_discrepancy.var_charge_othr_cor_amt, 3, 'ROUND_HALF_EVEN') AS var_charge_othr_cor_amt,
    ROUND(fact_rcom_pars_discrepancy.exp_charge_crnt_amt, 3, 'ROUND_HALF_EVEN') AS exp_charge_crnt_amt,
    ROUND(fact_rcom_pars_discrepancy.var_charge_end_amt, 3, 'ROUND_HALF_EVEN') AS var_charge_end_amt,
    fact_rcom_pars_discrepancy.var_charge_beg_cnt,
    fact_rcom_pars_discrepancy.var_charge_new_cnt,
    fact_rcom_pars_discrepancy.var_charge_resolved_cnt,
    fact_rcom_pars_discrepancy.var_charge_othr_cor_cnt,
    fact_rcom_pars_discrepancy.exp_charge_crnt_cnt,
    fact_rcom_pars_discrepancy.var_charge_end_cnt,
    fact_rcom_pars_discrepancy.primary_reason_code_change_cnt,
    fact_rcom_pars_discrepancy.discrepancy_days,
    fact_rcom_pars_discrepancy.discrepancy_resolved_date,
    ROUND(fact_rcom_pars_discrepancy.cc_reason_id, 0, 'ROUND_HALF_EVEN') AS cc_reason_id,
    ROUND(fact_rcom_pars_discrepancy.cc_account_payer_status_id, 0, 'ROUND_HALF_EVEN') AS cc_account_payer_status_id
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.fact_rcom_pars_discrepancy
;
