-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_rcom_pars_discrepancy_all.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_rcom_pars_discrepancy_all AS SELECT
    a.date_sid,
    a.pe_date,
    ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    a.iplan_insurance_order_num,
    a.eor_log_date,
    a.log_id,
    a.ar_bill_thru_date,
    a.log_sequence_num,
    a.discrepancy_creation_date,
    a.year_created_sid,
    a.cost_year_sid,
    a.cost_year_end_date,
    ROUND(a.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
    a.payor_financial_class_sid,
    a.payor_sid,
    a.payor_sequence_sid,
    a.patient_type_sid,
    a.unit_num_sid,
    a.discrepancy_age_month_sid,
    a.discharge_age_month_sid,
    a.scenario_sid,
    ROUND(a.reason_code_1_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_1_sid,
    ROUND(a.reason_code_2_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_2_sid,
    ROUND(a.reason_code_3_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_3_sid,
    ROUND(a.reason_code_4_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_4_sid,
    a.source_sid,
    ROUND(a.cc_account_payer_status_id, 0, 'ROUND_HALF_EVEN') AS cc_account_payer_status_id,
    a.coid,
    a.company_code,
    a.resolve_reason_code,
    a.discharge_date,
    a.ra_remit_date,
    a.reason_assignment_date_1,
    a.reason_assignment_date_2,
    a.reason_assignment_date_3,
    a.reason_assignment_date_4,
    ROUND(a.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
    a.primary_reason_code_change_cnt,
    a.discrepancy_days,
    a.discrepancy_resolved_date,
    a.discrepany_type_code,
    a.discrepancy_type_sid,
    a.same_store_sid,
    a.dollar_strtf_sid,
    ROUND(a.var_beg_amt, 3, 'ROUND_HALF_EVEN') AS var_beg_amt,
    ROUND(a.var_new_amt, 3, 'ROUND_HALF_EVEN') AS var_new_amt,
    ROUND(a.var_resolved_amt, 3, 'ROUND_HALF_EVEN') AS var_resolved_amt,
    ROUND(a.var_othr_cor_amt, 3, 'ROUND_HALF_EVEN') AS var_othr_cor_amt,
    ROUND(a.var_end_amt, 3, 'ROUND_HALF_EVEN') AS var_end_amt,
    ROUND(a.exp_crnt_amt, 3, 'ROUND_HALF_EVEN') AS exp_crnt_amt,
    ROUND(a.exp_rate_amt, 3, 'ROUND_HALF_EVEN') AS exp_rate_amt,
    a.var_beg_cnt,
    a.var_new_cnt,
    a.var_resolved_cnt,
    a.var_othr_cor_cnt,
    a.var_end_cnt,
    a.exp_crnt_cnt,
    a.exp_rate_cnt,
    a.resolved_days,
    a.end_days
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_dcrp_all AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS sf ON upper(a.coid) = upper(sf.co_id)
     AND upper(a.company_code) = upper(sf.company_code)
     AND session_user() = sf.user_id
;
