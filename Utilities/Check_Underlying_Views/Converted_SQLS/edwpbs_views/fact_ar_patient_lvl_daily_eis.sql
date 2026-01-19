-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_ar_patient_lvl_daily_eis.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_ar_patient_lvl_daily_eis AS SELECT
    a.rptg_date,
    a.scenario_sid,
    a.patient_type_sid,
    a.patient_financial_class_sid,
    a.unit_num_sid,
    a.source_sid,
    a.age_month_sid,
    a.account_type_sid,
    a.account_status_sid,
    a.contract_sid,
    a.payor_financial_class_sid,
    a.product_sid,
    a.payor_sid,
    ROUND(a.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
    a.collection_agency_sid,
    a.denial_sid,
    ROUND(a.payor_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS adjustments,
    ROUND(a.payor_contractual_amt, 3, 'ROUND_HALF_EVEN') AS allowances,
    a.payor_bill_cnt AS bills,
    ROUND(a.payor_denial_amt, 3, 'ROUND_HALF_EVEN') AS denial_amt,
    a.payor_denial_cnt AS denial_count,
    a.discharge_cnt AS dsg,
    ROUND(a.payor_expected_payment_amt, 3, 'ROUND_HALF_EVEN') AS exp_pmt,
    ROUND(a.gross_charge_amt, 3, 'ROUND_HALF_EVEN') AS gross_charges,
    ROUND(a.late_charge_credit_amt, 3, 'ROUND_HALF_EVEN') AS late_charges_cr,
    ROUND(a.late_charge_debit_amt, 3, 'ROUND_HALF_EVEN') AS late_charges_dr,
    ROUND(a.payor_prorated_liability_amt, 3, 'ROUND_HALF_EVEN') AS liability,
    ROUND(a.prorated_liability_sys_adj_amt, 3, 'ROUND_HALF_EVEN') AS liab_sys_adj_amt,
    a.patient_account_cnt AS num_accts,
    ROUND(a.payor_discrepancy_ovr_pmt_amt, 3, 'ROUND_HALF_EVEN') AS ovr_pmt,
    ROUND(a.payor_payment_amt, 3, 'ROUND_HALF_EVEN') AS payments,
    a.discharge_to_billing_day_cnt AS days_from_dsg,
    a.billed_patient_cnt AS num_billed,
    a.payor_rebill_cnt AS rebills,
    ROUND(a.ar_patient_amt, 3, 'ROUND_HALF_EVEN') AS self_pay_rev,
    ROUND(a.total_collect_amt, 3, 'ROUND_HALF_EVEN') AS total_collect,
    ROUND(a.ar_insurance_amt, 3, 'ROUND_HALF_EVEN') AS total_insurance,
    ROUND(a.payor_up_front_collection_amt, 3, 'ROUND_HALF_EVEN') AS uf_collect,
    ROUND(a.payor_discrepancy_undr_pmt_amt, 3, 'ROUND_HALF_EVEN') AS und_pmt,
    ROUND(a.write_off_amt, 3, 'ROUND_HALF_EVEN') AS writeoffs,
    ROUND(CASE
      WHEN a.iplan_insurance_order_num = 0 THEN a.ar_patient_amt
      ELSE a.ar_insurance_amt
    END, 3, 'ROUND_HALF_EVEN') AS ar_bal,
    ROUND(a.copay_deduct_amt, 3, 'ROUND_HALF_EVEN') AS copay_deduct,
    ROUND(a.unbilled_gross_bus_ofc_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_bus_ofc,
    ROUND(a.unbilled_gross_med_rec_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_med_rec,
    a.payor_sequence_sid,
    a.liability_account_cnt,
    a.dollar_strf_sid,
    a.appeal_code_sid,
    a.denial_status_code,
    a.denial_date,
    coalesce(p.discharge_date, parse_date('%Y-%m-%d', '9999-12-31')) AS discharge_date
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_ar_patient_level_daily AS a
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_account_detail AS p ON upper(p.coid) = upper(a.coid)
     AND p.pat_acct_num = a.patient_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(b.co_id) = upper(a.coid)
     AND upper(b.company_code) = upper(a.company_code)
     AND b.user_id = session_user()
;
