-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
    a.patient_sid,
    a.collection_agency_sid,
    a.denial_sid,
    a.payor_adjustment_amt AS adjustments,
    a.payor_contractual_amt AS allowances,
    a.payor_bill_cnt AS bills,
    a.payor_denial_amt AS denial_amt,
    a.payor_denial_cnt AS denial_count,
    a.discharge_cnt AS dsg,
    a.payor_expected_payment_amt AS exp_pmt,
    a.gross_charge_amt AS gross_charges,
    a.late_charge_credit_amt AS late_charges_cr,
    a.late_charge_debit_amt AS late_charges_dr,
    a.payor_prorated_liability_amt AS liability,
    a.prorated_liability_sys_adj_amt AS liab_sys_adj_amt,
    a.patient_account_cnt AS num_accts,
    a.payor_discrepancy_ovr_pmt_amt AS ovr_pmt,
    a.payor_payment_amt AS payments,
    a.discharge_to_billing_day_cnt AS days_from_dsg,
    a.billed_patient_cnt AS num_billed,
    a.payor_rebill_cnt AS rebills,
    a.ar_patient_amt AS self_pay_rev,
    a.total_collect_amt AS total_collect,
    a.ar_insurance_amt AS total_insurance,
    a.payor_up_front_collection_amt AS uf_collect,
    a.payor_discrepancy_undr_pmt_amt AS und_pmt,
    a.write_off_amt AS writeoffs,
    CASE
      WHEN a.iplan_insurance_order_num = 0 THEN a.ar_patient_amt
      ELSE a.ar_insurance_amt
    END AS ar_bal,
    a.copay_deduct_amt AS copay_deduct,
    a.unbilled_gross_bus_ofc_amt AS unbilled_bus_ofc,
    a.unbilled_gross_med_rec_amt AS unbilled_med_rec,
    a.payor_sequence_sid,
    a.liability_account_cnt,
    a.dollar_strf_sid,
    a.appeal_code_sid,
    a.denial_status_code,
    a.denial_date,
    coalesce(p.discharge_date, parse_date('%Y-%m-%d', '9999-12-31')) AS discharge_date
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_ar_patient_level_daily AS a
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_account_detail AS p ON p.coid = a.coid
     AND p.pat_acct_num = a.patient_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON b.co_id = a.coid
     AND b.company_code = a.company_code
     AND b.user_id = session_user()
;
