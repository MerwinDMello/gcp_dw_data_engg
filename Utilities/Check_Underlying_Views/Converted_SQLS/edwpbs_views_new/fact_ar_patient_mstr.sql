-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_ar_patient_mstr.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_ar_patient_mstr AS SELECT
    a.date_sid,
    a.scenario_sid,
    a.patient_type_sid,
    a.patient_financial_class_sid,
    a.unit_num_sid,
    CAST(bqutil.fn.cw_td_normalize_number(a.coid) as INT64) AS coid_sid,
    a.source_sid,
    a.age_month_sid,
    a.account_type_sid,
    a.account_status_sid,
    a.contract_sid,
    a.payor_financial_class_sid,
    a.product_sid,
    a.payor_sid,
    a.patient_sid,
    a.liability_account_cnt,
    a.payor_sequence_sid,
    a.dollar_strf_sid,
    a.collection_agency_sid,
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
    CASE
      WHEN a.iplan_insurance_order_num = 1 THEN a.ar_patient_amt
      ELSE NUMERIC '0.00'
    END AS copay_deduct,
    a.unbilled_gross_bus_ofc_amt AS unbilled_bus_ofc,
    a.unbilled_gross_med_rec_amt AS unbilled_med_rec,
    'M' AS daily_monthly_flag
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_ar_patient_level AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.dim_organization AS dorg ON a.unit_num_sid = dorg.org_sid
     AND upper(dorg.org_hier_name) LIKE '%AR ORG HIER%'
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON b.co_id = dorg.coid
     AND b.company_code = a.company_code
     AND b.user_id = session_user()
UNION ALL
SELECT
    CAST(bqutil.fn.cw_td_normalize_number(substr(format_date('%Y%m', a.rptg_date), 1, 6)) as INT64) AS date_sid,
    a.scenario_sid,
    a.patient_type_sid,
    a.patient_financial_class_sid,
    a.unit_num_sid,
    CAST(bqutil.fn.cw_td_normalize_number(a.coid) as INT64) AS coid_sid,
    a.source_sid,
    a.age_month_sid,
    a.account_type_sid,
    a.account_status_sid,
    a.contract_sid,
    a.payor_financial_class_sid,
    a.product_sid,
    a.payor_sid,
    a.patient_sid,
    a.liability_account_cnt,
    a.payor_sequence_sid,
    a.dollar_strf_sid,
    a.collection_agency_sid,
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
    'D' AS daily_monthly_flag
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_ar_patient_level_daily AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON b.co_id = a.coid
     AND b.company_code = a.company_code
     AND b.user_id = session_user()
;
