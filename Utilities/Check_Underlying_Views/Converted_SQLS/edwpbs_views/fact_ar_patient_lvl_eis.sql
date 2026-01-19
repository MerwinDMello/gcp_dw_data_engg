-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_ar_patient_lvl_eis.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_ar_patient_lvl_eis AS SELECT
    a.date_sid,
    a.scenario_sid,
    a.patient_type_sid,
    a.patient_financial_class_sid,
    a.unit_num_sid,
    a.coid,
    a.company_code,
    a.source_sid,
    a.age_month_sid,
    a.account_type_sid,
    a.account_status_sid,
    a.contract_sid,
    a.payor_financial_class_sid,
    a.product_sid,
    ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    a.iplan_id,
    a.iplan_insurance_order_num,
    a.payor_sid,
    ROUND(a.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
    a.liability_account_cnt,
    a.payor_sequence_sid,
    a.dollar_strf_sid,
    CASE
      WHEN upper(a.company_code) = 'H'
       AND upper(same_store.same_store_ind) = 'Y' THEN 1
      ELSE 0
    END AS same_store_sid,
    a.denial_sid,
    a.appeal_code_sid,
    a.denial_date,
    a.denial_status_code,
    a.collection_agency_sid,
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
    CASE
      WHEN a.iplan_insurance_order_num = 1 THEN a.ar_patient_amt
      ELSE NUMERIC '0.00'
    END AS copay_deduct,
    ROUND(a.unbilled_gross_bus_ofc_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_bus_ofc,
    ROUND(a.unbilled_gross_med_rec_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_med_rec
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_ar_patient_level AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.dim_organization AS dorg ON a.unit_num_sid = dorg.org_sid
     AND upper(dorg.org_hier_name) LIKE '%AR ORG HIER%'
     AND (upper(dorg.org_hier_name) NOT LIKE '%HISTORY'
     OR dorg.org_sid IN(
      28753, 28755, 28758, 28763, 28765, 28766, 28767, 28768, 28770, 28801, 28802, 28803, 28804, 28805, 28806, 28807, 28808, 28809, 28810, 28811, 28812, 28815, 28821, 28822, 28823, 28825, 28826, 28830, 28834, 28836, 28837, 28839, 28845, 28846, 28847, 28850, 28852, 28853, 28855
    ))
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(b.co_id) = upper(dorg.coid)
     AND upper(b.company_code) = upper(a.company_code)
     AND b.user_id = session_user()
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_same_store AS same_store ON upper(same_store.coid) = upper(a.coid)
     AND upper(same_store.gl_close_ind) = 'N'
     AND same_store.month_id = CASE
       format_date('%Y%m', CASE
        WHEN extract(DAY from current_date('US/Central')) < 4 THEN date_sub(date_add(current_date('US/Central'), interval -1 MONTH), interval extract(DAY from date_add(current_date('US/Central'), interval -1 MONTH)) DAY)
        ELSE date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
      END)
      WHEN '' THEN 0.0
      ELSE CAST(format_date('%Y%m', CASE
        WHEN extract(DAY from current_date('US/Central')) < 4 THEN date_sub(date_add(current_date('US/Central'), interval -1 MONTH), interval extract(DAY from date_add(current_date('US/Central'), interval -1 MONTH)) DAY)
        ELSE date_sub(current_date('US/Central'), interval extract(DAY from current_date('US/Central')) DAY)
      END) as FLOAT64)
    END
;
