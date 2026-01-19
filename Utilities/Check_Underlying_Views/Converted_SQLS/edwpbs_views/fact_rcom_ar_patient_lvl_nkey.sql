-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_rcom_ar_patient_lvl_nkey.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_rcom_ar_patient_lvl_nkey AS SELECT
    a.account_status_sid,
    b.account_status_name_child AS account_status_member,
    a.account_type_sid,
    c.account_type_member,
    a.age_month_sid,
    d.age_member,
    ROUND(a.ar_insurance_amt, 3, 'ROUND_HALF_EVEN') AS ar_insurance_amt,
    ROUND(a.ar_patient_amt, 3, 'ROUND_HALF_EVEN') AS ar_patient_amt,
    a.billed_patient_cnt,
    a.coid,
    a.collection_agency_sid,
    e.account_status_name_child AS collection_agency_member,
    a.company_code,
    a.contract_sid,
    f.contract_member,
    a.date_sid,
    g.time_member AS date_member,
    a.discharge_cnt,
    a.discharge_to_billing_day_cnt,
    ROUND(a.gross_charge_amt, 3, 'ROUND_HALF_EVEN') AS gross_charge_amt,
    a.iplan_insurance_order_num,
    ROUND(a.late_charge_credit_amt, 3, 'ROUND_HALF_EVEN') AS late_charge_credit_amt,
    ROUND(a.late_charge_debit_amt, 3, 'ROUND_HALF_EVEN') AS late_charge_debit_amt,
    a.patient_account_cnt,
    a.patient_financial_class_sid,
    h.patient_financial_class_member,
    ROUND(a.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
    a.patient_type_sid,
    i.patient_type_member,
    ROUND(a.payor_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS payor_adjustment_amt,
    ROUND(a.payor_prorated_liability_amt - a.prorated_liability_sys_adj_amt - a.payor_payment_amt - a.write_off_amt - a.payor_adjustment_amt - a.payor_contractual_amt, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt,
    ROUND(CASE
      WHEN a.iplan_insurance_order_num = 0 THEN a.ar_patient_amt
      ELSE a.ar_insurance_amt
    END, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt_calc,
    a.payor_bill_cnt,
    ROUND(a.payor_contractual_amt, 3, 'ROUND_HALF_EVEN') AS payor_contractual_amt,
    ROUND(a.payor_denial_amt, 3, 'ROUND_HALF_EVEN') AS payor_denial_amt,
    a.payor_denial_cnt,
    ROUND(a.payor_discrepancy_ovr_pmt_amt, 3, 'ROUND_HALF_EVEN') AS payor_discrepancy_ovr_pmt_amt,
    ROUND(a.payor_discrepancy_undr_pmt_amt, 3, 'ROUND_HALF_EVEN') AS payor_discrepancy_undr_pmt_amt,
    ROUND(a.payor_expected_payment_amt, 3, 'ROUND_HALF_EVEN') AS payor_expected_payment_amt,
    a.payor_financial_class_sid,
    j.payor_financial_class_member,
    ROUND(a.payor_payment_amt, 3, 'ROUND_HALF_EVEN') AS payor_payment_amt,
    ROUND(a.payor_prorated_liability_amt, 3, 'ROUND_HALF_EVEN') AS payor_prorated_liability_amt,
    a.payor_rebill_cnt,
    a.payor_sid,
    k.payor_member,
    ROUND(a.payor_up_front_collection_amt, 3, 'ROUND_HALF_EVEN') AS payor_up_front_collection_amt,
    a.product_sid,
    l.product_member,
    ROUND(a.prorated_liability_sys_adj_amt, 3, 'ROUND_HALF_EVEN') AS prorated_liability_sys_adj_amt,
    a.scenario_sid,
    m.scenario_member,
    a.source_sid,
    ROUND(a.total_collect_amt, 3, 'ROUND_HALF_EVEN') AS total_collect_amt,
    ROUND(a.unbilled_gross_bus_ofc_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_gross_bus_ofc_amt,
    ROUND(a.unbilled_gross_med_rec_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_gross_med_rec_amt,
    a.unit_num_sid,
    n.org_name_child AS unit_num_member,
    ROUND(a.write_off_amt, 3, 'ROUND_HALF_EVEN') AS write_off_amt,
    a.same_store_sid
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_ar_patient_level AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_account_status AS b ON a.account_status_sid = b.account_status_sid
     AND b.account_status_sid IN(
      1, 2, 3, 7, 8, 10
    )
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_account_type_dim AS c ON a.account_type_sid = c.account_type_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_age_dim AS d ON a.age_month_sid = d.age_month_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_account_status AS e ON a.collection_agency_sid + 7000 = e.account_status_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_contract_dim AS f ON a.contract_sid = f.contract_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS g ON a.date_sid = g.time_id
     AND a.date_sid = CASE
       format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
      WHEN '' THEN 0.0
      ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) as FLOAT64)
    END
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_patient_fin_class_dim AS h ON a.patient_financial_class_sid = h.patient_financial_class_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_patient_type_dim AS i ON a.patient_type_sid = i.patient_type_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_payor_financial_class_dim AS j ON a.payor_financial_class_sid = j.payor_financial_class_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_payor_dim AS k ON a.payor_sid = k.payor_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_product_dim AS l ON a.product_sid = l.product_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_scenario_dim AS m ON a.scenario_sid = m.scenario_sid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.dim_organization AS n ON a.unit_num_sid = n.org_sid
     AND upper(n.org_hier_name) LIKE '%AR ORG HIER%'
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS o ON upper(a.coid) = upper(o.co_id)
     AND upper(a.company_code) = upper(o.company_code)
     AND o.user_id = session_user()
;
