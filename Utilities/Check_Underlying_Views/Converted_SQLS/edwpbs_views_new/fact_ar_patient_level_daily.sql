-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_ar_patient_level_daily.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_ar_patient_level_daily AS SELECT
    a.company_code,
    a.coid,
    a.rptg_date,
    a.source_sid,
    a.unit_num_sid,
    a.patient_sid,
    a.account_type_sid,
    a.account_status_sid,
    a.patient_type_sid,
    a.age_month_sid,
    a.patient_financial_class_sid,
    a.collection_agency_sid,
    a.payor_financial_class_sid,
    a.product_sid,
    a.contract_sid,
    a.scenario_sid,
    a.payor_sid,
    a.iplan_insurance_order_num,
    a.payor_sequence_sid,
    a.dollar_strf_sid,
    a.denial_sid,
    a.appeal_code_sid,
    a.denial_date,
    a.denial_status_code,
    a.liability_account_cnt,
    a.patient_account_cnt,
    a.discharge_cnt,
    a.ar_patient_amt,
    a.ar_insurance_amt,
    a.write_off_amt,
    a.total_collect_amt,
    a.billed_patient_cnt,
    a.discharge_to_billing_day_cnt,
    a.gross_charge_amt,
    a.prorated_liability_sys_adj_amt,
    a.late_charge_credit_amt,
    a.late_charge_debit_amt,
    a.payor_prorated_liability_amt,
    a.payor_payment_amt,
    a.payor_adjustment_amt,
    a.payor_contractual_amt,
    a.payor_denial_amt,
    a.payor_denial_cnt,
    a.payor_expected_payment_amt,
    a.payor_discrepancy_ovr_pmt_amt,
    a.payor_discrepancy_undr_pmt_amt,
    a.payor_up_front_collection_amt,
    a.payor_rebill_cnt,
    a.payor_bill_cnt,
    a.unbilled_gross_med_rec_amt,
    a.unbilled_gross_bus_ofc_amt,
    a.copay_deduct_amt,
    a.source_system_code,
    a.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_ar_patient_level_daily AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
