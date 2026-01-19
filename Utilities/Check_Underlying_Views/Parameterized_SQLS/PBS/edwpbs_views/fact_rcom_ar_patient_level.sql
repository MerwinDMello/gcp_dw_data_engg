-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_rcom_ar_patient_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.fact_rcom_ar_patient_level
   OPTIONS(description='Similar to existing PF table that maintains the AR.')
  AS SELECT
      ROUND(a.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      a.account_type_sid,
      a.account_status_sid,
      a.age_month_sid,
      a.patient_financial_class_sid,
      a.patient_type_sid,
      a.collection_agency_sid,
      a.payor_financial_class_sid,
      a.product_sid,
      a.contract_sid,
      a.scenario_sid,
      a.unit_num_sid,
      a.source_sid,
      a.date_sid,
      a.payor_sid,
      a.dollar_strf_sid,
      a.same_store_sid,
      a.iplan_id,
      a.iplan_insurance_order_num,
      a.coid,
      a.company_code,
      a.denial_sid,
      a.appeal_code_sid,
      a.denial_date,
      a.denial_status_code,
      a.patient_account_cnt,
      a.liability_account_cnt,
      a.payor_sequence_sid,
      a.discharge_cnt,
      ROUND(a.ar_patient_amt, 3, 'ROUND_HALF_EVEN') AS ar_patient_amt,
      ROUND(a.ar_insurance_amt, 3, 'ROUND_HALF_EVEN') AS ar_insurance_amt,
      ROUND(a.write_off_amt, 3, 'ROUND_HALF_EVEN') AS write_off_amt,
      ROUND(a.total_collect_amt, 3, 'ROUND_HALF_EVEN') AS total_collect_amt,
      a.billed_patient_cnt,
      a.discharge_to_billing_day_cnt,
      ROUND(a.gross_charge_amt, 3, 'ROUND_HALF_EVEN') AS gross_charge_amt,
      ROUND(a.late_charge_credit_amt, 3, 'ROUND_HALF_EVEN') AS late_charge_credit_amt,
      ROUND(a.late_charge_debit_amt, 3, 'ROUND_HALF_EVEN') AS late_charge_debit_amt,
      ROUND(a.payor_prorated_liability_amt, 3, 'ROUND_HALF_EVEN') AS payor_prorated_liability_amt,
      ROUND(a.payor_payment_amt, 3, 'ROUND_HALF_EVEN') AS payor_payment_amt,
      ROUND(a.prorated_liability_sys_adj_amt, 3, 'ROUND_HALF_EVEN') AS prorated_liability_sys_adj_amt,
      ROUND(a.payor_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS payor_adjustment_amt,
      ROUND(a.payor_contractual_amt, 3, 'ROUND_HALF_EVEN') AS payor_contractual_amt,
      ROUND(a.payor_denial_amt, 3, 'ROUND_HALF_EVEN') AS payor_denial_amt,
      a.payor_denial_cnt,
      ROUND(a.payor_expected_payment_amt, 3, 'ROUND_HALF_EVEN') AS payor_expected_payment_amt,
      ROUND(a.payor_discrepancy_ovr_pmt_amt, 3, 'ROUND_HALF_EVEN') AS payor_discrepancy_ovr_pmt_amt,
      ROUND(a.payor_discrepancy_undr_pmt_amt, 3, 'ROUND_HALF_EVEN') AS payor_discrepancy_undr_pmt_amt,
      ROUND(a.payor_up_front_collection_amt, 3, 'ROUND_HALF_EVEN') AS payor_up_front_collection_amt,
      a.payor_bill_cnt,
      a.payor_rebill_cnt,
      ROUND(a.unbilled_gross_bus_ofc_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_gross_bus_ofc_amt,
      ROUND(a.unbilled_gross_med_rec_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_gross_med_rec_amt
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.fact_rcom_ar_patient_level AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
