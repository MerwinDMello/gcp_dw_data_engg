-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_ar_patient_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_ar_patient_level
   OPTIONS(description='Similar to existing PF table that maintains the AR.')
  AS SELECT
      ROUND(fact_rcom_ar_patient_level.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
      ROUND(fact_rcom_ar_patient_level.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      fact_rcom_ar_patient_level.account_type_sid,
      fact_rcom_ar_patient_level.account_status_sid,
      fact_rcom_ar_patient_level.age_month_sid,
      fact_rcom_ar_patient_level.patient_financial_class_sid,
      fact_rcom_ar_patient_level.patient_type_sid,
      fact_rcom_ar_patient_level.collection_agency_sid,
      fact_rcom_ar_patient_level.payor_financial_class_sid,
      fact_rcom_ar_patient_level.product_sid,
      fact_rcom_ar_patient_level.contract_sid,
      fact_rcom_ar_patient_level.scenario_sid,
      fact_rcom_ar_patient_level.unit_num_sid,
      fact_rcom_ar_patient_level.source_sid,
      fact_rcom_ar_patient_level.date_sid,
      fact_rcom_ar_patient_level.payor_sid,
      fact_rcom_ar_patient_level.dollar_strf_sid,
      fact_rcom_ar_patient_level.same_store_sid,
      fact_rcom_ar_patient_level.iplan_id,
      fact_rcom_ar_patient_level.iplan_insurance_order_num,
      fact_rcom_ar_patient_level.coid,
      fact_rcom_ar_patient_level.company_code,
      fact_rcom_ar_patient_level.denial_sid,
      fact_rcom_ar_patient_level.appeal_code_sid,
      fact_rcom_ar_patient_level.denial_date,
      fact_rcom_ar_patient_level.denial_status_code,
      fact_rcom_ar_patient_level.patient_account_cnt,
      fact_rcom_ar_patient_level.liability_account_cnt,
      fact_rcom_ar_patient_level.payor_sequence_sid,
      fact_rcom_ar_patient_level.discharge_cnt,
      ROUND(fact_rcom_ar_patient_level.ar_patient_amt, 3, 'ROUND_HALF_EVEN') AS ar_patient_amt,
      ROUND(fact_rcom_ar_patient_level.ar_insurance_amt, 3, 'ROUND_HALF_EVEN') AS ar_insurance_amt,
      ROUND(fact_rcom_ar_patient_level.write_off_amt, 3, 'ROUND_HALF_EVEN') AS write_off_amt,
      ROUND(fact_rcom_ar_patient_level.total_collect_amt, 3, 'ROUND_HALF_EVEN') AS total_collect_amt,
      fact_rcom_ar_patient_level.billed_patient_cnt,
      fact_rcom_ar_patient_level.discharge_to_billing_day_cnt,
      ROUND(fact_rcom_ar_patient_level.gross_charge_amt, 3, 'ROUND_HALF_EVEN') AS gross_charge_amt,
      ROUND(fact_rcom_ar_patient_level.late_charge_credit_amt, 3, 'ROUND_HALF_EVEN') AS late_charge_credit_amt,
      ROUND(fact_rcom_ar_patient_level.late_charge_debit_amt, 3, 'ROUND_HALF_EVEN') AS late_charge_debit_amt,
      ROUND(fact_rcom_ar_patient_level.payor_prorated_liability_amt, 3, 'ROUND_HALF_EVEN') AS payor_prorated_liability_amt,
      ROUND(fact_rcom_ar_patient_level.payor_payment_amt, 3, 'ROUND_HALF_EVEN') AS payor_payment_amt,
      ROUND(fact_rcom_ar_patient_level.prorated_liability_sys_adj_amt, 3, 'ROUND_HALF_EVEN') AS prorated_liability_sys_adj_amt,
      ROUND(fact_rcom_ar_patient_level.payor_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS payor_adjustment_amt,
      ROUND(fact_rcom_ar_patient_level.payor_contractual_amt, 3, 'ROUND_HALF_EVEN') AS payor_contractual_amt,
      ROUND(fact_rcom_ar_patient_level.payor_denial_amt, 3, 'ROUND_HALF_EVEN') AS payor_denial_amt,
      fact_rcom_ar_patient_level.payor_denial_cnt,
      ROUND(fact_rcom_ar_patient_level.payor_expected_payment_amt, 3, 'ROUND_HALF_EVEN') AS payor_expected_payment_amt,
      ROUND(fact_rcom_ar_patient_level.payor_discrepancy_ovr_pmt_amt, 3, 'ROUND_HALF_EVEN') AS payor_discrepancy_ovr_pmt_amt,
      ROUND(fact_rcom_ar_patient_level.payor_discrepancy_undr_pmt_amt, 3, 'ROUND_HALF_EVEN') AS payor_discrepancy_undr_pmt_amt,
      ROUND(fact_rcom_ar_patient_level.payor_up_front_collection_amt, 3, 'ROUND_HALF_EVEN') AS payor_up_front_collection_amt,
      fact_rcom_ar_patient_level.payor_bill_cnt,
      fact_rcom_ar_patient_level.payor_rebill_cnt,
      ROUND(fact_rcom_ar_patient_level.unbilled_gross_bus_ofc_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_gross_bus_ofc_amt,
      ROUND(fact_rcom_ar_patient_level.unbilled_gross_med_rec_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_gross_med_rec_amt
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.fact_rcom_ar_patient_level
  ;
