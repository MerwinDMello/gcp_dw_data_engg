-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_ar_patient_level_daily.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.fact_ar_patient_level_daily
   OPTIONS(description='This is Daily  Account Receivable Inventory for HCA and Parallon Customers.')
  AS SELECT
      fact_ar_patient_level_daily.company_code,
      fact_ar_patient_level_daily.coid,
      fact_ar_patient_level_daily.rptg_date,
      fact_ar_patient_level_daily.source_sid,
      fact_ar_patient_level_daily.unit_num_sid,
      ROUND(fact_ar_patient_level_daily.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
      fact_ar_patient_level_daily.account_type_sid,
      fact_ar_patient_level_daily.account_status_sid,
      fact_ar_patient_level_daily.patient_type_sid,
      fact_ar_patient_level_daily.age_month_sid,
      fact_ar_patient_level_daily.patient_financial_class_sid,
      fact_ar_patient_level_daily.collection_agency_sid,
      fact_ar_patient_level_daily.payor_financial_class_sid,
      fact_ar_patient_level_daily.product_sid,
      fact_ar_patient_level_daily.contract_sid,
      fact_ar_patient_level_daily.scenario_sid,
      fact_ar_patient_level_daily.payor_sid,
      fact_ar_patient_level_daily.iplan_insurance_order_num,
      fact_ar_patient_level_daily.payor_sequence_sid,
      fact_ar_patient_level_daily.dollar_strf_sid,
      fact_ar_patient_level_daily.denial_sid,
      fact_ar_patient_level_daily.appeal_code_sid,
      fact_ar_patient_level_daily.denial_date,
      fact_ar_patient_level_daily.denial_status_code,
      fact_ar_patient_level_daily.liability_account_cnt,
      fact_ar_patient_level_daily.patient_account_cnt,
      fact_ar_patient_level_daily.discharge_cnt,
      ROUND(fact_ar_patient_level_daily.ar_patient_amt, 3, 'ROUND_HALF_EVEN') AS ar_patient_amt,
      ROUND(fact_ar_patient_level_daily.ar_insurance_amt, 3, 'ROUND_HALF_EVEN') AS ar_insurance_amt,
      ROUND(fact_ar_patient_level_daily.write_off_amt, 3, 'ROUND_HALF_EVEN') AS write_off_amt,
      ROUND(fact_ar_patient_level_daily.total_collect_amt, 3, 'ROUND_HALF_EVEN') AS total_collect_amt,
      fact_ar_patient_level_daily.billed_patient_cnt,
      fact_ar_patient_level_daily.discharge_to_billing_day_cnt,
      ROUND(fact_ar_patient_level_daily.gross_charge_amt, 3, 'ROUND_HALF_EVEN') AS gross_charge_amt,
      ROUND(fact_ar_patient_level_daily.prorated_liability_sys_adj_amt, 3, 'ROUND_HALF_EVEN') AS prorated_liability_sys_adj_amt,
      ROUND(fact_ar_patient_level_daily.late_charge_credit_amt, 3, 'ROUND_HALF_EVEN') AS late_charge_credit_amt,
      ROUND(fact_ar_patient_level_daily.late_charge_debit_amt, 3, 'ROUND_HALF_EVEN') AS late_charge_debit_amt,
      ROUND(fact_ar_patient_level_daily.payor_prorated_liability_amt, 3, 'ROUND_HALF_EVEN') AS payor_prorated_liability_amt,
      ROUND(fact_ar_patient_level_daily.payor_payment_amt, 3, 'ROUND_HALF_EVEN') AS payor_payment_amt,
      ROUND(fact_ar_patient_level_daily.payor_adjustment_amt, 3, 'ROUND_HALF_EVEN') AS payor_adjustment_amt,
      ROUND(fact_ar_patient_level_daily.payor_contractual_amt, 3, 'ROUND_HALF_EVEN') AS payor_contractual_amt,
      ROUND(fact_ar_patient_level_daily.payor_denial_amt, 3, 'ROUND_HALF_EVEN') AS payor_denial_amt,
      fact_ar_patient_level_daily.payor_denial_cnt,
      ROUND(fact_ar_patient_level_daily.payor_expected_payment_amt, 3, 'ROUND_HALF_EVEN') AS payor_expected_payment_amt,
      ROUND(fact_ar_patient_level_daily.payor_discrepancy_ovr_pmt_amt, 3, 'ROUND_HALF_EVEN') AS payor_discrepancy_ovr_pmt_amt,
      ROUND(fact_ar_patient_level_daily.payor_discrepancy_undr_pmt_amt, 3, 'ROUND_HALF_EVEN') AS payor_discrepancy_undr_pmt_amt,
      ROUND(fact_ar_patient_level_daily.payor_up_front_collection_amt, 3, 'ROUND_HALF_EVEN') AS payor_up_front_collection_amt,
      fact_ar_patient_level_daily.payor_rebill_cnt,
      fact_ar_patient_level_daily.payor_bill_cnt,
      ROUND(fact_ar_patient_level_daily.unbilled_gross_med_rec_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_gross_med_rec_amt,
      ROUND(fact_ar_patient_level_daily.unbilled_gross_bus_ofc_amt, 3, 'ROUND_HALF_EVEN') AS unbilled_gross_bus_ofc_amt,
      ROUND(fact_ar_patient_level_daily.copay_deduct_amt, 3, 'ROUND_HALF_EVEN') AS copay_deduct_amt,
      fact_ar_patient_level_daily.source_system_code,
      fact_ar_patient_level_daily.dw_last_update_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.fact_ar_patient_level_daily
  ;
