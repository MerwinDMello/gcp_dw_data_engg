-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_patient_iplan_conv.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_patient_iplan_conv
   OPTIONS(description='This table can be used to determine the conversions in the Payments to Charges Rate by Iplan')
  AS SELECT
      ROUND(fact_patient_iplan_conv.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      fact_patient_iplan_conv.month_id,
      fact_patient_iplan_conv.pe_date,
      fact_patient_iplan_conv.week_month_code,
      fact_patient_iplan_conv.coid,
      fact_patient_iplan_conv.company_code,
      fact_patient_iplan_conv.unit_num,
      ROUND(fact_patient_iplan_conv.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      fact_patient_iplan_conv.same_store_sid,
      fact_patient_iplan_conv.from_patient_type_code,
      fact_patient_iplan_conv.to_patient_type_code,
      fact_patient_iplan_conv.from_financial_class_code,
      fact_patient_iplan_conv.to_financial_class_code,
      fact_patient_iplan_conv.from_iplan_id,
      fact_patient_iplan_conv.to_iplan_id,
      fact_patient_iplan_conv.from_case_cnt,
      fact_patient_iplan_conv.to_case_cnt,
      ROUND(fact_patient_iplan_conv.from_calc_amt, 3, 'ROUND_HALF_EVEN') AS from_calc_amt,
      ROUND(fact_patient_iplan_conv.to_calc_amt, 3, 'ROUND_HALF_EVEN') AS to_calc_amt,
      ROUND(fact_patient_iplan_conv.conv_change_amt, 3, 'ROUND_HALF_EVEN') AS conv_change_amt,
      ROUND(fact_patient_iplan_conv.from_total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS from_total_billed_charge_amt,
      ROUND(fact_patient_iplan_conv.to_total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS to_total_billed_charge_amt,
      ROUND(fact_patient_iplan_conv.from_eor_gross_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS from_eor_gross_reimbursement_amt,
      ROUND(fact_patient_iplan_conv.to_eor_gross_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS to_eor_gross_reimbursement_amt,
      ROUND(fact_patient_iplan_conv.from_financial_class_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS from_financial_class_payment_rate_calc,
      ROUND(fact_patient_iplan_conv.to_financial_class_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS to_financial_class_payment_rate_calc,
      ROUND(fact_patient_iplan_conv.from_iplan_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS from_iplan_payment_rate_calc,
      ROUND(fact_patient_iplan_conv.to_iplan_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS to_iplan_payment_rate_calc,
      ROUND(fact_patient_iplan_conv.from_sma_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS from_sma_payment_rate_calc,
      ROUND(fact_patient_iplan_conv.to_sma_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS to_sma_payment_rate_calc,
      fact_patient_iplan_conv.source_system_code,
      fact_patient_iplan_conv.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.fact_patient_iplan_conv
  ;
