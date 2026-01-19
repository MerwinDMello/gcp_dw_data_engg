-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_patient_iplan_conv.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.fact_patient_iplan_conv
   OPTIONS(description='This table can be used to determine the conversions in the Payments to Charges Rate by Iplan')
  AS SELECT
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.month_id,
      a.pe_date,
      a.week_month_code,
      a.coid,
      a.company_code,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.same_store_sid,
      a.from_patient_type_code,
      a.to_patient_type_code,
      a.from_financial_class_code,
      a.to_financial_class_code,
      a.from_iplan_id,
      a.to_iplan_id,
      a.from_case_cnt,
      a.to_case_cnt,
      ROUND(a.from_calc_amt, 3, 'ROUND_HALF_EVEN') AS from_calc_amt,
      ROUND(a.to_calc_amt, 3, 'ROUND_HALF_EVEN') AS to_calc_amt,
      ROUND(a.conv_change_amt, 3, 'ROUND_HALF_EVEN') AS conv_change_amt,
      ROUND(a.from_total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS from_total_billed_charge_amt,
      ROUND(a.to_total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS to_total_billed_charge_amt,
      ROUND(a.from_eor_gross_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS from_eor_gross_reimbursement_amt,
      ROUND(a.to_eor_gross_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS to_eor_gross_reimbursement_amt,
      ROUND(a.from_financial_class_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS from_financial_class_payment_rate_calc,
      ROUND(a.to_financial_class_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS to_financial_class_payment_rate_calc,
      ROUND(a.from_iplan_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS from_iplan_payment_rate_calc,
      ROUND(a.to_iplan_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS to_iplan_payment_rate_calc,
      ROUND(a.from_sma_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS from_sma_payment_rate_calc,
      ROUND(a.to_sma_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS to_sma_payment_rate_calc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.fact_patient_iplan_conv AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
