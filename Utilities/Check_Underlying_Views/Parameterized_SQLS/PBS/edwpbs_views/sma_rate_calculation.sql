-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/sma_rate_calculation.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.sma_rate_calculation
   OPTIONS(description='Every month this table maitains the Threshold Amounts and the payment rate calculations for all the patients across a facility.')
  AS SELECT
      a.month_id,
      a.coid,
      a.company_code,
      a.patient_type_code,
      ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      a.iplan_id,
      a.unit_num,
      a.case_cnt,
      ROUND(a.sma_threshold_amt, 3, 'ROUND_HALF_EVEN') AS sma_threshold_amt,
      ROUND(a.total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_billed_charge_amt,
      ROUND(a.total_payment_amt, 3, 'ROUND_HALF_EVEN') AS total_payment_amt,
      ROUND(a.financial_class_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS financial_class_payment_rate_calc,
      ROUND(a.iplan_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS iplan_payment_rate_calc,
      ROUND(a.sma_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS sma_payment_rate_calc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.sma_rate_calculation AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
