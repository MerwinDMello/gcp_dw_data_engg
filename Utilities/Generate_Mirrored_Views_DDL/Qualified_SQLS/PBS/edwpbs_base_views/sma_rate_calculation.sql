-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/sma_rate_calculation.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.sma_rate_calculation
   OPTIONS(description='Every month this table maitains the Threshold Amounts and the payment rate calculations for all the patients across a facility.')
  AS SELECT
      sma_rate_calculation.month_id,
      sma_rate_calculation.coid,
      sma_rate_calculation.company_code,
      sma_rate_calculation.patient_type_code,
      ROUND(sma_rate_calculation.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      sma_rate_calculation.iplan_id,
      sma_rate_calculation.unit_num,
      sma_rate_calculation.case_cnt,
      ROUND(sma_rate_calculation.sma_threshold_amt, 3, 'ROUND_HALF_EVEN') AS sma_threshold_amt,
      ROUND(sma_rate_calculation.total_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_billed_charge_amt,
      ROUND(sma_rate_calculation.total_payment_amt, 3, 'ROUND_HALF_EVEN') AS total_payment_amt,
      ROUND(sma_rate_calculation.financial_class_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS financial_class_payment_rate_calc,
      ROUND(sma_rate_calculation.iplan_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS iplan_payment_rate_calc,
      ROUND(sma_rate_calculation.sma_payment_rate_calc, 4, 'ROUND_HALF_EVEN') AS sma_payment_rate_calc,
      sma_rate_calculation.source_system_code,
      sma_rate_calculation.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.sma_rate_calculation
  ;
