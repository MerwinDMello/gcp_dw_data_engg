-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/sma_rate_calculation.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.sma_rate_calculation AS SELECT
    sma_rate_calculation.month_id,
    sma_rate_calculation.coid,
    sma_rate_calculation.company_code,
    sma_rate_calculation.patient_type_code,
    sma_rate_calculation.financial_class_code,
    sma_rate_calculation.iplan_id,
    sma_rate_calculation.unit_num,
    sma_rate_calculation.case_cnt,
    sma_rate_calculation.sma_threshold_amt,
    sma_rate_calculation.total_billed_charge_amt,
    sma_rate_calculation.total_payment_amt,
    sma_rate_calculation.financial_class_payment_rate_calc,
    sma_rate_calculation.iplan_payment_rate_calc,
    sma_rate_calculation.sma_payment_rate_calc,
    sma_rate_calculation.source_system_code,
    sma_rate_calculation.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.sma_rate_calculation
;
