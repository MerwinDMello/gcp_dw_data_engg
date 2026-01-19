-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/sma_rate_calculation.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.sma_rate_calculation AS SELECT
    a.month_id,
    a.coid,
    a.company_code,
    a.patient_type_code,
    a.financial_class_code,
    a.iplan_id,
    a.unit_num,
    a.case_cnt,
    a.sma_threshold_amt,
    a.total_billed_charge_amt,
    a.total_payment_amt,
    a.financial_class_payment_rate_calc,
    a.iplan_payment_rate_calc,
    a.sma_payment_rate_calc,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.sma_rate_calculation AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
