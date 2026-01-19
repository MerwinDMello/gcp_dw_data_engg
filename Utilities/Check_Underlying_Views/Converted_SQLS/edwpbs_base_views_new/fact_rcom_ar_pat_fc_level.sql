-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_ar_pat_fc_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_ar_pat_fc_level AS SELECT
    fact_rcom_ar_pat_fc_level.unit_num_sid,
    fact_rcom_ar_pat_fc_level.patient_financial_class_sid,
    fact_rcom_ar_pat_fc_level.patient_type_sid,
    fact_rcom_ar_pat_fc_level.scenario_sid,
    fact_rcom_ar_pat_fc_level.time_sid,
    fact_rcom_ar_pat_fc_level.source_sid,
    fact_rcom_ar_pat_fc_level.company_code,
    fact_rcom_ar_pat_fc_level.coid,
    fact_rcom_ar_pat_fc_level.cash_receipt_amt,
    fact_rcom_ar_pat_fc_level.gross_revenue_amt,
    fact_rcom_ar_pat_fc_level.source_system_code,
    fact_rcom_ar_pat_fc_level.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.fact_rcom_ar_pat_fc_level
;
