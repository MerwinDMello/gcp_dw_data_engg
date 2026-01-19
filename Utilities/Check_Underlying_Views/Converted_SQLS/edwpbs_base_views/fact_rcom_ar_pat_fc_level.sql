-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_rcom_ar_pat_fc_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_ar_pat_fc_level
   OPTIONS(description='Maintains Gross Revenue and Type 1 Cash AR by Patient financial Class level.')
  AS SELECT
      fact_rcom_ar_pat_fc_level.unit_num_sid,
      fact_rcom_ar_pat_fc_level.patient_financial_class_sid,
      fact_rcom_ar_pat_fc_level.patient_type_sid,
      fact_rcom_ar_pat_fc_level.scenario_sid,
      fact_rcom_ar_pat_fc_level.time_sid,
      fact_rcom_ar_pat_fc_level.source_sid,
      fact_rcom_ar_pat_fc_level.company_code,
      fact_rcom_ar_pat_fc_level.coid,
      ROUND(fact_rcom_ar_pat_fc_level.cash_receipt_amt, 3, 'ROUND_HALF_EVEN') AS cash_receipt_amt,
      ROUND(fact_rcom_ar_pat_fc_level.gross_revenue_amt, 3, 'ROUND_HALF_EVEN') AS gross_revenue_amt,
      fact_rcom_ar_pat_fc_level.source_system_code,
      fact_rcom_ar_pat_fc_level.dw_last_update_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.fact_rcom_ar_pat_fc_level
  ;
