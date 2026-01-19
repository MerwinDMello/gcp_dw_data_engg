-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_rcom_ar_pat_fc_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_rcom_ar_pat_fc_level
   OPTIONS(description='Maintains Gross Revenue and Type 1 Cash AR by Patient financial Class level.')
  AS SELECT
      a.unit_num_sid,
      a.patient_financial_class_sid,
      a.patient_type_sid,
      a.scenario_sid,
      a.time_sid,
      a.source_sid,
      a.company_code,
      a.coid,
      ROUND(a.cash_receipt_amt, 3, 'ROUND_HALF_EVEN') AS cash_receipt_amt,
      ROUND(a.gross_revenue_amt, 3, 'ROUND_HALF_EVEN') AS gross_revenue_amt,
      a.source_system_code,
      a.dw_last_update_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_ar_pat_fc_level AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
