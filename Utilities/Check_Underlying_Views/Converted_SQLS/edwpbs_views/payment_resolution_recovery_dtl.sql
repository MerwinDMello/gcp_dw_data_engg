-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/payment_resolution_recovery_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.payment_resolution_recovery_dtl
   OPTIONS(description='Payment resolution amounts captured by Payment Resolution Application at the transaction level is maintained in the table.')
  AS SELECT
      a.payment_resolution_recovery_id,
      a.reporting_date,
      a.rpt_freq_type_code,
      ROUND(a.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.company_code,
      a.coid,
      a.unit_num,
      a.created_by_user_id,
      a.created_date_time,
      a.file_id,
      a.invalid_ind,
      a.month_id,
      a.iplan_id,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.financial_class_sid,
      a.patient_type_sid,
      a.payment_date,
      a.payor_sid,
      ROUND(a.reason_code_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_sid,
      a.updated_by_user_id,
      a.updated_by_date_time,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_resolution_recovery_dtl AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
