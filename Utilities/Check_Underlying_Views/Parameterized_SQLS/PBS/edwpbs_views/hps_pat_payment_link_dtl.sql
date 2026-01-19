-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/hps_pat_payment_link_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.hps_pat_payment_link_dtl
   OPTIONS(description='This table is used for reporting the Payment link details which are created for the HPS transactions and passed on to PA to settle the patient balances.')
  AS SELECT
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.coid,
      a.payment_link_created_date_time_ct,
      a.payment_link_accessed_seq_num,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.unit_num,
      a.company_code,
      a.adjustment_applied_ind,
      ROUND(a.payment_amt, 3, 'ROUND_HALF_EVEN') AS payment_amt,
      ROUND(a.adjustment_amt, 3, 'ROUND_HALF_EVEN') AS adjustment_amt,
      a.payment_link_last_accessed_date_time_ct,
      a.payment_processed_ind,
      a.payment_processed_date_time_ct,
      a.payment_taker_user_id,
      a.payment_link_expiration_date_time_ct,
      a.unsubscribed_reminder_ind,
      a.unsubscribed_reminder_timestamp,
      a.email_reminder_sent_timestamp,
      a.email_reminder_sent_cnt,
      a.data_source_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.hps_pat_payment_link_dtl AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
