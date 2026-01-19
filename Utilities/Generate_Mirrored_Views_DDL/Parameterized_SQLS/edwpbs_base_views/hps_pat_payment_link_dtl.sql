-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/hps_pat_payment_link_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_base_views_dataset_name }}.hps_pat_payment_link_dtl
   OPTIONS(description='This table is used for reporting the Payment link details which are created for the HPS transactions and passed on to PA to settle the patient balances.')
  AS SELECT
      ROUND(hps_pat_payment_link_dtl.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      hps_pat_payment_link_dtl.coid,
      hps_pat_payment_link_dtl.payment_link_created_date_time_ct,
      hps_pat_payment_link_dtl.payment_link_accessed_seq_num,
      ROUND(hps_pat_payment_link_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      hps_pat_payment_link_dtl.unit_num,
      hps_pat_payment_link_dtl.company_code,
      hps_pat_payment_link_dtl.adjustment_applied_ind,
      ROUND(hps_pat_payment_link_dtl.payment_amt, 3, 'ROUND_HALF_EVEN') AS payment_amt,
      ROUND(hps_pat_payment_link_dtl.adjustment_amt, 3, 'ROUND_HALF_EVEN') AS adjustment_amt,
      hps_pat_payment_link_dtl.payment_link_last_accessed_date_time_ct,
      hps_pat_payment_link_dtl.payment_processed_ind,
      hps_pat_payment_link_dtl.payment_processed_date_time_ct,
      hps_pat_payment_link_dtl.payment_taker_user_id,
      hps_pat_payment_link_dtl.payment_link_expiration_date_time_ct,
      hps_pat_payment_link_dtl.unsubscribed_reminder_ind,
      hps_pat_payment_link_dtl.unsubscribed_reminder_timestamp,
      hps_pat_payment_link_dtl.email_reminder_sent_timestamp,
      hps_pat_payment_link_dtl.email_reminder_sent_cnt,
      hps_pat_payment_link_dtl.data_source_code,
      hps_pat_payment_link_dtl.source_system_code,
      hps_pat_payment_link_dtl.dw_last_update_date_time
    FROM
      {{ params.param_pbs_core_dataset_name }}.hps_pat_payment_link_dtl
  ;
