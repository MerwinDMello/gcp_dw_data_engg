-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/hps_pat_payment_link_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.hps_pat_payment_link_dtl AS SELECT
    hps_pat_payment_link_dtl.pat_acct_num,
    hps_pat_payment_link_dtl.coid,
    hps_pat_payment_link_dtl.payment_link_created_date_time_ct,
    hps_pat_payment_link_dtl.payment_link_accessed_seq_num,
    hps_pat_payment_link_dtl.patient_dw_id,
    hps_pat_payment_link_dtl.unit_num,
    hps_pat_payment_link_dtl.company_code,
    hps_pat_payment_link_dtl.adjustment_applied_ind,
    hps_pat_payment_link_dtl.payment_amt,
    hps_pat_payment_link_dtl.adjustment_amt,
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
    `hca-hin-dev-cur-parallon`.edwpbs.hps_pat_payment_link_dtl
;
