-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/hps_pat_payment_link_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.hps_pat_payment_link_dtl AS SELECT
    a.pat_acct_num,
    a.coid,
    a.payment_link_created_date_time_ct,
    a.payment_link_accessed_seq_num,
    a.patient_dw_id,
    a.unit_num,
    a.company_code,
    a.adjustment_applied_ind,
    a.payment_amt,
    a.adjustment_amt,
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
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.hps_pat_payment_link_dtl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
