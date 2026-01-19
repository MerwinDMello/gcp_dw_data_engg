-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/contact_savvy_dialer_call_log.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.contact_savvy_dialer_call_log AS SELECT
    a.call_log_key_num,
    a.call_initiated_date_time,
    a.acct_activity_id,
    a.artiva_instance_code,
    a.patient_dw_id,
    a.call_log_pool_num,
    a.month_id,
    a.company_code,
    a.coid,
    a.unit_num,
    a.pat_acct_num,
    a.dialed_phone_num,
    a.call_terminated_date_time,
    a.call_record_id,
    a.call_talk_time_scnd_amt,
    a.call_hold_time_scnd_amt,
    a.call_dialed_date_time,
    a.call_wait_time_scnd_amt,
    a.call_update_time_scnd_amt,
    a.call_manual_time_scnd_amt,
    a.call_log_type_code,
    a.pool_dialer_type_code,
    a.voice_trak_recorded_ind,
    a.short_call_ind,
    a.pool_assignment_id,
    a.file_created_by_user_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.contact_savvy_dialer_call_log AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
