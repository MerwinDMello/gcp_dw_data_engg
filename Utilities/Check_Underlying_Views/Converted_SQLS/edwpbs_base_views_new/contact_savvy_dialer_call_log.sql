-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/contact_savvy_dialer_call_log.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.contact_savvy_dialer_call_log AS SELECT
    contact_savvy_dialer_call_log.call_log_key_num,
    contact_savvy_dialer_call_log.call_initiated_date_time,
    contact_savvy_dialer_call_log.acct_activity_id,
    contact_savvy_dialer_call_log.artiva_instance_code,
    contact_savvy_dialer_call_log.patient_dw_id,
    contact_savvy_dialer_call_log.call_log_pool_num,
    contact_savvy_dialer_call_log.month_id,
    contact_savvy_dialer_call_log.company_code,
    contact_savvy_dialer_call_log.coid,
    contact_savvy_dialer_call_log.unit_num,
    contact_savvy_dialer_call_log.pat_acct_num,
    contact_savvy_dialer_call_log.dialed_phone_num,
    contact_savvy_dialer_call_log.call_terminated_date_time,
    contact_savvy_dialer_call_log.call_record_id,
    contact_savvy_dialer_call_log.call_talk_time_scnd_amt,
    contact_savvy_dialer_call_log.call_hold_time_scnd_amt,
    contact_savvy_dialer_call_log.call_dialed_date_time,
    contact_savvy_dialer_call_log.call_wait_time_scnd_amt,
    contact_savvy_dialer_call_log.call_update_time_scnd_amt,
    contact_savvy_dialer_call_log.call_manual_time_scnd_amt,
    contact_savvy_dialer_call_log.call_log_type_code,
    contact_savvy_dialer_call_log.pool_dialer_type_code,
    contact_savvy_dialer_call_log.voice_trak_recorded_ind,
    contact_savvy_dialer_call_log.short_call_ind,
    contact_savvy_dialer_call_log.pool_assignment_id,
    contact_savvy_dialer_call_log.file_created_by_user_id,
    contact_savvy_dialer_call_log.source_system_code,
    contact_savvy_dialer_call_log.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.contact_savvy_dialer_call_log
;
