-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/contact_savvy_dialer_call_log.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.contact_savvy_dialer_call_log
   OPTIONS(description='Daily detailed Dialer Call Log from Contact Savvy Application Add-On to Artiva system.')
  AS SELECT
      ROUND(a.call_log_key_num, 0, 'ROUND_HALF_EVEN') AS call_log_key_num,
      a.call_initiated_date_time,
      ROUND(a.acct_activity_id, 0, 'ROUND_HALF_EVEN') AS acct_activity_id,
      a.artiva_instance_code,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.call_log_pool_num,
      a.month_id,
      a.company_code,
      a.coid,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      ROUND(a.dialed_phone_num, 0, 'ROUND_HALF_EVEN') AS dialed_phone_num,
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
      {{ params.param_pbs_base_views_dataset_name }}.contact_savvy_dialer_call_log AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
