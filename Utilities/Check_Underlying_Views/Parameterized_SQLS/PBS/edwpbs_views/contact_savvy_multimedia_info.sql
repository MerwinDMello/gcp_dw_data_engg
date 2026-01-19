-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/contact_savvy_multimedia_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.contact_savvy_multimedia_info
   OPTIONS(description='Multimedia information from Contact Savvy Application Add-On to Artiva system that has the Voice Trak recording information')
  AS SELECT
      ROUND(a.multimedia_key_num, 0, 'ROUND_HALF_EVEN') AS multimedia_key_num,
      a.file_create_date_time,
      ROUND(a.acct_activity_id, 0, 'ROUND_HALF_EVEN') AS acct_activity_id,
      a.artiva_instance_code,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.voice_trak_pool_num,
      a.month_id,
      a.company_code,
      a.coid,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.unit_num,
      a.multimedia_file_desc,
      a.multimedia_file_path_text,
      ROUND(a.call_log_key_num, 0, 'ROUND_HALF_EVEN') AS call_log_key_num,
      a.file_type_code,
      a.short_call_ind,
      a.call_log_type_code,
      a.pool_dialer_type_code,
      a.voice_trak_rec_type_code,
      ROUND(a.voice_trak_rec_time_scnd_amt, 0, 'ROUND_HALF_EVEN') AS voice_trak_rec_time_scnd_amt,
      a.file_created_by_user_id,
      a.pool_assignment_id,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.contact_savvy_multimedia_info AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
