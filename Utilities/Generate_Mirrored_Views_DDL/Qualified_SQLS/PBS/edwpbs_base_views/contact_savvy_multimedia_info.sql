-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/contact_savvy_multimedia_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.contact_savvy_multimedia_info
   OPTIONS(description='Multimedia information from Contact Savvy Application Add-On to Artiva system that has the Voice Trak recording information')
  AS SELECT
      ROUND(contact_savvy_multimedia_info.multimedia_key_num, 0, 'ROUND_HALF_EVEN') AS multimedia_key_num,
      contact_savvy_multimedia_info.file_create_date_time,
      ROUND(contact_savvy_multimedia_info.acct_activity_id, 0, 'ROUND_HALF_EVEN') AS acct_activity_id,
      contact_savvy_multimedia_info.artiva_instance_code,
      ROUND(contact_savvy_multimedia_info.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      contact_savvy_multimedia_info.voice_trak_pool_num,
      contact_savvy_multimedia_info.month_id,
      contact_savvy_multimedia_info.company_code,
      contact_savvy_multimedia_info.coid,
      ROUND(contact_savvy_multimedia_info.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      contact_savvy_multimedia_info.unit_num,
      contact_savvy_multimedia_info.multimedia_file_desc,
      contact_savvy_multimedia_info.multimedia_file_path_text,
      ROUND(contact_savvy_multimedia_info.call_log_key_num, 0, 'ROUND_HALF_EVEN') AS call_log_key_num,
      contact_savvy_multimedia_info.file_type_code,
      contact_savvy_multimedia_info.short_call_ind,
      contact_savvy_multimedia_info.call_log_type_code,
      contact_savvy_multimedia_info.pool_dialer_type_code,
      contact_savvy_multimedia_info.voice_trak_rec_type_code,
      ROUND(contact_savvy_multimedia_info.voice_trak_rec_time_scnd_amt, 0, 'ROUND_HALF_EVEN') AS voice_trak_rec_time_scnd_amt,
      contact_savvy_multimedia_info.file_created_by_user_id,
      contact_savvy_multimedia_info.pool_assignment_id,
      contact_savvy_multimedia_info.source_system_code,
      contact_savvy_multimedia_info.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.contact_savvy_multimedia_info
  ;
