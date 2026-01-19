-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/contact_savvy_multimedia_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.contact_savvy_multimedia_info AS SELECT
    contact_savvy_multimedia_info.multimedia_key_num,
    contact_savvy_multimedia_info.file_create_date_time,
    contact_savvy_multimedia_info.acct_activity_id,
    contact_savvy_multimedia_info.artiva_instance_code,
    contact_savvy_multimedia_info.patient_dw_id,
    contact_savvy_multimedia_info.voice_trak_pool_num,
    contact_savvy_multimedia_info.month_id,
    contact_savvy_multimedia_info.company_code,
    contact_savvy_multimedia_info.coid,
    contact_savvy_multimedia_info.pat_acct_num,
    contact_savvy_multimedia_info.unit_num,
    contact_savvy_multimedia_info.multimedia_file_desc,
    contact_savvy_multimedia_info.multimedia_file_path_text,
    contact_savvy_multimedia_info.call_log_key_num,
    contact_savvy_multimedia_info.file_type_code,
    contact_savvy_multimedia_info.short_call_ind,
    contact_savvy_multimedia_info.call_log_type_code,
    contact_savvy_multimedia_info.pool_dialer_type_code,
    contact_savvy_multimedia_info.voice_trak_rec_type_code,
    contact_savvy_multimedia_info.voice_trak_rec_time_scnd_amt,
    contact_savvy_multimedia_info.file_created_by_user_id,
    contact_savvy_multimedia_info.pool_assignment_id,
    contact_savvy_multimedia_info.source_system_code,
    contact_savvy_multimedia_info.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.contact_savvy_multimedia_info
;
