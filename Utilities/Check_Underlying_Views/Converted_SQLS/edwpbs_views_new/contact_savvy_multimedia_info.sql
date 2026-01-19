-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/contact_savvy_multimedia_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.contact_savvy_multimedia_info AS SELECT
    a.multimedia_key_num,
    a.file_create_date_time,
    a.acct_activity_id,
    a.artiva_instance_code,
    a.patient_dw_id,
    a.voice_trak_pool_num,
    a.month_id,
    a.company_code,
    a.coid,
    a.pat_acct_num,
    a.unit_num,
    a.multimedia_file_desc,
    a.multimedia_file_path_text,
    a.call_log_key_num,
    a.file_type_code,
    a.short_call_ind,
    a.call_log_type_code,
    a.pool_dialer_type_code,
    a.voice_trak_rec_type_code,
    a.voice_trak_rec_time_scnd_amt,
    a.file_created_by_user_id,
    a.pool_assignment_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.contact_savvy_multimedia_info AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
