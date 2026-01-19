-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/hps_user.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.hps_user AS SELECT
    hps_user.hps_user_id,
    hps_user.hps_user_role_name,
    hps_user.user_full_name,
    hps_user.email_address_text,
    hps_user.company_name,
    hps_user.hps_jurisdiction_type,
    hps_user.source_system_code,
    hps_user.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.hps_user
;
