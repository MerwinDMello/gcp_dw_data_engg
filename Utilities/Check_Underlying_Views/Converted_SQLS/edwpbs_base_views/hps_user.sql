-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/hps_user.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.hps_user
   OPTIONS(description='Maintains HealthCare Payment System  User details')
  AS SELECT
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
