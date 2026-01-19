-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/hps_user.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.hps_user
   OPTIONS(description='Maintains HealthCare Payment System  User details')
  AS SELECT
      a.hps_user_id,
      a.hps_user_role_name,
      CASE
        WHEN session_user() = pn.userid THEN '***'
        ELSE a.user_full_name
      END AS user_full_name,
      CASE
        WHEN session_user() = pn.userid THEN '***'
        ELSE a.email_address_text
      END AS email_address_text,
      a.company_name,
      a.hps_jurisdiction_type,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.hps_user AS a
      LEFT OUTER JOIN (
        SELECT
            security_mask_and_exception.userid,
            security_mask_and_exception.masked_column_code
          FROM
            `hca-hin-dev-cur-parallon`.edw_sec_base_views.security_mask_and_exception
          WHERE security_mask_and_exception.userid = session_user()
           AND upper(security_mask_and_exception.masked_column_code) = 'PN'
      ) AS pn ON pn.userid = session_user()
  ;
