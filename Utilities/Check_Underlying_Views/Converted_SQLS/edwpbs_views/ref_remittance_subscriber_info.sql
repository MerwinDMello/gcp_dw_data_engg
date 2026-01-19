-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_remittance_subscriber_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_remittance_subscriber_info
   OPTIONS(description='Reference table to maintain the Subscriber details of the claims sent.')
  AS SELECT
      a.remittance_subscriber_sid,
      a.patient_health_ins_num,
      a.insured_identification_qualifier_code,
      a.subscriber_id,
      a.insured_entity_type_qualifier_code,
      CASE
        WHEN session_user() = pn.userid THEN '***'
        ELSE a.subscriber_last_name
      END AS subscriber_last_name,
      CASE
        WHEN session_user() = pn.userid THEN '***'
        ELSE a.subscriber_first_name
      END AS subscriber_first_name,
      CASE
        WHEN session_user() = pn.userid THEN '***'
        ELSE a.subscriber_middle_name
      END AS subscriber_middle_name,
      a.subscriber_name_suffix,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_remittance_subscriber_info AS a
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
