-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_remittance_subscriber_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_remittance_subscriber_info AS SELECT
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
         AND security_mask_and_exception.masked_column_code = 'PN'
    ) AS pn ON pn.userid = session_user()
;
