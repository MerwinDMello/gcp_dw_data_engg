-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_contact_center_application.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_contact_center_application
   OPTIONS(description='Contains reference details for all Contact Center Application.')
  AS SELECT
      ref_contact_center_application.application_id,
      ref_contact_center_application.application_code,
      ref_contact_center_application.application_name,
      ref_contact_center_application.application_desc,
      ref_contact_center_application.application_active_ind,
      ref_contact_center_application.application_type_code,
      ref_contact_center_application.application_type_short_name,
      ref_contact_center_application.source_system_code,
      ref_contact_center_application.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.ref_contact_center_application
  ;
