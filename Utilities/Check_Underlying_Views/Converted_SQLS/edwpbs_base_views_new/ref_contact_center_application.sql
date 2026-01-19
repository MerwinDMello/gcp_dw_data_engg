-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_contact_center_application.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_contact_center_application AS SELECT
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
