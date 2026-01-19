-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/secref_mstr_facility.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_mstr_facility AS SELECT
    secref_mstr_facility.user_id,
    secref_mstr_facility.coid_sid,
    secref_mstr_facility.mstr_project_name,
    secref_mstr_facility.application_name,
    secref_mstr_facility.source_system_code,
    secref_mstr_facility.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.secref_mstr_facility
;
