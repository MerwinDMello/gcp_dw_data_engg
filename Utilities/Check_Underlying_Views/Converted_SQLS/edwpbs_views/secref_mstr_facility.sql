-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/secref_mstr_facility.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.secref_mstr_facility
   OPTIONS(description='Security Table that will contain Microstrategy Users by project and facility level')
  AS SELECT
      a.user_id,
      a.coid_sid,
      a.mstr_project_name,
      a.application_name,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_mstr_facility AS a
  ;
