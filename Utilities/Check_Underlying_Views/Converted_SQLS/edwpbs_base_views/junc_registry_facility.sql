-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_registry_facility.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_registry_facility
   OPTIONS(description='The facilities in each Registry')
  AS SELECT
      junc_registry_facility.coid,
      junc_registry_facility.company_code,
      junc_registry_facility.site_code,
      junc_registry_facility.registry_code,
      junc_registry_facility.von_center_id,
      junc_registry_facility.participant_id,
      junc_registry_facility.active_ind,
      junc_registry_facility.source_system_code,
      junc_registry_facility.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.junc_registry_facility
  ;
