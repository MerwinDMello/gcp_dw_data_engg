-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_registry_facility.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_registry_facility AS SELECT
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
