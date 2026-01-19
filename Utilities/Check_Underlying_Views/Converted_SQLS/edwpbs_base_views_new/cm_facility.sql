-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/cm_facility.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.cm_facility AS SELECT
    cm_facility.company_code,
    cm_facility.coid,
    cm_facility.midas_facility_code,
    cm_facility.eff_from_date,
    cm_facility.facility_mnemonic_cs,
    cm_facility.unit_num,
    cm_facility.midas_facility_name,
    cm_facility.midas_live_date,
    cm_facility.eff_to_date,
    cm_facility.initial_record_ind,
    cm_facility.source_system_key,
    cm_facility.source_system_code,
    cm_facility.active_dw_ind,
    cm_facility.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcm_base_views.cm_facility
;
