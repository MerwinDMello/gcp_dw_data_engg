-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
