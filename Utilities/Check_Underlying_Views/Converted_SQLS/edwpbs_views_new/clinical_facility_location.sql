-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/clinical_facility_location.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.clinical_facility_location AS SELECT
    clinical_facility_location.company_code,
    clinical_facility_location.coid,
    clinical_facility_location.location_mnemonic_cs,
    clinical_facility_location.location_mnemonic,
    clinical_facility_location.location_code,
    clinical_facility_location.location_type_code,
    clinical_facility_location.location_desc,
    clinical_facility_location.location_active_ind,
    clinical_facility_location.sub_unit_num,
    clinical_facility_location.default_dept_num,
    clinical_facility_location.psych_location_ind,
    clinical_facility_location.rehab_location_ind,
    clinical_facility_location.outpatient_type_code,
    clinical_facility_location.last_update_timestamp,
    clinical_facility_location.facility_mnemonic_cs,
    clinical_facility_location.network_mnemonic_cs,
    clinical_facility_location.source_system_code,
    clinical_facility_location.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.clinical_facility_location
;
