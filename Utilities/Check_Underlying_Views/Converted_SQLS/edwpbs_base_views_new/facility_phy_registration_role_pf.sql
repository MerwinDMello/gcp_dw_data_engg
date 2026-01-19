-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/facility_phy_registration_role_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_phy_registration_role_pf AS SELECT
    facility_phy_registration_role.coid,
    facility_phy_registration_role.company_code,
    facility_phy_registration_role.patient_dw_id,
    facility_phy_registration_role.facility_physician_num,
    facility_phy_registration_role.role_type_code,
    facility_phy_registration_role.physician_role_start_date,
    facility_phy_registration_role.pat_acct_num,
    facility_phy_registration_role.source_system_code,
    facility_phy_registration_role.physician_name,
    facility_phy_registration_role.physician_national_provider_id,
    facility_phy_registration_role.physician_upin,
    facility_phy_registration_role.physician_state_license_id
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.facility_phy_registration_role
;
