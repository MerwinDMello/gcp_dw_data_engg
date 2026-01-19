-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/facility_phy_registration_role_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.facility_phy_registration_role_pf AS SELECT
    facility_phy_registration_role_pf.coid,
    facility_phy_registration_role_pf.company_code,
    ROUND(facility_phy_registration_role_pf.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    facility_phy_registration_role_pf.facility_physician_num,
    facility_phy_registration_role_pf.role_type_code,
    facility_phy_registration_role_pf.physician_role_start_date,
    ROUND(facility_phy_registration_role_pf.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    facility_phy_registration_role_pf.source_system_code,
    facility_phy_registration_role_pf.physician_name,
    ROUND(facility_phy_registration_role_pf.physician_national_provider_id, 0, 'ROUND_HALF_EVEN') AS physician_national_provider_id,
    facility_phy_registration_role_pf.physician_upin,
    facility_phy_registration_role_pf.physician_state_license_id
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_phy_registration_role_pf
;
