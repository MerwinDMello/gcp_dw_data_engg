-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_ce_physician_cdm.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_ce_physician_cdm AS SELECT
    a.coid,
    a.company_code,
    ROUND(a.physician_npi, 0, 'ROUND_HALF_EVEN') AS physician_npi,
    a.cactus_physician_user_id,
    a.facility_assignment_status_desc,
    a.facility_assignment_cred_status_ind,
    a.facility_assignment_active_ind,
    a.facility_assignment_eff_from_date,
    a.facility_assignment_eff_to_date,
    a.physician_category_desc,
    a.physician_group_name,
    a.credential_completed_date,
    a.physician_first_name,
    a.physician_middle_name,
    a.physician_last_name,
    a.physician_postnominal_code,
    a.physician_full_name,
    a.physician_birth_date,
    a.physician_email_address,
    a.practice_area_code,
    a.practice_area_desc,
    a.standard_practice_area_ind,
    a.assigned_coid_ind,
    ROUND(a.hcp_dw_id, 0, 'ROUND_HALF_EVEN') AS hcp_dw_id,
    a.facility_physician_num,
    a.physician_mnemonic_cs,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwcdm_base_views.ref_ce_physician AS a
;
