-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/mcaid_elig_placement_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.mcaid_elig_placement_dtl AS SELECT
    mcaid_elig_placement_dtl.patient_dw_id,
    mcaid_elig_placement_dtl.pe_date,
    mcaid_elig_placement_dtl.coid,
    mcaid_elig_placement_dtl.company_code,
    mcaid_elig_placement_dtl.patient_type_alias_name,
    mcaid_elig_placement_dtl.unit_num,
    mcaid_elig_placement_dtl.pat_acct_num,
    mcaid_elig_placement_dtl.mcaid_elig_phase_id,
    mcaid_elig_placement_dtl.mcaid_elig_acct_status_code,
    mcaid_elig_placement_dtl.elig_referral_date,
    mcaid_elig_placement_dtl.application_approved_date,
    mcaid_elig_placement_dtl.closed_date,
    mcaid_elig_placement_dtl.application_filed_date,
    mcaid_elig_placement_dtl.approval_age_num,
    mcaid_elig_placement_dtl.source_system_code,
    mcaid_elig_placement_dtl.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.mcaid_elig_placement_dtl
;
