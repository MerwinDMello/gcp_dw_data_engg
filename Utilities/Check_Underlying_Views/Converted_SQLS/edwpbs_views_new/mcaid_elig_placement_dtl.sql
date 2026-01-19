-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/mcaid_elig_placement_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.mcaid_elig_placement_dtl AS SELECT
    a.patient_dw_id,
    a.pe_date,
    a.coid,
    a.company_code,
    a.patient_type_alias_name,
    a.unit_num,
    a.pat_acct_num,
    a.mcaid_elig_phase_id,
    a.mcaid_elig_acct_status_code,
    a.elig_referral_date,
    a.application_approved_date,
    a.closed_date,
    a.application_filed_date,
    a.approval_age_num,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.mcaid_elig_placement_dtl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
