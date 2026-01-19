-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/mcaid_elig_placement_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.mcaid_elig_placement_dtl
   OPTIONS(description='Monthly snapshot table to load Medicaid Eligibility Placement Details only')
  AS SELECT
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.pe_date,
      a.coid,
      a.company_code,
      a.patient_type_alias_name,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
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
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
