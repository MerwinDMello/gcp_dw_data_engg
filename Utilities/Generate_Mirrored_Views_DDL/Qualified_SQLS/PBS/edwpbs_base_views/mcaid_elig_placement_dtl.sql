-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/mcaid_elig_placement_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.mcaid_elig_placement_dtl
   OPTIONS(description='Monthly snapshot table to load Medicaid Eligibility Placement Details only')
  AS SELECT
      ROUND(mcaid_elig_placement_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      mcaid_elig_placement_dtl.pe_date,
      mcaid_elig_placement_dtl.coid,
      mcaid_elig_placement_dtl.company_code,
      mcaid_elig_placement_dtl.patient_type_alias_name,
      mcaid_elig_placement_dtl.unit_num,
      ROUND(mcaid_elig_placement_dtl.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
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
      `hca-hin-curated-mirroring-td`.edwpbs.mcaid_elig_placement_dtl
  ;
