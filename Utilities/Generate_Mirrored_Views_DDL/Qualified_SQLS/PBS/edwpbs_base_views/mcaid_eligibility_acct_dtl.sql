-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/mcaid_eligibility_acct_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.mcaid_eligibility_acct_dtl
   OPTIONS(description='Table will maintain the daily inventory of accounts from Parallon Medicaid Eligibility tool.')
  AS SELECT
      ROUND(mcaid_eligibility_acct_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      mcaid_eligibility_acct_dtl.status_code,
      mcaid_eligibility_acct_dtl.referral_date,
      mcaid_eligibility_acct_dtl.closed_date,
      mcaid_eligibility_acct_dtl.company_code,
      mcaid_eligibility_acct_dtl.coid,
      ROUND(mcaid_eligibility_acct_dtl.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      mcaid_eligibility_acct_dtl.elig_phase_code,
      mcaid_eligibility_acct_dtl.program_code,
      mcaid_eligibility_acct_dtl.application_filed_date,
      mcaid_eligibility_acct_dtl.approved_date,
      mcaid_eligibility_acct_dtl.source_system_code,
      mcaid_eligibility_acct_dtl.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.mcaid_eligibility_acct_dtl
  ;
