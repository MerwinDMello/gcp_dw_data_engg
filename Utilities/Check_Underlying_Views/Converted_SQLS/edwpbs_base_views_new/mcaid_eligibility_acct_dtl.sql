-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/mcaid_eligibility_acct_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.mcaid_eligibility_acct_dtl AS SELECT
    mcaid_eligibility_acct_dtl.patient_dw_id,
    mcaid_eligibility_acct_dtl.status_code,
    mcaid_eligibility_acct_dtl.referral_date,
    mcaid_eligibility_acct_dtl.closed_date,
    mcaid_eligibility_acct_dtl.company_code,
    mcaid_eligibility_acct_dtl.coid,
    mcaid_eligibility_acct_dtl.pat_acct_num,
    mcaid_eligibility_acct_dtl.elig_phase_code,
    mcaid_eligibility_acct_dtl.program_code,
    mcaid_eligibility_acct_dtl.application_filed_date,
    mcaid_eligibility_acct_dtl.approved_date,
    mcaid_eligibility_acct_dtl.source_system_code,
    mcaid_eligibility_acct_dtl.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.mcaid_eligibility_acct_dtl
;
