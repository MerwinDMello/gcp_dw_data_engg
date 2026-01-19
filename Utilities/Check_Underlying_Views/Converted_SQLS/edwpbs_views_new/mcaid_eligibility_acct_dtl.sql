-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/mcaid_eligibility_acct_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.mcaid_eligibility_acct_dtl AS SELECT
    a.patient_dw_id,
    a.status_code,
    a.referral_date,
    a.closed_date,
    a.company_code,
    a.coid,
    a.pat_acct_num,
    a.elig_phase_code,
    a.program_code,
    a.application_filed_date,
    a.approved_date,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.mcaid_eligibility_acct_dtl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
