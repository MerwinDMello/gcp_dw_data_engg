-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/sbs_triage_accts.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.sbs_triage_accts AS SELECT
    a.rptg_period AS monthend,
    a.unit_num,
    a.coid,
    a.agency_name AS triage_status,
    ROUND(a.recovery_amt, 0, 'ROUND_HALF_EVEN') AS pat_acct_num
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.armap_recoveries AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_organization AS c ON upper(a.coid) = upper(c.coid)
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(b.co_id) = upper(a.coid)
     AND upper(b.company_code) = upper(c.company_code)
     AND b.user_id = session_user()
  WHERE upper(a.agency_type) = 'SBS_TRAIGE_ACCTS'
;
