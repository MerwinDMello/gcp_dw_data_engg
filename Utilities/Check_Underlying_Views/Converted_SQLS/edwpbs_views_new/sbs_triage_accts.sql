-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_organization AS c ON a.coid = c.coid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON b.co_id = a.coid
     AND b.company_code = c.company_code
     AND b.user_id = session_user()
  WHERE upper(a.agency_type) = 'SBS_TRAIGE_ACCTS'
;
