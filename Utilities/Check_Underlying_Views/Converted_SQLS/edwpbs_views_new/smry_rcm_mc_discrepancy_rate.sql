-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/smry_rcm_mc_discrepancy_rate.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.smry_rcm_mc_discrepancy_rate AS SELECT
    a.coid,
    a.company_code,
    a.month_id,
    a.over_under_amt,
    a.exp_reimbursement_amt,
    a.discrepancy_rate,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_rcm_mc_discrepancy_rate AS a
    CROSS JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_facility AS b
  WHERE b.co_id = a.coid
   AND b.company_code = a.company_code
   AND b.user_id = session_user()
;
