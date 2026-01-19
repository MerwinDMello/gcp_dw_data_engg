-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_rcm_mc_discrepancy_rate.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_rcm_mc_discrepancy_rate AS SELECT
    smry_rcm_mc_discrepancy_rate.coid,
    smry_rcm_mc_discrepancy_rate.company_code,
    smry_rcm_mc_discrepancy_rate.month_id,
    smry_rcm_mc_discrepancy_rate.over_under_amt,
    smry_rcm_mc_discrepancy_rate.exp_reimbursement_amt,
    smry_rcm_mc_discrepancy_rate.discrepancy_rate,
    smry_rcm_mc_discrepancy_rate.dw_last_update_date_time,
    smry_rcm_mc_discrepancy_rate.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.smry_rcm_mc_discrepancy_rate
;
