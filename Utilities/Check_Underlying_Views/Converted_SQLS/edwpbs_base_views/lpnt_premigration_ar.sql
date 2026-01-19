-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lpnt_premigration_ar.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lpnt_premigration_ar AS SELECT
    lpnt_premigration_ar.coid,
    lpnt_premigration_ar.unit_num,
    lpnt_premigration_ar.rptg_period,
    ROUND(lpnt_premigration_ar.ar_amt_gt_150, 3, 'ROUND_HALF_EVEN') AS ar_amt_gt_150,
    ROUND(lpnt_premigration_ar.ar_amt, 3, 'ROUND_HALF_EVEN') AS ar_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lpnt_premigration_ar
;
