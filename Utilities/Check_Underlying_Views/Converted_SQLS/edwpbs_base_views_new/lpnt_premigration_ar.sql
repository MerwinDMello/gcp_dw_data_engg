-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lpnt_premigration_ar.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lpnt_premigration_ar AS SELECT
    lpnt_premigration_ar.coid,
    lpnt_premigration_ar.unit_num,
    lpnt_premigration_ar.rptg_period,
    lpnt_premigration_ar.ar_amt_gt_150,
    lpnt_premigration_ar.ar_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lpnt_premigration_ar
;
