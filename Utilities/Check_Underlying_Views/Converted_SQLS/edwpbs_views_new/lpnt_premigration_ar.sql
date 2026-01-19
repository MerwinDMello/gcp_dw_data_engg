-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lpnt_premigration_ar.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lpnt_premigration_ar AS SELECT
    ar.coid,
    ar.unit_num,
    ar.rptg_period,
    ar.ar_amt_gt_150,
    ar.ar_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lpnt_premigration_ar AS ar
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS sf ON ar.coid = sf.co_id
     AND sf.user_id = session_user()
;
