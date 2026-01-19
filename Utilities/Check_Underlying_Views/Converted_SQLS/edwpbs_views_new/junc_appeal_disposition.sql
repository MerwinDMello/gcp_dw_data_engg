-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/junc_appeal_disposition.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.junc_appeal_disposition AS SELECT
    junc_appeal_disposition.disposition_num,
    junc_appeal_disposition.cc_disposition_code,
    junc_appeal_disposition.disposition_desc,
    junc_appeal_disposition.disposition_status,
    junc_appeal_disposition.appeal_disp_sid
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_appeal_disposition
;
