-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/junc_appeal_disposition.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.junc_appeal_disposition AS SELECT
    junc_appeal_disposition.disposition_num,
    junc_appeal_disposition.cc_disposition_code,
    junc_appeal_disposition.disposition_desc,
    junc_appeal_disposition.disposition_status,
    junc_appeal_disposition.appeal_disp_sid
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.junc_appeal_disposition
;
