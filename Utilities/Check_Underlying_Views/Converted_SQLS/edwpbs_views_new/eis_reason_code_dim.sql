-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/eis_reason_code_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.eis_reason_code_dim AS SELECT
    eis_reason_code_dim.reason_code_sid,
    eis_reason_code_dim.reason_code_member,
    eis_reason_code_dim.reason_code_alias,
    eis_reason_code_dim.reason_code_gen04,
    eis_reason_code_dim.reason_code_gen04_alias,
    eis_reason_code_dim.reason_code_gen03,
    eis_reason_code_dim.reason_code_gen03_alias,
    eis_reason_code_dim.reason_code_gen02,
    eis_reason_code_dim.reason_code_gen02_alias
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_reason_code_dim
;
